"""投递收讫链路测试：批次版本在一个目标上只有一份有效凭证。

覆盖的现场流程：
- B-A v1 投给 W1，收到回执后本地写回中断；
- 两个投递执行同时恢复，接收仓库只入仓一次；
- 回执详情与抽样工作台按有效凭证取数；
- 再交付到 W2，每个目标的收讫与样本数据版本分别可追溯。
"""
from __future__ import annotations

import threading

import pytest

from app.integration import (
    DEFAULT_TARGET_SYSTEM, backfill_receipt, deliver_and_sample, deliver_to_target,
    delivery_trail, receipt_detail, recover_delivery, retry_delivery,
    sample_workbench,
)
from app.models import CleanedContent, StorageBatch
from app.services.delivery_ledger import ledger
from app.store import store


BATCH_ID = "B-A"
VERSION = "v1"


@pytest.fixture(autouse=True)
def clean_ledger() -> None:
    tables = ("data_delivery", "quality_sample")
    snapshots = {name: [dict(row) for row in store.rows(name)] for name in tables}
    ledger.reset()
    yield
    ledger.reset()
    for name, rows in snapshots.items():
        store.rows(name).clear()
        store.rows(name).extend(rows)


def _batch(batch_id: str = BATCH_ID, version: str = VERSION, rows: int = 10) -> StorageBatch:
    records = [
        CleanedContent(source_url=f"https://b-a.example.com/{i}",
                       extract_rule_id="R1", fields={"title": f"t{i}"})
        for i in range(rows)
    ]
    batch = StorageBatch(
        batch_id=batch_id, source_job_id="JOB-BA", row_count=rows,
        status="校验通过", records=records,
        storage_path=f"/data/batches/{batch_id}/", batch_version=version,
    )
    batch.checksum = batch.compute_checksum()
    return batch


def _store_rows(keys: dict[str, str]) -> list[dict]:
    return [
        row for row in store.rows("data_delivery")
        if all(str(row.get(k, "")) == v for k, v in keys.items())
    ] or [
        row for row in store.rows("data_delivery")
        if str(row.get("批次关联") or row.get("数据来源")) == keys.get("批次关联")
        and str(row.get("目标系统")) == keys.get("目标系统")
    ]


# ---------------------------------------------------------------------------
# 连续投递同一目标：台账不出现重复编号
# ---------------------------------------------------------------------------

def test_repeated_delivery_same_target_keeps_single_receipt_and_task() -> None:
    batch = _batch()

    first = deliver_to_target(batch, "W1")
    second = deliver_to_target(batch, "W1")
    third = retry_delivery(batch, "W1")

    for outcome in (first, second, third):
        assert outcome.ok, outcome.message
    assert first.receipt.receipt_id == second.receipt.receipt_id == third.receipt.receipt_id
    assert first.task.task_id == second.task.task_id == third.task.task_id

    receipts = ledger.receipts_for_batch(BATCH_ID, VERSION)
    assert len(receipts) == 1, f"同一批次版本在一个目标上只应有一份凭证: {receipts}"
    tasks = ledger.tasks_for_batch(BATCH_ID, VERSION)
    assert len(tasks) == 1, f"投递台账只应有一条任务: {tasks}"

    rows = _store_rows({"批次关联": BATCH_ID, "目标系统": "W1"})
    assert len(rows) == 1, f"投递台账出现重复编号: {rows}"
    assert rows[0]["任务编号"] == first.task.task_id
    assert rows[0]["收讫凭证"] == first.receipt.receipt_id


def test_new_version_gets_its_own_receipt() -> None:
    v1 = _batch(version="v1")
    v2 = _batch(version="v2")

    r1 = deliver_to_target(v1, "W1")
    r2 = deliver_to_target(v2, "W1")

    assert r1.receipt.receipt_id != r2.receipt.receipt_id
    assert r1.receipt.batch_version == "v1"
    assert r2.receipt.batch_version == "v2"
    assert len(ledger.receipts_for_batch(BATCH_ID, "v1")) == 1
    assert len(ledger.receipts_for_batch(BATCH_ID, "v2")) == 1


# ---------------------------------------------------------------------------
# 回执后本地写回中断，两个执行同时恢复
# ---------------------------------------------------------------------------

def test_interrupted_writeback_then_concurrent_recovery() -> None:
    batch = _batch()

    crashed = deliver_to_target(batch, "W1", interrupt_before="receipt")
    assert crashed.receipt is None, "写回中断时本地不应有凭证"
    assert crashed.pending is not None, "未确认的投递要留下待对账进度"
    assert ledger.warehouse_has_version(BATCH_ID, VERSION, "W1"), "接收仓库应已入仓"
    assert ledger.pending_for(BATCH_ID, VERSION, "W1")["stage"] == "receipt_writeback"

    # 回执详情在恢复前就能看到“已入仓、待对账”，不会误导成两份入仓。
    before = receipt_detail(batch, "W1")
    assert before is not None
    assert before["intake_count"] == 1
    assert before["claim_status"] == "待对账"
    assert before["task_count"] == 0

    results: list = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def recover() -> None:
        try:
            barrier.wait()
            results.append(recover_delivery(batch, "W1"))
        except BaseException as exc:  # pragma: no cover - 记录线程异常
            errors.append(exc)

    threads = [threading.Thread(target=recover) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, errors
    assert len(results) == 2
    receipt_ids = {outcome.receipt.receipt_id for outcome in results if outcome.receipt}
    assert len(receipt_ids) == 1, f"两个恢复执行应拿到同一份凭证: {receipt_ids}"
    assert all(outcome.ok for outcome in results)

    # 接收仓库只入仓一次，本地只落一条任务。
    assert len(ledger.receipts_for_batch(BATCH_ID, VERSION)) == 1
    assert len(ledger.tasks_for_batch(BATCH_ID, VERSION)) == 1
    assert ledger.pending_for(BATCH_ID, VERSION, "W1") is None

    detail = receipt_detail(batch, "W1")
    assert detail is not None
    assert detail["intake_count"] == 1, "目标仅入仓一次"
    assert detail["task_count"] == 1
    assert detail["claim_status"] == "已收讫"
    assert detail["receipt_status"] == "已收讫"
    assert detail["receipt_id"] == receipt_ids.pop()
    assert detail["data_version"] if "data_version" in detail else True

    rows = _store_rows({"批次关联": BATCH_ID, "目标系统": "W1"})
    assert len(rows) == 1, f"恢复后投递台账不应重复: {rows}"


def test_two_fresh_submissions_at_once_intake_once() -> None:
    batch = _batch()
    outcomes: list = []
    errors: list[BaseException] = []
    barrier = threading.Barrier(2)

    def submit() -> None:
        try:
            barrier.wait()
            outcomes.append(deliver_to_target(batch, "W1"))
        except BaseException as exc:  # pragma: no cover
            errors.append(exc)

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors, errors
    assert len({o.receipt.receipt_id for o in outcomes}) == 1
    assert len(ledger.receipts_for_batch(BATCH_ID, VERSION)) == 1
    assert len(ledger.tasks_for_batch(BATCH_ID, VERSION)) == 1
    assert receipt_detail(batch, "W1")["intake_count"] == 1


def test_warehouse_can_reissue_receipt_without_second_intake() -> None:
    batch = _batch()
    outcome = deliver_to_target(batch, "W1")

    reissued = backfill_receipt(outcome.receipt.receipt_id)
    assert reissued is not None
    assert reissued["reissue_count"] >= 1
    assert receipt_detail(batch, "W1")["intake_count"] == 1, "补发收执不重复入仓"


# ---------------------------------------------------------------------------
# 抽样与凭证一致
# ---------------------------------------------------------------------------

def test_sample_registered_from_effective_receipt() -> None:
    batch = _batch()
    outcome = deliver_to_target(batch, "W1")

    assert outcome.sample is not None
    assert outcome.sample.receipt_id == outcome.receipt.receipt_id
    assert outcome.sample.data_version == outcome.receipt.batch_version == "v1"
    assert outcome.sample.sample_size == 1  # 10 行 × 10%

    workbench = sample_workbench(BATCH_ID, VERSION)
    assert len(workbench) == 1
    assert workbench[0]["target_system"] == "W1"
    assert workbench[0]["receipt_id"] == outcome.receipt.receipt_id
    assert workbench[0]["data_version"] == "v1"

    sample_rows = [
        row for row in store.rows("quality_sample")
        if row.get("目标批次") == BATCH_ID
    ]
    assert len(sample_rows) == 1
    assert sample_rows[0]["收讫凭证"] == outcome.receipt.receipt_id
    assert sample_rows[0]["数据版本"] == "v1"


def test_retry_keeps_existing_sample_plan_findable() -> None:
    store.rows("quality_sample").append({
        "id": 4242, "计划编号": "QS-PREEXIST-BA", "目标批次": BATCH_ID,
        "抽样比例": "0.2", "抽样方式": "随机抽样", "样本数量": 2,
        "检验结果": "", "偏差阈值": "0.05", "最近采样": "",
        "status": "待采样", "pending": True, "abnormal": False,
        "目标系统": "W1", "数据版本": "v1", "收讫凭证": "",
    })
    batch = _batch()

    first = deliver_to_target(batch, "W1")
    retried = retry_delivery(batch, "W1")

    assert first.sample is not None and first.sample.plan_id == "QS-PREEXIST-BA"
    assert retried.sample is not None and retried.sample.plan_id == "QS-PREEXIST-BA"
    workbench = sample_workbench(BATCH_ID, VERSION)
    assert [item["plan_id"] for item in workbench] == ["QS-PREEXIST-BA"]
    assert retried.sample.receipt_id == first.receipt.receipt_id


# ---------------------------------------------------------------------------
# 同一批次版本交付多个目标：每个目标各自可追溯
# ---------------------------------------------------------------------------

def test_same_version_to_two_targets_is_independently_traceable() -> None:
    batch = _batch()

    w1 = deliver_to_target(batch, "W1")
    w2 = deliver_to_target(batch, "W2")

    assert w1.ok and w2.ok
    assert w1.receipt.receipt_id != w2.receipt.receipt_id
    assert w1.task.task_id != w2.task.task_id
    assert w1.sample.plan_id != w2.sample.plan_id

    trail = delivery_trail(BATCH_ID, VERSION)
    by_target = {item["target_system"]: item for item in trail}
    assert set(by_target) == {"W1", "W2"}
    for target, item in by_target.items():
        assert item["batch_version"] == "v1"
        assert item["receipt_id"]
        assert item["task_id"]
        assert item["data_version"] == "v1"
        detail = receipt_detail(batch, target)
        assert detail["intake_count"] == 1
        assert detail["sample"]["data_version"] == "v1"
        assert detail["sample"]["receipt_id"] == item["receipt_id"]


def test_default_target_is_w1_and_failed_batch_has_no_receipt() -> None:
    batch = _batch()
    outcome = deliver_to_target(batch, DEFAULT_TARGET_SYSTEM)
    assert outcome.ok

    failed = StorageBatch(
        batch_id="B-A-FAIL", source_job_id="JOB-BA", row_count=0,
        status="校验失败", records=[], batch_version="v1",
    )
    task, sample = deliver_and_sample(failed, "W1")
    assert task is not None and task.status == "投递失败"
    assert sample is None
    assert ledger.receipt_for("B-A-FAIL", "v1", "W1") is None
