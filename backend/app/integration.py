"""跨模块数据流编排：真实业务流水线，各模块间通过dataclass传递数据。

实现以下数据流：
- crawl_site → crawl_job → content_extract → field_clean → batch_store
- crawl_job → url_dedup → parse_log
- proxy_pool → anti_block → crawl_limit
- batch_store → data_delivery → quality_sample
- incr_crawl → crawl_job → diff_check
- data_archive ← batch_store
- crawl_cost ← proxy_pool
- crawl_job → scheduler
"""
from __future__ import annotations

import random
import re
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.models import (
    AntiBlockStrategy, AntiType, ArchiveFormat, ArchivePolicy, CleanedContent,
    CleanRule, CostRecord, CrawlJobSpec, CrawlLimitRule, DeliveryFormat,
    DeliveryReceipt, DeliveryTask, DiffRecord, ExtractRule, IncrCursor,
    ParseLogEntry, ProxyNode, QualitySample, RawExtractedContent, SampleMethod,
    SchedulePlan, SiteConfig, SiteType, StorageBatch, UrlFingerprint,
)
from app.services.batch_catalog import catalog
from app.services.delivery_ledger import ledger
from app.store import store


# 默认接收仓库；同一批次版本可以分别投给多个目标，每个目标各留一份凭证。
DEFAULT_TARGET_SYSTEM = "W1"


# ---------------------------------------------------------------------------
# 流水线 1: crawl_site → crawl_job → content_extract → field_clean → batch_store
# ---------------------------------------------------------------------------

def run_crawl_pipeline(site_row: dict[str, Any], job_row: dict[str, Any],
                       extract_rows: list[dict[str, Any]],
                       clean_rows: list[dict[str, Any]]) -> StorageBatch | None:
    """端到端采集链路：站点配置 → 作业创建 → 内容抽取 → 字段清洗 → 批次存储。

    返回完整的 StorageBatch 或 None（不可恢复错误时）。
    """
    # Step 1: 站点验证（crawl_site）
    site = SiteConfig.from_seed_row(site_row)
    if not site.is_crawlable():
        return None  # 站点未验证或 Robots 禁止采集

    # Step 2: 创建采集作业（crawl_job）
    job = CrawlJobSpec.from_seed_row(job_row, site=site)
    if not job.entry_urls:
        return None  # 没有入口 URL

    # Step 3: 内容抽取（content_extract）
    rules = [ExtractRule.from_seed_row(er) for er in extract_rows if er.get("status") == "生效中"]
    if not rules:
        rules = [ExtractRule.from_seed_row(extract_rows[0])] if extract_rows else []
    extracted_contents: list[RawExtractedContent] = []
    parse_logs: list[ParseLogEntry] = []

    for i, url in enumerate(job.entry_urls):
        rule = rules[i % len(rules)] if rules else None
        if rule is None:
            continue
        raw = _simulate_extract(url, rule)
        extracted_contents.append(raw)

        # 记录解析日志（parse_log）
        log = ParseLogEntry(
            record_id=f"PLOG-{job.job_id}-{i+1:04d}",
            source_url=url,
            parse_rule_id=rule.rule_id,
            parse_result="成功" if not raw.has_errors() else "部分失败",
            error_type="字段缺失" if raw.has_errors() else "",
            retry_count=1 if raw.has_errors() else 0,
            error_detail="; ".join(raw.errors) if raw.has_errors() else "",
        )
        parse_logs.append(log)
        _persist_parse_log(log)

    job.completed_pages = len(extracted_contents)
    job.failed_pages = sum(1 for r in extracted_contents if r.has_errors())
    job.status = "已完成" if job.failed_pages == 0 else "部分失败"

    # Step 4: 字段清洗（field_clean）
    clean_rules = [CleanRule.from_seed_row(cr) for cr in clean_rows if cr.get("status") == "生效中"]
    if not clean_rules:
        clean_rules = [CleanRule.from_seed_row(clean_rows[0])] if clean_rows else []
    cleaned: list[CleanedContent] = []

    for raw in extracted_contents:
        cc = _apply_clean_rules(raw, clean_rules)
        cleaned.append(cc)

    # Step 5: 批次存储（batch_store）
    batch = StorageBatch(
        batch_id=f"BATCH-{job.job_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        source_job_id=job.job_id,
        row_count=len(cleaned),
        storage_path=f"/data/batches/{job.job_id}/",
        compression="gzip",
        schema_version="v1",
        stored_at=datetime.now(timezone.utc).isoformat(),
        status="已落盘",
        records=cleaned,
    )
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(c.is_valid() for c in cleaned) else "校验失败"

    _persist_batch(batch)
    catalog.register(batch)

    return batch


def _simulate_extract(url: str, rule: ExtractRule) -> RawExtractedContent:
    """模拟从 URL 抽取内容。

    字段映射规则：
    - 若 value 含逗号：按逗号拆分为多个字段名（种子数据模式）
    - 若 key 非 "raw"：将 key 用作字段名（测试覆盖模式）
    """
    errors: list[str] = []
    fields: dict[str, str] = {}

    if "error" in url.lower() or "404" in url:
        errors.append("页面不可达")
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id, errors=errors)

    if "empty" in url.lower():
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id,
                                   raw_fields={}, errors=["页面内容为空"])

    for key, value in rule.field_mapping.items():
        parts = [p.strip() for p in value.split(",") if p.strip()]
        if len(parts) > 1:
            for field_name in parts:
                fields[field_name] = f"extracted:{field_name}:{hash(url + field_name) % 10000}"
        elif key != "raw":
            fields[key] = f"extracted:{key}:{hash(url + key) % 10000}"
        elif parts:
            fields[parts[0]] = f"extracted:{parts[0]}:{hash(url + parts[0]) % 10000}"
        else:
            fields[key] = f"extracted:{key}:{hash(url) % 10000}"

    return RawExtractedContent(
        source_url=url, extract_rule_id=rule.rule_id,
        raw_fields=fields, errors=errors,
    )


def _apply_clean_rules(raw: RawExtractedContent, rules: list[CleanRule]) -> CleanedContent:
    """对原始抽取内容逐字段应用清洗规则。同字段多条规则链式执行。"""
    cleaned_fields: dict[str, Any] = {}
    errors: list[str] = []

    for rule in rules:
        field = rule.target_field
        # 已清洗过的字段从 cleaned_fields 读取，实现链式清洗
        raw_value = cleaned_fields.get(field, raw.raw_fields.get(field, ""))

        if not raw_value and rule.null_strategy == "skip":
            continue
        if not raw_value and rule.null_strategy == "fill_default":
            raw_value = "N/A"

        try:
            if rule.clean_type.value == "去空格":
                value = raw_value.strip()
            elif rule.clean_type.value == "标准化":
                value = raw_value.lower().replace(" ", "_")
            elif rule.clean_type.value == "校验":
                if rule.validation_range and not re.match(rule.validation_range, raw_value):
                    errors.append(f"字段 {field} 校验失败: {raw_value}")
                    value = raw_value
                else:
                    value = raw_value
            elif rule.clean_type.value == "正则提取":
                m = re.search(rule.expression, raw_value) if rule.expression else None
                value = m.group(1) if m else raw_value
            elif rule.clean_type.value == "类型转换":
                value = _cast_type(raw_value, rule.expression)
            else:
                value = raw_value
        except Exception as exc:
            errors.append(f"字段 {field} 清洗异常: {exc}")
            value = raw_value

        cleaned_fields[field] = value

    return CleanedContent(
        source_url=raw.source_url,
        extract_rule_id=raw.extract_rule_id,
        fields=cleaned_fields,
        clean_errors=errors,
    )


def _cast_type(value: str, target: str) -> Any:
    if target == "int":
        return int(value) if value.isdigit() else value
    if target == "float":
        try:
            return float(value)
        except ValueError:
            return value
    if target == "datetime":
        return value  # 简化实现
    return value


# ---------------------------------------------------------------------------
# 流水线 2: crawl_job → url_dedup → parse_log
# ---------------------------------------------------------------------------

def run_dedup_flow(job: CrawlJobSpec) -> tuple[list[UrlFingerprint], list[UrlFingerprint]]:
    """对采集作业的入口 URL 执行去重，返回新增和重复列表。"""
    existing: dict[str, UrlFingerprint] = {}
    for row in store.rows("url_dedup"):
        fp = UrlFingerprint.from_seed_row(row)
        existing[fp.hash_value] = fp

    new_fps: list[UrlFingerprint] = []
    dup_fps: list[UrlFingerprint] = []

    for url in job.entry_urls:
        fp = UrlFingerprint.compute(url)
        if fp.hash_value in existing:
            existing[fp.hash_value].duplicate_count += 1
            dup_fps.append(existing[fp.hash_value])
        else:
            existing[fp.hash_value] = fp
            new_fps.append(fp)
            _persist_fingerprint(fp)

    return new_fps, dup_fps


# ---------------------------------------------------------------------------
# 流水线 3: proxy_pool → anti_block → crawl_limit
# ---------------------------------------------------------------------------

def select_proxy_for_job(job: CrawlJobSpec) -> ProxyNode | None:
    """从代理池为采集作业选择可用代理节点，并检查反爬策略和限速规则。

    返回最佳代理或 None（无可用代理时）。
    """
    proxies: list[ProxyNode] = []
    for row in store.rows("proxy_pool"):
        p = ProxyNode.from_seed_row(row)
        if p.is_available():
            proxies.append(p)

    if not proxies:
        return None

    # 按延迟排序，选择最快的
    proxies.sort(key=lambda p: p.latency_ms)

    # 检查反爬策略
    anti_strategies: list[AntiBlockStrategy] = []
    for row in store.rows("anti_block"):
        a = AntiBlockStrategy.from_seed_row(row)
        if a.target_site == job.site.domain and a.status == "生效中":
            anti_strategies.append(a)

    best_proxy = proxies[0]

    # 检查限速规则
    for row in store.rows("crawl_limit"):
        limit = CrawlLimitRule.from_seed_row(row)
        if limit.target_site == job.site.domain and limit.status == "生效中":
            job.concurrency = min(job.concurrency, limit.concurrency_max)
            if limit.request_interval_ms > 0:
                best_proxy.latency_ms = max(best_proxy.latency_ms, limit.request_interval_ms)

    return best_proxy


# ---------------------------------------------------------------------------
# 流水线 4: batch_store → data_delivery → quality_sample
# ---------------------------------------------------------------------------

@dataclass
class DeliveryOutcome:
    """一次投递的完整结果：任务、有效收讫凭证、抽样计划和对账状态。"""
    task: DeliveryTask | None = None
    receipt: DeliveryReceipt | None = None
    sample: QualitySample | None = None
    claimed: bool = False
    reissued: bool = False
    deduplicated: bool = False
    reconciled: bool = False
    intake_count: int = 0
    pending: dict[str, Any] | None = None
    message: str = ""
    sample_message: str = ""

    @property
    def ok(self) -> bool:
        return self.receipt is not None and self.receipt.is_valid()


# 同一批次版本并发提交时，串行化“查重 + 入仓 + 写回”这一段。
_delivery_lock = threading.RLock()


def _delivery_task_row(
    batch: StorageBatch, target_system: str, version: str | None = None,
) -> dict[str, Any] | None:
    """按业务键找已有投递任务行，复用任务编号，避免台账重复。"""
    batch_version = version or batch.batch_version
    for row in store.rows("data_delivery"):
        if (
            str(row.get("批次关联") or row.get("数据来源")) == batch.batch_id
            and str(row.get("批次版本") or "v1") == batch_version
            and str(row.get("目标系统")) == target_system
        ):
            return row
    return None


def _deliver(
    batch: StorageBatch,
    target_system: str,
    *,
    submission_token: str = "",
    interrupt_before: str | None = None,
    reconcile_pending: bool = False,
) -> DeliveryOutcome:
    """批次版本投递到指定目标，全程以收讫台账判定投递资格。

    `interrupt_before` 可取值：
    - "receipt"：收到回执后、本地凭证写回前断开；
    - "task"：凭证已写回、投递任务写回前断开。

    全程串行化：查重、入仓、写凭证、写任务、抽样是一个事务；恢复任务与定时
    执行同时进来时，后到的一方只能看到前一方写好的有效凭证，从而复用而不是
    再领一份投递资格。
    """
    outcome = DeliveryOutcome()
    token = submission_token or _submission_token(batch, target_system)

    if batch.status != "校验通过":
        _record_failed_delivery(batch, target_system, outcome)
        return outcome

    with _delivery_lock:
        if reconcile_pending:
            outcome.reconciled = ledger.reconcile(batch, target_system) is not None

        receipt, claim_message = ledger.claim(
            batch=batch, target_system=target_system,
            submission_token=token, persist=False)
        if receipt is None:
            outcome.message = "接收仓库未确认，投递保持待对账"
            return outcome

        outcome.intake_count = 1 if ledger.warehouse_has_version(
            batch.batch_id, batch.batch_version, target_system) else 0
        outcome.reissued = claim_message in ("补全凭证", "补发收执")
        outcome.deduplicated = outcome.reissued and claim_message == "补全凭证"
        # 只有首次入仓的提交才拿到新的投递资格；复用/补全都不是新资格。
        outcome.claimed = claim_message == "待确认"

        (outcome.receipt, outcome.task, outcome.sample,
         outcome.pending, message) = _writeback(
            batch, target_system, receipt, token, interrupt_before)
        outcome.message = message
        if outcome.sample is not None:
            outcome.sample_message = "已登记"

        if claim_message == "补发收执" and outcome.receipt is not None:
            outcome.message = "接收仓库已入仓该版本，补发收执并复用投递任务"
        elif outcome.deduplicated and outcome.receipt is not None:
            outcome.message = "接收仓库已入仓该版本，只补全本地凭证，未重复入仓"
        return outcome


def _writeback(
    batch: StorageBatch,
    target_system: str,
    receipt: DeliveryReceipt,
    token: str,
    interrupt_before: str | None,
) -> tuple[
    DeliveryReceipt | None, DeliveryTask | None, QualitySample | None,
    dict[str, Any] | None, str,
]:
    """把一次投递的收讫结果写回本地：凭证 → 任务 → 抽样。

    任一步被中断都返回已完成的部分并留下待对账进度；远端入仓事实不受影响，
    恢复任务核对后会用同一份凭证把剩余写回补全。
    """
    if interrupt_before == "receipt":
        progress = _record_pending(batch, target_system, token, "receipt_writeback")
        return None, None, None, progress, "已收到回执，本地凭证写回中断，等待对账"

    ledger.persist_receipt(receipt)

    row = _delivery_task_row(batch, target_system)
    task = DeliveryTask(
        task_id=(str(row.get("任务编号")) if row
                 else f"DEL-{batch.batch_id}-{target_system}"),
        data_source=batch.batch_id,
        target_system=target_system,
        delivery_format=DeliveryFormat.CSV,
        frequency=str(row.get("投递频率", "manual")) if row else "manual",
        partition_strategy=str(row.get("分区策略", "by_date")) if row else "by_date",
        batch_ref_id=batch.batch_id,
        batch_version=batch.batch_version,
    )
    if not ledger.register_task(task, receipt, interrupt_before=interrupt_before):
        progress = _record_pending(batch, target_system, token, "task_writeback")
        return receipt, None, None, progress, "已收到回执，本地任务写回中断，等待对账"

    _persist_delivery_row(task, batch, receipt)
    _mark_pending_resolved(batch, target_system)

    # 抽样从同一份有效收讫凭证取数，登记接收方实际入仓的版本。
    sample, _ = ledger.bind_sample(
        batch, target_system,
        make_sample=lambda rcpt: _make_sample(batch, rcpt),
        adopt_existing=_adopt_existing_sample,
    )
    if sample is not None:
        _run_sample_check(batch, sample)
        _persist_sample_row(sample)

    return receipt, task, sample, None, f"投递完成，凭证 {receipt.receipt_id}"


def _submission_token(batch: StorageBatch, target_system: str) -> str:
    return f"{batch.batch_id}@{batch.batch_version}->{target_system}"


def _record_pending(
    batch: StorageBatch, target_system: str, token: str, stage: str,
) -> dict[str, Any]:
    progress = ledger.record_pending(
        batch=batch, target_system=target_system,
        submission_token=token, stage=stage,
    )
    _persist_pending_row(batch, target_system, stage)
    return progress


def _record_failed_delivery(
    batch: StorageBatch, target_system: str, outcome: DeliveryOutcome,
) -> None:
    """校验失败的批次不产生收讫凭证，只把失败结果记到投递台账上。"""
    row = _delivery_task_row(batch, target_system)
    task = DeliveryTask(
        task_id=(str(row.get("任务编号")) if row
                 else f"DEL-{batch.batch_id}-{target_system}"),
        data_source=batch.batch_id,
        target_system=target_system,
        delivery_format=DeliveryFormat.CSV,
        frequency=str(row.get("投递频率", "manual")) if row else "manual",
        partition_strategy=str(row.get("分区策略", "by_date")) if row else "by_date",
        last_delivery=datetime.now(timezone.utc).isoformat(),
        status="投递失败",
        batch_ref_id=batch.batch_id,
        batch_version=batch.batch_version,
        delivered_row_count=0,
    )
    _persist_delivery_row(task, batch, None)
    outcome.task = task
    outcome.message = f"批次{batch.status}，不允许投递"

def deliver_to_target(
    batch: StorageBatch,
    target_system: str,
    *,
    submission_token: str = "",
    interrupt_before: str | None = None,
) -> DeliveryOutcome:
    """把批次版本投递到指定目标仓库，并同步抽样。"""
    catalog.register(batch)
    return _deliver(
        batch, target_system,
        submission_token=submission_token,
        interrupt_before=interrupt_before,
    )


def retry_delivery(
    batch: StorageBatch,
    target_system: str = DEFAULT_TARGET_SYSTEM,
    *,
    submission_token: str = "",
) -> DeliveryOutcome:
    """投递重试：已有成功凭证直接复用，未确认的进度先对账再补发收执。"""
    return _deliver(
        batch, target_system,
        submission_token=submission_token,
        reconcile_pending=True,
    )


def recover_delivery(
    batch: StorageBatch,
    target_system: str = DEFAULT_TARGET_SYSTEM,
    *,
    submission_token: str = "",
) -> DeliveryOutcome:
    """恢复中断的投递：先按待对账进度对账，再走幂等投递资格。

    两个恢复执行同时进来时，后到的一方只能看到前一方写好的有效凭证，
    不会重复入仓、也不会在投递台账里留下第二个编号。
    """
    return _deliver(
        batch, target_system,
        submission_token=submission_token,
        reconcile_pending=True,
    )


def backfill_receipt(
    receipt_id: str,
    target_system: str = DEFAULT_TARGET_SYSTEM,
) -> dict[str, Any] | None:
    """接收仓库按入仓事实补发收执，供投递回执详情页对账。"""
    return ledger.reissue_receipt(receipt_id)


def receipt_detail(
    batch: StorageBatch,
    target_system: str = DEFAULT_TARGET_SYSTEM,
) -> dict[str, Any] | None:
    """投递回执详情：目标只入仓一次，凭证与任务一一对应。

    本地写回中断时凭证可能还没入台账，但接收仓库的入仓事实与待对账进度
    仍然可查，详情页据此显示“待对账”，恢复任务补全后再变成“已收讫”。
    """
    receipt = ledger.receipt_for(batch.batch_id, batch.batch_version, target_system)
    pending = ledger.pending_for(batch.batch_id, batch.batch_version, target_system)
    intake = ledger.warehouse_version(
        batch.batch_id, batch.batch_version, target_system)
    if receipt is None and pending is None and intake is None:
        return None
    task = ledger.task_for(batch.batch_id, batch.batch_version, target_system)
    sample = ledger.sample_for(batch.batch_id, batch.batch_version, target_system)
    return {
        "batch_id": batch.batch_id,
        "batch_version": batch.batch_version,
        "target_system": target_system,
        "receipt_id": receipt.receipt_id if receipt else "",
        "receipt_status": receipt.status if receipt else "待确认",
        "reconciliation_status": (
            receipt.reconciliation_status if receipt else "待对账"),
        "claim_status": ledger.claim_status(
            batch.batch_id, batch.batch_version, target_system),
        "reissue_count": receipt.reissue_count if receipt else 0,
        "intake_count": 1 if intake else 0,
        "received_at": receipt.received_at if receipt else "",
        "delivered_row_count": receipt.delivered_row_count if receipt else 0,
        "task_id": task.task_id if task else "",
        "task_count": 1 if task else 0,
        "pending_stage": (pending or {}).get("stage", ""),
        "sample": _sample_payload(sample),
    }


def sample_workbench(
    batch_id: str, batch_version: str = "v1",
) -> list[dict[str, Any]]:
    """抽样工作台：每个有效收讫凭证对应一个样本，登记的数据版本可追溯。"""
    workbench: list[dict[str, Any]] = []
    for receipt in ledger.receipts_for_batch(batch_id, batch_version):
        if not receipt.is_valid():
            continue
        sample = ledger.sample_for(
            batch_id, receipt.batch_version, receipt.target_system)
        workbench.append({
            "target_system": receipt.target_system,
            "receipt_id": receipt.receipt_id,
            "data_version": sample.data_version if sample else receipt.batch_version,
            "plan_id": sample.plan_id if sample else "",
            "sample_size": sample.sample_size if sample else 0,
            "check_result": sample.check_result if sample else "",
            "status": sample.status if sample else "待采样",
        })
    return workbench


def delivery_trail(batch_id: str, batch_version: str = "v1") -> list[dict[str, Any]]:
    """一个批次版本投过的所有目标：每个目标一份凭证、一条任务。"""
    trail: list[dict[str, Any]] = []
    for receipt in ledger.receipts_for_batch(batch_id, batch_version):
        task = ledger.task_for(batch_id, receipt.batch_version, receipt.target_system)
        sample = ledger.sample_for(
            batch_id, receipt.batch_version, receipt.target_system)
        trail.append({
            "target_system": receipt.target_system,
            "batch_version": receipt.batch_version,
            "receipt_id": receipt.receipt_id,
            "receipt_status": receipt.status,
            "reissue_count": receipt.reissue_count,
            "task_id": task.task_id if task else "",
            "sample_plan_id": sample.plan_id if sample else "",
            "data_version": sample.data_version if sample else "",
        })
    return trail


def deliver_and_sample(
    batch: StorageBatch,
    target_system: str = DEFAULT_TARGET_SYSTEM,
) -> tuple[DeliveryTask | None, QualitySample | None]:
    """从批次存储投递数据，并对投递结果进行质量抽样。"""
    catalog.register(batch)
    outcome = deliver_to_target(batch, target_system)
    return outcome.task, outcome.sample


def _make_sample(batch: StorageBatch, receipt: DeliveryReceipt) -> QualitySample:
    """以有效收讫凭证为准创建抽样计划，样本量按实际入仓行数计算。"""
    row_count = receipt.delivered_row_count or batch.row_count
    sample_size = max(1, int(row_count * 0.1)) if row_count > 0 else 0
    return QualitySample(
        plan_id=f"QS-{batch.batch_id}-{receipt.target_system}-{receipt.batch_version}",
        target_batch=batch.batch_id,
        sample_ratio=0.1,
        sample_method=SampleMethod.RANDOM,
        sample_size=sample_size,
        last_sampled=datetime.now(timezone.utc).isoformat(),
        status="待采样",
        target_system=receipt.target_system,
        receipt_id=receipt.receipt_id,
        data_version=receipt.batch_version,
        delivery_task_id=receipt.task_id,
    )


def _adopt_existing_sample(receipt: DeliveryReceipt) -> QualitySample | None:
    """复用重试前已经存在的抽样计划，保证旧计划仍能查到。"""
    for row in store.rows("quality_sample"):
        if str(row.get("目标批次")) != receipt.batch_id:
            continue
        row_target = str(row.get("目标系统") or "")
        row_version = str(row.get("数据版本") or "")
        if row_target not in ("", receipt.target_system):
            continue
        if row_version not in ("", receipt.batch_version):
            continue
        sample = QualitySample.from_seed_row(row)
        sample.target_system = receipt.target_system
        sample.receipt_id = receipt.receipt_id
        sample.data_version = receipt.batch_version
        sample.status = "待采样"
        sample.check_result = ""
        return sample
    return None


def _run_sample_check(batch: StorageBatch, qs: QualitySample) -> None:
    """从凭证记录的入仓数据里抽样校验。"""
    receipt = ledger.receipt_for(qs.target_batch, qs.data_version, qs.target_system)
    pool = list(receipt.records) if receipt is not None else list(batch.records)
    if not pool or qs.sample_size <= 0:
        qs.status = "待采样"
        return
    sampled = random.sample(pool, min(qs.sample_size, len(pool)))
    valid_count = sum(1 for s in sampled if s.is_valid())
    actual_ratio = valid_count / len(sampled) if sampled else 0
    qs.check_result = f"抽样{len(sampled)}条，有效{valid_count}条（数据版本{qs.data_version}）"
    if batch.status != "校验通过" or actual_ratio < (1 - qs.deviation_threshold):
        qs.status = "偏差告警"
    else:
        qs.status = "已通过"


def _sample_payload(qs: QualitySample | None) -> dict[str, Any] | None:
    if qs is None:
        return None
    return {
        "plan_id": qs.plan_id,
        "target_batch": qs.target_batch,
        "target_system": qs.target_system,
        "receipt_id": qs.receipt_id,
        "data_version": qs.data_version,
        "delivery_task_id": qs.delivery_task_id,
        "sample_size": qs.sample_size,
        "check_result": qs.check_result,
        "status": qs.status,
    }


def _persist_delivery_row(
    task: DeliveryTask, batch: StorageBatch, receipt: DeliveryReceipt | None,
) -> None:
    """按业务键写回投递台账：已有任务行就更新，绝不追加重复编号。"""
    rows = store.rows("data_delivery")
    target_system = receipt.target_system if receipt else task.target_system
    row = _delivery_task_row(batch, target_system)
    payload = {
        "任务编号": task.task_id,
        "数据来源": batch.batch_id,
        "目标系统": target_system,
        "投递格式": task.delivery_format.value,
        "投递频率": task.frequency,
        "分区策略": task.partition_strategy,
        "最近投递": task.last_delivery,
        "投递状态": task.status,
        "status": task.status,
        "pending": False,
        "abnormal": task.status == "投递失败",
        "批次关联": batch.batch_id,
        "批次版本": batch.batch_version,
        "投递行数": task.delivered_row_count,
        "收讫凭证": receipt.receipt_id if receipt else "",
    }
    if row is not None:
        row.update(payload)
        row["id"] = int(row.get("id", 0))
        return
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        **payload,
    })


def _persist_pending_row(
    batch: StorageBatch, target_system: str, stage: str,
) -> None:
    """未确认的投递进度写入投递台账，状态为待对账，不占有效凭证。"""
    rows = store.rows("data_delivery")
    row = _delivery_task_row(batch, target_system)
    payload = {
        "任务编号": f"DEL-{batch.batch_id}-{target_system}",
        "数据来源": batch.batch_id,
        "目标系统": target_system,
        "投递格式": DeliveryFormat.CSV.value,
        "投递频率": "manual",
        "分区策略": "by_date",
        "最近投递": datetime.now(timezone.utc).isoformat(),
        "投递状态": "待对账",
        "status": "待对账",
        "pending": True,
        "abnormal": False,
        "批次关联": batch.batch_id,
        "批次版本": batch.batch_version,
        "投递行数": batch.row_count,
        "收讫凭证": "",
        "待对账阶段": stage,
    }
    if row is not None:
        row.update(payload)
        return
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        **payload,
    })


def _mark_pending_resolved(batch: StorageBatch, target_system: str) -> None:
    """对账完成后清掉台账上的待对账标记。"""
    row = _delivery_task_row(batch, target_system)
    if row is not None:
        row["待对账阶段"] = ""


def _persist_sample_row(qs: QualitySample) -> None:
    """按 (批次, 目标, 版本) 写回抽样清单，重试时更新已有计划。"""
    rows = store.rows("quality_sample")
    payload = {
        "计划编号": qs.plan_id,
        "目标批次": qs.target_batch,
        "抽样比例": str(qs.sample_ratio),
        "抽样方式": qs.sample_method.value,
        "样本数量": qs.sample_size,
        "检验结果": qs.check_result,
        "偏差阈值": str(qs.deviation_threshold),
        "最近采样": qs.last_sampled,
        "status": qs.status,
        "pending": False,
        "abnormal": qs.status == "偏差告警",
        "目标系统": qs.target_system,
        "收讫凭证": qs.receipt_id,
        "数据版本": qs.data_version,
        "投递任务编号": qs.delivery_task_id,
    }
    for row in rows:
        same_plan = str(row.get("计划编号")) == qs.plan_id
        same_scope = (
            str(row.get("目标系统") or "") == qs.target_system
            and str(row.get("数据版本") or qs.data_version) == qs.data_version
        )
        if same_plan or (
                str(row.get("目标批次")) == qs.target_batch and same_scope):
            row.update(payload)
            return
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        **payload,
    })


# ---------------------------------------------------------------------------
# 流水线 5: incr_crawl → crawl_job → diff_check
# ---------------------------------------------------------------------------

def run_incremental_flow(cursor: IncrCursor,
                         previous_batch: StorageBatch | None,
                         current_data: list[CleanedContent]) -> DiffRecord | None:
    """增量采集流程：比较增量游标与当前批次，生成差异检测记录。"""
    if previous_batch is None or not previous_batch.records:
        return None

    prev_fields = {r.source_url: r.fields for r in previous_batch.records}
    curr_fields = {r.source_url: r.fields for r in current_data}

    # 检测新增
    new_urls = set(curr_fields) - set(prev_fields)
    # 检测变更
    changed_urls = {url for url in (set(curr_fields) & set(prev_fields))
                    if curr_fields[url] != prev_fields[url]}
    # 检测删除
    removed_urls = set(prev_fields) - set(curr_fields)

    total_changes = len(new_urls) + len(changed_urls) + len(removed_urls)
    if total_changes == 0:
        return None

    diff = DiffRecord(
        record_id=f"DIFF-{cursor.cursor_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        target_site=cursor.target_site,
        page_url=",".join(list(new_urls | changed_urls)[:5]),
        change_type=_classify_change_type(len(new_urls), len(changed_urls), len(removed_urls)),
        old_summary=f"原{len(prev_fields)}条记录",
        new_summary=f"新{len(curr_fields)}条记录，新增{len(new_urls)}变更{len(changed_urls)}删除{len(removed_urls)}",
        affected_fields=list(curr_fields[next(iter(curr_fields))]) if curr_fields else [],
    )

    _persist_diff(diff)

    # 更新游标
    cursor.new_count = len(new_urls)
    cursor.updated_count = len(changed_urls)
    cursor.last_cursor = cursor.current_cursor
    cursor.current_cursor = datetime.now(timezone.utc).isoformat()
    cursor.last_sync = cursor.current_cursor
    cursor.status = "已追上"

    return diff


def _classify_change_type(new_count: int, changed_count: int, removed_count: int) -> str:
    if new_count > 0 and changed_count == 0 and removed_count == 0:
        return "新增内容"
    if changed_count > 0 and new_count == 0 and removed_count == 0:
        return "内容变更"
    if removed_count > 0 and new_count == 0 and changed_count == 0:
        return "内容删除"
    return "混合变更"


# ---------------------------------------------------------------------------
# 流水线 6: data_archive ← batch_store
# ---------------------------------------------------------------------------

def archive_batch(batch: StorageBatch) -> ArchivePolicy | None:
    """从批次存储归档符合条件的批次数据。"""
    # 优先查找与当前批次相关的策略
    matched_policies: list[ArchivePolicy] = []
    policies: list[ArchivePolicy] = []
    for row in store.rows("data_archive"):
        p = ArchivePolicy.from_seed_row(row)
        if p.status in ("待触发", "已完成"):
            policies.append(p)
            if batch.batch_id in p.data_scope or p.data_scope == batch.batch_id:
                matched_policies.append(p)

    # 优先使用匹配的策略
    if matched_policies:
        policy = matched_policies[0]
        policy.last_archive = datetime.now(timezone.utc).isoformat()
        policy.status = "已完成"
        return policy

    if not policies:
        # 创建默认归档策略
        policy = ArchivePolicy(
            policy_id=f"ARCH-{batch.batch_id}",
            data_scope=batch.batch_id,
            archive_cycle="on_demand",
            retention_days=90,
            storage_location=f"/archive/{batch.batch_id}/",
            last_archive=datetime.now(timezone.utc).isoformat(),
            status="已完成",
        )
        _persist_archive_policy(policy)
        return policy

    policy = policies[0]
    policy.last_archive = datetime.now(timezone.utc).isoformat()
    policy.status = "已完成"
    return policy


# ---------------------------------------------------------------------------
# 流水线 7: crawl_cost ← proxy_pool
# ---------------------------------------------------------------------------

def compute_crawl_cost(job: CrawlJobSpec, proxy: ProxyNode | None) -> CostRecord:
    """根据采集作业和代理使用情况计算采集成本。"""
    proxy_hours = job.completed_pages * 0.01 if proxy else 0
    data_mb = job.completed_pages * 0.5  # 每页 0.5MB 估算

    record = CostRecord(
        record_id=f"COST-{job.job_id}",
        site=job.site.domain,
        billing_cycle=datetime.now(timezone.utc).strftime("%Y-%m"),
        request_count=job.completed_pages + job.failed_pages,
        data_volume_mb=round(data_mb, 2),
        proxy_usage_hours=round(proxy_hours, 2),
        estimated_cost=round(data_mb * 0.001 + proxy_hours * 0.5, 4),
        status="已核算" if job.status in ("已完成", "部分失败") else "统计中",
    )
    _persist_cost(record)
    return record


# ---------------------------------------------------------------------------
# 流水线 8: crawl_job → scheduler
# ---------------------------------------------------------------------------

def schedule_job(job: CrawlJobSpec) -> SchedulePlan | None:
    """为采集作业创建调度计划。"""
    # 找关联的调度计划
    for row in store.rows("scheduler"):
        if str(row.get("关联作业")) == job.job_id:
            plan = SchedulePlan.from_seed_row(row)
            plan.next_run = datetime.now(timezone.utc).isoformat()
            plan.status = "执行中" if job.status in ("待执行", "采集执行") else "已完成"
            return plan

    # 创建新的调度计划
    plan = SchedulePlan(
        plan_id=f"SCHED-{job.job_id}",
        linked_job_id=job.job_id,
        cron_expr="0 */6 * * *",  # 每6小时
        next_run=datetime.now(timezone.utc).isoformat(),
        status="执行中" if job.status in ("待执行", "采集执行") else "已完成",
    )
    _persist_schedule(plan)
    return plan


# ---------------------------------------------------------------------------
# 跨模块失败处理和恢复逻辑
# ---------------------------------------------------------------------------

def recover_failed_batch(batch: StorageBatch) -> StorageBatch | None:
    """恢复失败的批次：重新对缺失记录执行抽取和清洗。"""
    if batch.status != "校验失败":
        return None

    # 找出失败记录
    failed_records = [r for r in batch.records if not r.is_valid()]
    if not failed_records:
        return None

    # 重试：对失败记录重新填充缺失字段
    for record in failed_records:
        for field, value in list(record.fields.items()):
            if not value or value == "":
                record.fields[field] = f"recovered:{field}"
        # 恢复后清除之前清洗错误标记
        record.clean_errors = []

    # 重新校验
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(r.is_valid() for r in batch.records) else "校验失败"
    batch.stored_at = datetime.now(timezone.utc).isoformat()
    return batch


def handle_duplicate_batch(batch: StorageBatch) -> StorageBatch | None:
    """处理重复批次：检查是否与已有批次产生冲突。"""
    existing_batch_ids = set()
    for row in store.rows("batch_store"):
        existing_batch_ids.add(str(row.get("批次编号")))

    if batch.batch_id in existing_batch_ids:
        # 重复批次：追加时间戳后缀
        suffix = datetime.now(timezone.utc).strftime("%H%M%S")
        batch.batch_id = f"{batch.batch_id}-{suffix}"
        batch.storage_path = f"/data/batches/{batch.source_job_id}-retry/"

    return batch


def handle_interrupted_pipeline(job: CrawlJobSpec) -> CrawlJobSpec:
    """处理中断的采集管线：恢复未完成的工作。"""
    if job.status not in ("部分失败", "待执行"):
        return job

    # 找到失败的页面并标记重试
    if job.failed_pages > 0:
        job.failed_pages = max(0, job.failed_pages - 1)  # 重试一页
        job.status = "采集执行"
        if job.failed_pages + job.completed_pages == 0:
            job.status = "已完成"

    return job


# ---------------------------------------------------------------------------
# 内部持久化辅助
# ---------------------------------------------------------------------------

def _persist_batch(batch: StorageBatch) -> None:
    rows = store.rows("batch_store")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "批次编号": batch.batch_id,
        "来源作业": batch.source_job_id,
        "数据行数": batch.row_count,
        "存储路径": batch.storage_path,
        "压缩格式": batch.compression,
        "Schema版本": batch.schema_version,
        "批次版本": batch.batch_version,
        "校验摘要": batch.checksum[:16],
        "完整校验摘要": batch.checksum,
        "入库时间": batch.stored_at,
        "status": batch.status,
        "pending": False,
        "abnormal": batch.status == "校验失败",
    })


def _persist_fingerprint(fp: UrlFingerprint) -> None:
    rows = store.rows("url_dedup")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "指纹编号": fp.fingerprint_id,
        "源URL": fp.source_url,
        "指纹算法": fp.algorithm,
        "指纹哈希": fp.hash_value,
        "发现渠道": fp.discovery_channel,
        "首次发现": fp.first_seen,
        "重复次数": fp.duplicate_count,
        "去重状态": fp.status,
        "status": fp.status,
        "pending": False,
        "abnormal": False,
    })


def _persist_parse_log(log: ParseLogEntry) -> None:
    rows = store.rows("parse_log")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "记录编号": log.record_id,
        "来源URL": log.source_url,
        "解析规则": log.parse_rule_id,
        "解析结果": log.parse_result,
        "异常类型": log.error_type,
        "重试次数": log.retry_count,
        "错误详情": log.error_detail,
        "记录时间": log.recorded_at,
        "status": "成功" if not log.error_type else "格式异常",
        "pending": False,
        "abnormal": bool(log.error_type),
    })


def _persist_diff(diff: DiffRecord) -> None:
    rows = store.rows("diff_check")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": diff.record_id, "目标站点": diff.target_site,
        "检测页面": diff.page_url, "变更类型": diff.change_type,
        "旧内容摘要": diff.old_summary, "新内容摘要": diff.new_summary,
        "影响字段": ",".join(diff.affected_fields), "检测时间": diff.detected_at,
        "status": diff.status, "pending": False, "abnormal": False,
    })


def _persist_archive_policy(p: ArchivePolicy) -> None:
    rows = store.rows("data_archive")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "策略编号": p.policy_id, "数据范围": p.data_scope,
        "归档周期": p.archive_cycle, "归档格式": str(p.archive_format),
        "保留时长": str(p.retention_days), "存储位置": p.storage_location,
        "压缩方式": p.compression_method, "最后归档": p.last_archive,
        "status": p.status, "pending": False, "abnormal": False,
    })


def _persist_cost(c: CostRecord) -> None:
    rows = store.rows("crawl_cost")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": c.record_id, "采集站点": c.site,
        "计费周期": c.billing_cycle, "请求次数": str(c.request_count),
        "数据流量": str(c.data_volume_mb), "代理用量": str(c.proxy_usage_hours),
        "费用预估": c.estimated_cost, "账单状态": c.status,
        "status": c.status, "pending": False,
        "abnormal": c.estimated_cost > 100,
    })


def _persist_schedule(plan: SchedulePlan) -> None:
    rows = store.rows("scheduler")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": plan.plan_id, "关联作业": plan.linked_job_id,
        "Cron表达式": plan.cron_expr, "时区设置": plan.timezone_setting,
        "依赖检查": plan.dependency_check, "失败通知": plan.failure_notify,
        "并发控制": plan.concurrency_control, "下次执行": plan.next_run,
        "status": plan.status, "pending": False, "abnormal": False,
    })