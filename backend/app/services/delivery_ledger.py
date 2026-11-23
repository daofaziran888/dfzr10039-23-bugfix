"""投递收讫台账：一条批次版本在一个目标系统上只允许一份有效收讫凭证。

投递链路分三段，任何一段被打断都可以从这里恢复：

1. 投递资格：本地投递任务按 (批次编号, 批次版本, 目标系统) 唯一登记，
   已收讫的批次版本不会因为重试/恢复/并发而再领一份投递资格。
2. 接收方入仓：接收仓库按业务键幂等入仓，同一批次版本并发提交只记一次，
   已经入仓的批次版本只补发收执，不重复入仓。
3. 本地写回：收讫凭证写回台账与投递任务；写回前中断的提交会留下
   “待对账”进度，恢复任务通过接收仓库核对该版本是否已入仓来补全。

写回顺序固定为“接收仓库入仓 → 收讫凭证 → 投递任务”，本地写回中断时
最多留下待对账的孤儿进度，不会出现两份有效凭证，也不会出现重复任务编号。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any, Callable

from app.models import DeliveryReceipt, DeliveryTask, QualitySample, StorageBatch

BusinessKey = tuple[str, str, str]  # (目标系统, 批次编号, 批次版本)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class DeliveryLedger:
    """批次版本投递台账，同时扮演接收仓库的入仓核对端。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._reset_state()

    def _reset_state(self) -> None:
        # 接收仓库（远端）与本地台账分开存储，模拟“远端已入仓、本地未写回”。
        self._received: dict[str, dict[str, Any]] = {}
        self._receipts: dict[BusinessKey, DeliveryReceipt] = {}
        self._tasks: dict[BusinessKey, DeliveryTask] = {}
        self._samples: dict[BusinessKey, QualitySample] = {}
        self._pending: dict[BusinessKey, dict[str, Any]] = {}

    def reset(self) -> None:
        """清空接收仓库与台账（测试用）。"""
        with self._lock:
            self._reset_state()

    # ------------------------------------------------------------------
    # 接收仓库：核对批次版本是否已入仓，可补发收执
    # ------------------------------------------------------------------

    def warehouse_has_version(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> bool:
        """接收仓库按 (目标, 批次编号, 批次版本) 核对是否已入仓。"""
        with self._lock:
            return self._key(batch_id, batch_version, target_system) in self._received

    def warehouse_intake(self, batch: StorageBatch, target_system: str) -> bool:
        """幂等入仓：同一目标上同一批次版本只记一次，返回是否首次入仓。"""
        key = self._key(batch.batch_id, batch.batch_version, target_system)
        with self._lock:
            if key in self._received:
                return False
            self._received[key] = {
                "batch_id": batch.batch_id,
                "batch_version": batch.batch_version,
                "target_system": target_system,
                "row_count": batch.row_count,
                "checksum": batch.checksum,
                "records": list(batch.records),
                "received_at": _now(),
            }
            return True

    def warehouse_version(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> dict[str, Any] | None:
        with self._lock:
            return self._received.get(
                self._key(batch_id, batch_version, target_system))

    def warehouse_versions(self, batch_id: str) -> list[str]:
        with self._lock:
            return [
                entry["batch_version"] for entry in self._received.values()
                if entry["batch_id"] == batch_id
            ]

    def reissue_receipt(self, receipt_id: str) -> dict[str, Any] | None:
        """接收仓库按已入仓记录补发收执，不修改入仓事实。"""
        with self._lock:
            receipt = self._find_receipt(receipt_id)
            if receipt is None:
                return None
            if not self.warehouse_has_version(
                    receipt.batch_id, receipt.batch_version, receipt.target_system):
                return None
            receipt.reissue_count += 1
            receipt.received_at = _now()
            receipt.reconciliation_status = "已对账"
            return {
                "receipt_id": receipt.receipt_id,
                "batch_id": receipt.batch_id,
                "batch_version": receipt.batch_version,
                "target_system": receipt.target_system,
                "reissue_count": receipt.reissue_count,
            }

    # ------------------------------------------------------------------
    # 投递资格
    # ------------------------------------------------------------------

    def claim(
        self,
        *,
        batch: StorageBatch,
        target_system: str,
        submission_token: str,
        persist: bool = True,
    ) -> tuple[DeliveryReceipt | None, str]:
        """为批次版本申请投递资格。

        - 已有有效收讫凭证：直接返回该凭证，不再产生新的投递资格。
        - 接收仓库已入仓但本地没有有效凭证：上次写回前中断，补全本地凭证。
        - 否则幂等入仓并开立凭证。

        `persist=False` 只返回临时凭证（仍处于“待确认”），由调用方在
        接收方回执确认后再写回，用于真实模拟本地写回中断。

        返回的判定文案：
        - "已收讫"：复用已有有效凭证；
        - "补全凭证"：接收仓库已入仓，本地凭证此前未写回；
        - "补发收执"：本地曾有凭证但失效，接收仓库按入仓事实补发；
        - "待确认"：首次提交，等回执确认后写回。
        """
        key = self._business_key(batch.batch_id, batch.batch_version, target_system)
        with self._lock:
            existing = self._receipts.get(key)
            if existing is not None and existing.is_valid():
                return existing, "已收讫"

            intake_before = self.warehouse_has_version(
                batch.batch_id, batch.batch_version, target_system)
            self.warehouse_intake(batch, target_system)
            if existing is not None:
                verdict = "补发收执"
                reissue_count = existing.reissue_count + 1
            elif intake_before:
                verdict = "补全凭证"
                reissue_count = 0
            else:
                verdict = "待确认" if not persist else "已收讫"
                reissue_count = 0
            receipt = self._open_receipt(
                batch,
                target_system,
                submission_token=submission_token,
                task_id=existing.task_id if existing else "",
                reissue_count=reissue_count,
            )
            if persist:
                self._receipts[key] = receipt
            return receipt, verdict

    def receipt_for(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> DeliveryReceipt | None:
        with self._lock:
            return self._receipts.get(
                self._business_key(batch_id, batch_version, target_system))

    def claim_status(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> str:
        """投递资格状态：已收讫 / 待对账 / 未投递。"""
        if self.pending_for(batch_id, batch_version, target_system):
            return "待对账"
        receipt = self.receipt_for(batch_id, batch_version, target_system)
        if receipt is not None and receipt.is_valid():
            return "已收讫"
        if receipt is not None:
            return "待对账"
        return "未投递"

    def effective_receipt(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> DeliveryReceipt | None:
        receipt = self.receipt_for(batch_id, batch_version, target_system)
        return receipt if receipt is not None and receipt.is_valid() else None

    # ------------------------------------------------------------------
    # 本地写回（可中断）
    # ------------------------------------------------------------------

    def persist_receipt(
        self, receipt: DeliveryReceipt, *, interrupt_before: str | None = None,
    ) -> bool:
        """把收讫凭证写回本地台账。

        `interrupt_before` 模拟写回被打断：命中步骤后返回 False，
        接收仓库的入仓事实保留，本地只留下待对账进度。
        """
        with self._lock:
            if interrupt_before == "receipt":
                return False
            current = self._receipts.get(receipt.business_key)
            # 台账里已有有效凭证时以台账为准：并发写回是幂等成功，
            # 不会覆盖、也不会新增第二份凭证。
            if current is not None and current.is_valid():
                return True
            # 没有有效凭证时才落盘；此前可能只有一份待对账的临时凭证。
            self._receipts[receipt.business_key] = receipt
            return True

    def register_task(
        self, task: DeliveryTask, receipt: DeliveryReceipt, *,
        interrupt_before: str | None = None,
    ) -> bool:
        """把投递任务与凭证关联写回本地台账，同一业务键只保留一条任务。"""
        with self._lock:
            if interrupt_before == "task":
                return False
            existing = self._tasks.get(receipt.business_key)
            task.task_id = task.task_id or (existing.task_id if existing else "")
            task.receipt_id = receipt.receipt_id
            task.batch_version = receipt.batch_version
            task.delivered_row_count = receipt.delivered_row_count
            task.last_delivery = receipt.received_at
            task.status = "已完成"
            # 凭证反向记录投递任务，抽样据此登记同一份任务的版本。
            receipt.task_id = task.task_id
            self._tasks[receipt.business_key] = task
            return True

    def task_for(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> DeliveryTask | None:
        with self._lock:
            return self._tasks.get(
                self._business_key(batch_id, batch_version, target_system))

    def tasks_for_batch(self, batch_id: str, batch_version: str) -> list[DeliveryTask]:
        with self._lock:
            return [
                task for key, task in self._tasks.items()
                if key[1] == batch_id and key[2] == (batch_version or "v1")
            ]

    def all_tasks(self) -> list[DeliveryTask]:
        with self._lock:
            return list(self._tasks.values())

    def receipts_for_batch(self, batch_id: str, batch_version: str) -> list[DeliveryReceipt]:
        with self._lock:
            return [
                receipt for key, receipt in self._receipts.items()
                if key[1] == batch_id and key[2] == (batch_version or "v1")
            ]

    # ------------------------------------------------------------------
    # 待对账进度：写回中断后由恢复任务消费
    # ------------------------------------------------------------------

    def record_pending(
        self, *, batch: StorageBatch, target_system: str,
        submission_token: str, stage: str,
    ) -> dict[str, Any]:
        """持久化未确认的投递进度，等待对账或恢复任务认领。"""
        progress = {
            "batch_id": batch.batch_id,
            "batch_version": batch.batch_version,
            "target_system": target_system,
            "submission_token": submission_token,
            "stage": stage,
            "row_count": batch.row_count,
            "checksum": batch.checksum,
            "recorded_at": _now(),
        }
        with self._lock:
            self._pending[self._business_key(
                batch.batch_id, batch.batch_version, target_system)] = progress
        return progress

    def pending_for(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> dict[str, Any] | None:
        with self._lock:
            return self._pending.get(
                self._business_key(batch_id, batch_version, target_system))

    def all_pending(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._pending.values())

    def clear_pending(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> None:
        with self._lock:
            self._pending.pop(
                self._business_key(batch_id, batch_version, target_system), None)

    def reconcile(self, batch: StorageBatch, target_system: str) -> DeliveryReceipt | None:
        """恢复任务核对接收仓库并补全本地凭证与投递任务。

        返回补全后的有效凭证；接收仓库确认已入仓才补凭证，否则保持待对账。
        入仓事实只有一份，所以这里补出来的凭证与首次提交的是同一份。
        """
        with self._lock:
            receipt = self.receipt_for(
                batch.batch_id, batch.batch_version, target_system)
            if receipt is None or not receipt.is_valid():
                if not self.warehouse_has_version(
                        batch.batch_id, batch.batch_version, target_system):
                    return None
                receipt = self._open_receipt(
                    batch,
                    target_system,
                    submission_token=receipt.submission_token if receipt else "",
                    task_id=receipt.task_id if receipt else "",
                    reissue_count=(receipt.reissue_count + 1) if receipt else 1,
                )
                self._receipts[receipt.business_key] = receipt
            self.clear_pending(batch.batch_id, batch.batch_version, target_system)
            return receipt

    # ------------------------------------------------------------------
    # 质量抽样：登记的样本数据版本取自有效凭证
    # ------------------------------------------------------------------

    def bind_sample(
        self, batch: StorageBatch, target_system: str, *,
        make_sample: Callable[[DeliveryReceipt], QualitySample] | None = None,
        adopt_existing: Callable[[DeliveryReceipt], QualitySample | None] | None = None,
    ) -> tuple[QualitySample | None, str]:
        """以有效收讫凭证为准登记抽样计划。

        - 只有有效凭证才允许登记（没有收讫就没有可抽样的数据版本）。
        - 同一目标上重试前已存在的计划继续复用，只刷新数据版本和凭证。
        """
        key = self._business_key(batch.batch_id, batch.batch_version, target_system)
        with self._lock:
            receipt = self._receipts.get(key)
            if receipt is None or not receipt.is_valid():
                return None, "未收讫"

            sample = self._samples.get(key)
            if sample is None:
                # 优先复用重试前已经存在的计划，保证旧计划编号仍能查到。
                sample = adopt_existing(receipt) if adopt_existing else None
            if sample is None and make_sample is not None:
                sample = make_sample(receipt)
            if sample is None:
                return None, "无可用抽样计划"
            self._samples[key] = sample

            sample.target_system = target_system
            sample.receipt_id = receipt.receipt_id
            sample.data_version = receipt.batch_version
            sample.delivery_task_id = receipt.task_id
            sample.last_sampled = _now()
            return sample, "已登记"

    def sample_for(
        self, batch_id: str, batch_version: str, target_system: str,
    ) -> QualitySample | None:
        with self._lock:
            return self._samples.get(
                self._business_key(batch_id, batch_version, target_system))

    def samples_for_batch(self, batch_id: str, batch_version: str = "") -> list[QualitySample]:
        with self._lock:
            return [
                sample for key, sample in self._samples.items()
                if key[1] == batch_id
                and (not batch_version or key[2] == (batch_version or "v1"))
            ]

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _open_receipt(
        self, batch: StorageBatch, target_system: str, submission_token: str, *,
        task_id: str, reissue_count: int, status: str = "已收讫",
    ) -> DeliveryReceipt:
        intake = self._received.get(
            self._key(batch.batch_id, batch.batch_version, target_system))
        return DeliveryReceipt(
            receipt_id=f"RCPT-{batch.batch_id}-{batch.batch_version}-{target_system}",
            batch_id=batch.batch_id,
            batch_version=batch.batch_version,
            target_system=target_system,
            delivered_row_count=int(intake["row_count"]) if intake else batch.row_count,
            checksum=str(intake["checksum"]) if intake else batch.checksum,
            received_at=str(intake["received_at"]) if intake else _now(),
            status=status,
            task_id=task_id,
            reissue_count=reissue_count,
            reconciliation_status="待对账" if status != "已收讫" else "已对账",
            submission_token=submission_token,
            records=list(intake["records"]) if intake else list(batch.records),
        )

    def _find_receipt(self, receipt_id: str) -> DeliveryReceipt | None:
        for receipt in self._receipts.values():
            if receipt.receipt_id == receipt_id:
                return receipt
        return None

    @staticmethod
    def _key(batch_id: str, batch_version: str, target_system: str) -> str:
        return f"{target_system}/{batch_id}@{batch_version or 'v1'}"

    @staticmethod
    def _business_key(
        batch_id: str, batch_version: str, target_system: str,
    ) -> BusinessKey:
        return (target_system, batch_id, batch_version or "v1")


ledger = DeliveryLedger()
