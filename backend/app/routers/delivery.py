"""投递回执与抽样工作台接口：围绕收讫凭证的查询、重试与对账。

这些接口不修改投递资格本身的判定规则，只把领域服务的编排结果暴露出来：
- 详情：一个批次版本在一个目标上的收讫状态、入仓次数与样本版本；
- 重试：已有成功凭证复用，未确认的进度先对账再补发收执；
- 恢复：模拟本地写回中断后的恢复，可与定时执行同时调用；
- 工作台：按有效凭证列出样本，数据版本可追溯到接收版本。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.integration import (
    DEFAULT_TARGET_SYSTEM, backfill_receipt, deliver_to_target, delivery_trail,
    receipt_detail, recover_delivery, retry_delivery, sample_workbench,
)
from app.schemas import ActionResult, DeliveryActionPayload
from app.services.batch_catalog import catalog

router = APIRouter(prefix="/api/delivery", tags=["投递回执"])


def _require_batch(batch_id: str, version: str):
    batch = catalog.get(batch_id, version or "v1")
    if batch is None:
        raise HTTPException(status_code=404, detail=f"批次 {batch_id}@{version} 不存在")
    return batch


def _outcome_payload(outcome) -> dict[str, Any]:
    return {
        "ok": outcome.ok,
        "message": outcome.message,
        "claimed": outcome.claimed,
        "reissued": outcome.reissued,
        "reconciled": outcome.reconciled,
        "intake_count": outcome.intake_count,
        "pending_stage": (outcome.pending or {}).get("stage", ""),
        "receipt": {
            "receipt_id": outcome.receipt.receipt_id,
            "batch_version": outcome.receipt.batch_version,
            "target_system": outcome.receipt.target_system,
            "status": outcome.receipt.status,
            "reissue_count": outcome.receipt.reissue_count,
        } if outcome.receipt else None,
        "task_id": outcome.task.task_id if outcome.task else "",
        "sample": {
            "plan_id": outcome.sample.plan_id,
            "receipt_id": outcome.sample.receipt_id,
            "data_version": outcome.sample.data_version,
            "status": outcome.sample.status,
        } if outcome.sample else None,
    }


@router.get("/{batch_id}/{batch_version}/targets/{target_system}")
def get_receipt_detail(
    batch_id: str, batch_version: str, target_system: str,
) -> dict[str, Any]:
    batch = _require_batch(batch_id, batch_version)
    detail = receipt_detail(batch, target_system)
    if detail is None:
        raise HTTPException(
            status_code=404, detail=f"{batch_id}@{batch_version} 尚未向 {target_system} 投递")
    return detail


@router.get("/{batch_id}/{batch_version}/trail")
def get_delivery_trail(batch_id: str, batch_version: str) -> list[dict[str, Any]]:
    _require_batch(batch_id, batch_version)
    return delivery_trail(batch_id, batch_version)


@router.get("/{batch_id}/{batch_version}/samples")
def get_sample_workbench(batch_id: str, batch_version: str) -> list[dict[str, Any]]:
    _require_batch(batch_id, batch_version)
    return sample_workbench(batch_id, batch_version)


@router.post("/{batch_id}/{batch_version}/targets/{target_system}/actions")
def run_delivery_action(
    batch_id: str, batch_version: str, target_system: str,
    payload: DeliveryActionPayload,
) -> ActionResult:
    batch = _require_batch(batch_id, batch_version)
    action = payload.action.strip()
    token = payload.submission_token or ""

    if action == "投递":
        outcome = deliver_to_target(
            batch, target_system, submission_token=token,
            interrupt_before=payload.interrupt_before)
    elif action == "重试投递":
        outcome = retry_delivery(batch, target_system, submission_token=token)
    elif action == "恢复投递":
        outcome = recover_delivery(batch, target_system, submission_token=token)
    elif action == "补发收执":
        detail = receipt_detail(batch, target_system)
        receipt_id = (detail or {}).get("receipt_id", "")
        if not receipt_id:
            raise HTTPException(status_code=409, detail="该目标还没有可补发的收执")
        issued = backfill_receipt(receipt_id, target_system)
        return ActionResult(
            ok=issued is not None,
            message=f"收执已补发（第{issued['reissue_count']}次）" if issued else "补发失败",
            entry=issued,
        )
    else:
        raise HTTPException(status_code=400, detail=f"不支持的投递动作：{action}")

    detail = receipt_detail(batch, target_system)
    return ActionResult(
        ok=outcome.ok,
        message=outcome.message,
        entry={"outcome": _outcome_payload(outcome), "detail": detail},
    )
