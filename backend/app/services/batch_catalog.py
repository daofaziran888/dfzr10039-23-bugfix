"""批次目录：保存已落盘批次的数据版本，供投递回执详情和抽样工作台重新进入。

内存版平台里批次记录会随请求消失，投递链路需要按 (批次编号, 批次版本)
找回批次数据。目录只缓存已经过校验的批次，不参与投递资格判定。
"""
from __future__ import annotations

from datetime import datetime, timezone

from app.models import CleanedContent, StorageBatch
from app.store import store


class BatchCatalog:
    def __init__(self) -> None:
        self._batches: dict[tuple[str, str], StorageBatch] = {}

    def register(self, batch: StorageBatch) -> StorageBatch:
        self._batches[(batch.batch_id, batch.batch_version or "v1")] = batch
        return batch

    def get(self, batch_id: str, batch_version: str = "v1") -> StorageBatch | None:
        batch = self._batches.get((batch_id, batch_version or "v1"))
        if batch is not None:
            return batch
        # 目录里没有时退回批次台账，至少能还原基础字段（不含记录明细）。
        for row in store.rows("batch_store"):
            if str(row.get("批次编号")) != batch_id:
                continue
            if str(row.get("批次版本") or "v1") != (batch_version or "v1"):
                continue
            batch = StorageBatch.from_seed_row(row)
            self._batches[(batch_id, batch_version or "v1")] = batch
            return batch
        return None

    def batches(self) -> list[StorageBatch]:
        return list(self._batches.values())


catalog = BatchCatalog()


def ensure_demo_batches() -> None:
    """准备演示用的已校验批次 B-A v1，起服务后可以直接投递。"""
    if catalog.get("B-A", "v1") is not None:
        return
    records = [
        CleanedContent(
            source_url=f"https://b-a.example.com/article/{i}",
            extract_rule_id="CONT-BA-01",
            fields={"title": f"B-A 样例文章 {i}", "body": f"正文 {i}"},
        )
        for i in range(1, 401)
    ]
    batch = StorageBatch(
        batch_id="B-A",
        source_job_id="JOB-BA",
        row_count=len(records),
        storage_path="/data/batches/JOB-BA/",
        compression="gzip",
        schema_version="v1",
        batch_version="v1",
        stored_at=datetime.now(timezone.utc).isoformat(),
        status="校验通过",
        records=records,
    )
    batch.checksum = batch.compute_checksum()
    catalog.register(batch)

    rows = store.rows("batch_store")
    if not any(str(row.get("批次编号")) == "B-A" for row in rows):
        rows.append({
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            "批次编号": "B-A",
            "来源作业": "JOB-BA",
            "数据行数": len(records),
            "存储路径": "/data/batches/JOB-BA/",
            "压缩格式": "gzip",
            "Schema版本": "v1",
            "批次版本": "v1",
            "校验摘要": batch.checksum[:16],
            "完整校验摘要": batch.checksum,
            "入库时间": batch.stored_at,
            "status": "校验通过",
            "pending": False,
            "abnormal": False,
        })
