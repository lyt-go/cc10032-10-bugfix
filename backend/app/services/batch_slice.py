"""batch_slice 领域服务：批次分片管理与数据切割。

业务逻辑：
1. 批次分片配置：源表→分片键→起始/结束值→并行线程规划
2. 数据切割链路：接收 stream_source 元信息 → 按分片键切割 → 输出数据行到下游
3. 失败场景：分片起始>结束值冲突、脏行清洗、重复批次拒绝、写入中断
4. 与上下游流水线：消费 stream_source 元信息；为 merge_plan 提供批式来源数据

跨模块数据流：
  stream_source → batch_slice（消费流式源元信息，进行批式切割）
  batch_slice → merge_plan（提供批式来源数据，参与流批合并）
  batch_slice → watermark（记录批次处理水位）
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class BatchSliceService(BaseModuleService):
    MODULE = "batch_slice"
    ENTITY = "分片批次"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["批次编号", "源表名称", "分片键列", "分片起始值", "分片结束值", "预计行数", "并行线程", "完成时间"]
    REQUIRED_FIELDS = ["批次编号", "源表名称", "分片键列"]
    STATUS_ORDER = ["待执行", "执行中", "已完成", "已跳过"]
    ACTION_RULES = {"发起执行": "执行中", "标记完成": "已完成", "跳过批次": "已跳过"}
    NEGATIVE_ACTIONS = []

    # 跨模块流水线
    PIPELINE = "batch_slicing"
    UPSTREAM_MODULES = ["stream_source"]
    DOWNSTREAM_MODULES = ["merge_plan", "watermark"]
    PRODUCES_ARTIFACT = "批次分片数据行"

    # 已处理的batch_id注册表（防重复）
    _processed_batches: set[str] = set()
    # 投递版本序号（防同毫秒批次号冲突）
    _publish_seq: int = 0

    def execute_slice(self, entry_id: int) -> dict[str, Any]:
        """执行分片切割：从源表生成模拟数据行。

        失败场景：
        - 起始值 >= 结束值 → 分片参数无效
        - 批次状态非待执行 → 状态冲突
        - 重复批次 → 拒绝执行
        - 写入中断模拟 → 部分数据丢失
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"批次 {entry_id} 不存在"}
        if entry.get("status") != "待执行":
            return {"ok": False, "reason": f"批次 {entry_id} 状态为 {entry.get('status')}，非待执行"}

        batch_no = str(entry.get("批次编号", ""))
        if batch_no in self._processed_batches:
            return {"ok": False, "reason": f"重复批次 {batch_no}，已处理过"}

        start_str = str(entry.get("分片起始值", "0"))
        end_str = str(entry.get("分片结束值", "0"))
        try:
            start_val = float(start_str)
            end_val = float(end_str)
        except ValueError:
            start_val = 0.0
            end_val = 100.0

        if start_val >= end_val:
            error_msg = f"分片参数无效: 起始值 {start_val} >= 结束值 {end_val}"
            entry["abnormal"] = True
            entry["status"] = "待执行"
            return {"ok": False, "reason": error_msg}

        rows_count = int(end_val - start_val)
        if rows_count > 100000:
            return {"ok": False, "reason": f"分片范围过大({rows_count}行)，建议缩小"}

        # 生成模拟数据行
        data_rows = []
        dirty_rows = []
        for i in range(rows_count):
            val = start_val + i
            if i % 7 == 0 and i > 0:
                # 模拟脏行：空值或异常值
                dirty_rows.append({"row_index": i, "value": val, "issue": "空值分片键"})
                continue
            data_rows.append({
                "batch": batch_no,
                "source_table": entry.get("源表名称"),
                "slice_key": str(entry.get("分片键列")),
                "value": val,
                "ts": time.time() - i * 0.1,
                "batch_id": batch_no,
            })

        # 模拟写入中断（每第5个批次）
        if entry_id % 5 == 0 and data_rows:
            # 保留一半数据，模拟中断
            data_rows = data_rows[:len(data_rows) // 2]
            self._interrupted_write = True

        # 通过流水线写入数据产物，附带来源证明供下游按归属核验
        batch_id = f"{batch_no}_{int(time.time() * 1000)}"
        self.produce_artifact(data_rows, batch_id, provenance=self._provenance(entry))

        self._processed_batches.add(batch_no)
        entry["status"] = "执行中"
        entry["pending"] = True

        return {
            "ok": True,
            "batch": batch_no,
            "rows_ingested": len(data_rows),
            "dirty_rows": len(dirty_rows),
            "total_range": rows_count,
            "pipeline": self.PIPELINE,
        }

    def _provenance(self, entry: dict[str, Any]) -> dict[str, Any]:
        """本分片产物的来源证明：产出条目身份 + 源表名称。"""
        return {
            "producer_module": self.MODULE,
            "producer_entry_id": int(entry.get("id", 0)),
            "source_kind": "batch",
            "批次编号": entry.get("批次编号"),
            "源表名称": entry.get("源表名称"),
        }

    def publish_slice(self, entry_id: int, rows: list[dict[str, Any]]) -> dict[str, Any]:
        """投递一个归属明确的新版分片：以登记条目身份发布显式数据行。

        与计划执行（execute_slice）共用同一条流水线，但版本独立，
        供上游在不重跑整批的情况下向合并侧投递新的分片版本。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"批次 {entry_id} 不存在"}
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            return {"ok": False, "reason": "分片数据行必须是对象数组"}

        batch_no = str(entry.get("批次编号", ""))
        self._publish_seq += 1
        batch_id = f"{batch_no}_PUB_{int(time.time() * 1000)}_{self._publish_seq}"
        result = self.produce_artifact(rows, batch_id, provenance=self._provenance(entry))
        return {
            "ok": True,
            "batch": batch_no,
            "version": result.get("version"),
            "rows_ingested": result.get("ingested", 0),
            "dirty_details": result.get("dirty_details", []),
            "pipeline": self.PIPELINE,
        }

    def consume_stream_sources(self) -> dict[str, Any]:
        """消费上游 stream_source 模块的已启用源清单，确定批次切割策略"""
        sources = store.rows("stream_source")
        enabled = [s for s in sources if s.get("status") == "已启用"]
        source_topics = [s.get("Topic名称") for s in enabled if s.get("Topic名称")]
        return {
            "enabled_stream_sources": len(enabled),
            "topics": source_topics,
            "mapped_batches": [r.get("批次编号") for r in store.rows(self.MODULE) if r.get("status") == "待执行"],
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["stream_sources_consumed"] = self.consume_stream_sources()
        base["processed_batches"] = len(self._processed_batches)
        return base