"""merge_plan 领域服务：流批合并策略。

业务逻辑：
1. 流批合并：流式来源 + 批式来源 按合并键做去重/冲突解决
2. 合并链路：stream_lookup结果 + batch_slice数据 → merge_plan去重
3. 失败场景：冲突版本、合并窗口超限、去重策略冲突
4. 与上下游流水线：消费 stream_lookup 和 batch_slice；输出合并后数据

来源归属（修复要点）：
- 合并只允许读取归属已核验的输入：批式来源对应 batch_slicing 流水线中
  源表名称一致的已发布分片版本，流式来源对应 stream_lookup_join 流水线中
  左流来源一致的已发布关联版本，不再直接读全局共享流水线的最早产物。
- 历史产物没有来源证明时按行内源名称兼容核对；重名或缺失且无法证明
  归属的记录留待核对，本计划的读取权限不扩大到归属未确认的来源。
- 上游并发发布新版本时，每一轮发布整轮采用同一组已核验版本；
  已打开的合并预览保留旧来源证明，发布时把实际使用版本与待补范围
  回写到合并输入清单，分片版本与关联版本分别记录。

跨模块数据流：
  batch_slice + stream_lookup → merge_plan（流批合并）
"""
from __future__ import annotations

import hashlib
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class MergePlanService(BaseModuleService):
    MODULE = "merge_plan"
    ENTITY = "合并计划"
    OPERATION_TERM = "去重"
    LIST_FIELDS = ["计划编号", "流式来源", "批式来源", "合并键列", "冲突解决", "输出路径", "合并窗口", "去重策略"]
    REQUIRED_FIELDS = ["计划编号", "流式来源", "批式来源"]
    STATUS_ORDER = ["草稿", "待验证", "运行中", "已完成"]
    ACTION_RULES = {"提交验证": "待验证", "启动合并": "运行中", "标记完成": "已完成"}
    NEGATIVE_ACTIONS = []

    PIPELINE = "stream_batch_merge"
    UPSTREAM_MODULES = ["batch_slice", "stream_lookup"]
    PRODUCES_ARTIFACT = "流批合并记录"

    # 已处理的键值对版本追踪（防冲突）
    _merge_versions: dict[str, str] = {}
    # 发布批次序号（防同毫秒批次号冲突）
    _merge_seq: int = 0

    # 两个输入侧的核验配置：侧标签 → 流水线 / 产出模块 / 源名称字段 / 计划声明字段
    _SIDE_CONFIG = (
        {"kind": "batch", "label": "分片", "pipeline": "batch_slicing",
         "module": "batch_slice", "name_field": "源表名称", "plan_field": "批式来源"},
        {"kind": "stream", "label": "关联", "pipeline": "stream_lookup_join",
         "module": "stream_lookup", "name_field": "左流来源", "plan_field": "流式来源"},
    )

    # ---- 来源核验 ----
    @staticmethod
    def _historical_names(publication: dict[str, Any], kind: str) -> set[str]:
        """历史产物没有来源证明，从行内提取源名称（兼容旧版产物）。"""
        fields = ("source_table", "源表名称") if kind == "batch" else ("left_source", "左流来源")
        names: set[str] = set()
        for row in publication.get("rows", []):
            for field in fields:
                value = row.get(field)
                if value:
                    names.add(str(value))
        return names

    def _verify_publication(
        self,
        publication: dict[str, Any],
        side: dict[str, str],
        source_name: str,
    ) -> dict[str, str] | None:
        """核验一条发布是否可归为本计划输入。

        返回 None 表示属于其他计划；否则给出 已核验 / 留待核对 结论。
        """
        name_field = side["name_field"]
        provenance = publication.get("provenance")
        if provenance:
            if str(provenance.get(name_field) or "") != source_name:
                return None
            producer_id = provenance.get("producer_entry_id")
            producer = None
            if producer_id is not None:
                try:
                    producer = store.find(side["module"], int(producer_id))
                except (TypeError, ValueError):
                    producer = None
            if producer is None:
                return {"status": "留待核对",
                        "reason": f"来源证明指向的{side['module']}条目缺失，无法证明归属"}
            if str(producer.get(name_field) or "") != source_name:
                return {"status": "留待核对",
                        "reason": "来源证明与登记条目的源名称不一致"}
            return {"status": "已核验", "reason": "来源证明与登记条目一致"}

        # 历史产物：只能按行内源名称核对归属
        if source_name not in self._historical_names(publication, side["kind"]):
            return None
        claimants = [
            row for row in store.rows(side["module"])
            if str(row.get(name_field) or "") == source_name
        ]
        if len(claimants) == 1:
            return {"status": "已核验", "reason": "历史产物按源名称归属（兼容旧版）"}
        if not claimants:
            return {"status": "留待核对",
                    "reason": f"源名称 {source_name} 无登记条目，归属缺失无法证明"}
        return {"status": "留待核对",
                "reason": f"源名称 {source_name} 存在 {len(claimants)} 个重名条目，归属无法证明"}

    def _resolve_inputs(self, entry: dict[str, Any]) -> dict[str, Any]:
        """按计划的来源归属解析两个输入侧的已核验版本与留待核对记录。"""
        resolved: dict[str, Any] = {"sides": {}, "pending_review": []}
        for side in self._SIDE_CONFIG:
            source_name = str(entry.get(side["plan_field"]) or "")
            verified: list[dict[str, Any]] = []
            pending: list[dict[str, Any]] = []
            if source_name:
                pipeline = store.pipeline(side["pipeline"])
                publications = pipeline.publications() if pipeline else []
                for publication in publications:
                    verdict = self._verify_publication(publication, side, source_name)
                    if verdict is None:
                        continue
                    item = {
                        "version": publication["version"],
                        "batch_id": publication["batch_id"],
                        "rows": publication["rows"],
                        "row_count": publication["row_count"],
                        "ts": publication["ts"],
                        "provenance": publication.get("provenance"),
                        "reason": verdict["reason"],
                    }
                    if verdict["status"] == "已核验":
                        verified.append(item)
                    else:
                        pending.append(item)
            # 上游并发发布新版本时，整轮只采用最新一组可追溯的已核验版本
            selected = verified[-1] if verified else None
            resolved["sides"][side["kind"]] = {
                "label": side["label"],
                "source_name": source_name,
                "selected": selected,
                "verified_versions": [item["version"] for item in verified],
                "pending": pending,
            }
            for item in pending:
                resolved["pending_review"].append({
                    "侧": side["label"],
                    "来源名称": source_name,
                    "版本": item["version"],
                    "原因": item["reason"],
                })
        return resolved

    @staticmethod
    def _coverage(rows: list[dict[str, Any]], merge_key: str) -> list[Any]:
        """一组输入行覆盖的合并键（分片覆盖范围）。"""
        keys = {row.get(merge_key) for row in rows if row.get(merge_key) is not None}
        return sorted(keys, key=str)

    def _write_manifest(self, entry: dict[str, Any], resolved: dict[str, Any]) -> dict[str, Any]:
        """把来源核验结果回写到合并输入清单（审计只读视图的数据来源）。"""
        merge_key = str(entry.get("合并键列", "id"))
        manifest = entry.get("合并输入清单") or {}
        sides_view: dict[str, Any] = {}
        for kind, info in resolved["sides"].items():
            selected = info["selected"]
            if not info["source_name"]:
                status, reason = "未声明来源", "计划未填写该侧来源"
            elif selected:
                status, reason = "已核验", selected["reason"]
            elif info["pending"]:
                status, reason = "留待核对", info["pending"][-1]["reason"]
            else:
                status, reason = "无候选产物", "声明来源在流水线中无可归属产物"
            sides_view[info["label"]] = {
                "来源名称": info["source_name"],
                "状态": status,
                "已核验版本": selected["version"] if selected else None,
                "来源说明": reason,
                "覆盖键": self._coverage(selected["rows"], merge_key) if selected else [],
                "行数": selected["row_count"] if selected else 0,
                "可核对版本": info["verified_versions"],
                "样例行": selected["rows"][:5] if selected else [],
            }
        manifest.update({
            "计划编号": entry.get("计划编号"),
            "批式来源": entry.get("批式来源"),
            "流式来源": entry.get("流式来源"),
            "输入": sides_view,
            "留待核对": resolved["pending_review"],
            "核验时间": time.time(),
        })
        manifest.setdefault("预览证明", None)
        manifest.setdefault("已发布分片版本", None)
        manifest.setdefault("已发布关联版本", None)
        manifest.setdefault("待补范围", [])
        manifest.setdefault("发布记录", [])
        entry["合并输入清单"] = manifest
        return manifest

    # ---- 合并输入查询面板 / 合并预览 ----
    def merge_inputs(self, entry_id: int) -> dict[str, Any]:
        """合并输入查询面板：审计只读查看已选输入与来源核验结果。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"计划 {entry_id} 不存在"}
        resolved = self._resolve_inputs(entry)
        manifest = self._write_manifest(entry, resolved)
        return {"ok": True, "entry_id": entry_id, "manifest": manifest}

    def preview_merge(self, entry_id: int) -> dict[str, Any]:
        """打开合并预览：为当前已核验输入留下一份来源证明（版本 + 分片覆盖）。

        证明一旦留下不随后续上游投递变化，重新打开预览才会刷新。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"计划 {entry_id} 不存在"}
        resolved = self._resolve_inputs(entry)
        manifest = self._write_manifest(entry, resolved)
        merge_key = str(entry.get("合并键列", "id"))
        proof: dict[str, Any] = {"打开时间": time.time()}
        raw: list[str] = []
        for kind, label in (("batch", "分片"), ("stream", "关联")):
            selected = resolved["sides"][kind]["selected"]
            version = selected["version"] if selected else None
            keys = self._coverage(selected["rows"], merge_key) if selected else []
            proof[f"{label}版本"] = version
            proof[f"{label}覆盖键"] = keys
            raw.append(f"{kind}:{version}:{keys}")
        proof["证明编号"] = hashlib.md5("|".join(raw).encode()).hexdigest()[:12]
        manifest["预览证明"] = proof
        entry["合并输入清单"] = manifest
        return {"ok": True, "entry_id": entry_id, "proof": proof, "manifest": manifest}

    def get_preview(self, entry_id: int) -> dict[str, Any]:
        """查看已打开的合并预览及其来源证明（不刷新，保留打开时的旧证明）。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"计划 {entry_id} 不存在"}
        manifest = entry.get("合并输入清单") or {}
        proof = manifest.get("预览证明")
        if not proof:
            return {"ok": False, "reason": "尚未打开合并预览"}
        return {"ok": True, "entry_id": entry_id, "proof": proof, "manifest": manifest}

    @staticmethod
    def _missing_keys(proof: dict[str, Any] | None, used_keys: set[Any]) -> list[Any]:
        """待补范围：预览证明覆盖、但本轮实际采用版本未覆盖的合并键。"""
        if not proof:
            return []
        proof_keys = set(proof.get("分片覆盖键") or []) | set(proof.get("关联覆盖键") or [])
        return sorted((key for key in proof_keys if key not in used_keys), key=str)

    # ---- 发布入口 ----
    def execute_merge(self, entry_id: int) -> dict[str, Any]:
        """执行流批合并（发布入口）：整轮采用一组归属已核验的输入版本。

        失败/暂缓场景：
        - 无已核验输入（全部缺失或留待核对）→ 暂缓，读取权限不扩大到归属未确认的来源
        - 冲突版本：同一key有多个版本
        - 去重策略引冲突
        无论继续执行还是暂缓，发布尝试都会留下实际使用版本与待补范围，
        并回写到合并输入清单（分片版本与关联版本分别记录）。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "reason": f"计划 {entry_id} 不存在"}
        if entry.get("status") != "运行中":
            return {"ok": False, "reason": f"计划未运行，当前状态: {entry.get('status')}"}

        merge_key = str(entry.get("合并键列", "id"))
        conflict_policy = str(entry.get("冲突解决", "stream_win"))
        dedup_policy = str(entry.get("去重策略", "unique"))

        # 来源核验：只取归属已核验的最新一组版本，核验结果回写合并输入清单
        resolved = self._resolve_inputs(entry)
        manifest = self._write_manifest(entry, resolved)
        batch_sel = resolved["sides"]["batch"]["selected"]
        stream_sel = resolved["sides"]["stream"]["selected"]
        proof = manifest.get("预览证明")

        if batch_sel is None and stream_sel is None:
            # 暂缓：没有归属已核验的输入，不能扩大读取权限
            missing = self._missing_keys(proof, set())
            manifest["待补范围"] = missing
            manifest["发布记录"].append({
                "结果": "暂缓",
                "原因": "无已核验的合并输入",
                "时间": time.time(),
                "结果批次": None,
                "分片版本": None,
                "关联版本": None,
                "待补范围": missing,
            })
            entry["合并输入清单"] = manifest
            return {
                "ok": False,
                "deferred": True,
                "reason": "无已核验的合并输入，发布暂缓（归属未确认的来源不读取）",
                "entry_id": entry_id,
                "pending_review": resolved["pending_review"],
                "待补范围": missing,
                "manifest": manifest,
            }

        # 整轮只消费这组已核验版本的行，不再读全局共享流水线的最早产物
        batch_rows = list(batch_sel["rows"]) if batch_sel else []
        stream_rows = list(stream_sel["rows"]) if stream_sel else []

        # 合并去重逻辑
        merged: dict[Any, dict[str, Any]] = {}
        conflicts = []

        for row in batch_rows:
            key = row.get(merge_key)
            if key is None:
                continue
            if key in merged:
                conflicts.append({"key": key, "existing": merged[key], "new": row, "type": "batch_batch_conflict"})
                if conflict_policy == "stream_win":
                    continue
            merged[key] = row

        for row in stream_rows:
            key = row.get(merge_key)
            if key is None:
                continue
            if key in merged:
                conflicts.append({"key": key, "existing": merged[key], "new": row, "type": "stream_batch_conflict"})
                if conflict_policy == "stream_win":
                    merged[key] = row  # 流式优先覆盖
            else:
                merged[key] = row

        result = list(merged.values())

        # 去重：根据去重策略
        if dedup_policy == "unique":
            seen = set()
            deduped = []
            for r in result:
                k = r.get(merge_key)
                if k not in seen:
                    seen.add(k)
                    deduped.append(r)
            result = deduped

        used_versions = {
            "分片版本": batch_sel["version"] if batch_sel else None,
            "关联版本": stream_sel["version"] if stream_sel else None,
        }
        used_keys = {
            row.get(merge_key) for row in (*batch_rows, *stream_rows)
            if row.get(merge_key) is not None
        }
        missing = self._missing_keys(proof, used_keys)

        self._merge_seq += 1
        batch_id = f"MERGE_{entry_id}_{int(time.time() * 1000)}_{self._merge_seq}"
        self.produce_artifact(result, batch_id, provenance={
            "producer_module": self.MODULE,
            "producer_entry_id": entry_id,
            "source_kind": "merge",
            "计划编号": entry.get("计划编号"),
            "采用分片版本": used_versions["分片版本"],
            "采用关联版本": used_versions["关联版本"],
        })

        # 回写实际使用版本与待补范围，分片与关联分别记录
        manifest["已发布分片版本"] = used_versions["分片版本"]
        manifest["已发布关联版本"] = used_versions["关联版本"]
        manifest["待补范围"] = missing
        manifest["发布记录"].append({
            "结果": "已发布",
            "时间": time.time(),
            "结果批次": batch_id,
            **used_versions,
            "待补范围": missing,
        })
        entry["合并输入清单"] = manifest

        return {
            "ok": True,
            "entry_id": entry_id,
            "batch_rows_consumed": len(batch_rows),
            "stream_rows_consumed": len(stream_rows),
            "merged_result": len(result),
            "conflicts": len(conflicts),
            "conflict_samples": conflicts[:3],
            "merged_sample": result[:10],
            "used_versions": used_versions,
            "待补范围": missing,
            "manifest": manifest,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        base["active_plans"] = sum(1 for r in store.rows(self.MODULE) if r.get("status") == "运行中")
        base["pending_review_inputs"] = sum(
            len((r.get("合并输入清单") or {}).get("留待核对") or [])
            for r in store.rows(self.MODULE)
        )
        return base
