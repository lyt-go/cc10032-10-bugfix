"""流批合并来源归属验收测试。

场景对齐验收口径：
1. 甲乙计划都含键 7（分别为 A 和 B），先运行乙，再从甲发起合并，
   甲只能读到甲归属的输入（键 7 = A），不能读到乙先发布的全局产物。
2. 给甲准备旧版键 8，打开预览后投递归属明确但只含键 7 的新版分片，
   再执行发布：实际采用新版分片版本，待补范围为键 8，并回写合并输入清单。
3. 历史产物按源名称兼容核验；重名或缺失且无法证明归属的记录留待核对，
   该计划不得读取归属未确认的来源（发布暂缓）。
"""
from __future__ import annotations

from app.services import service_for
from app.store import store


def _add_merge_plan(plan_no: str, stream_src: str, batch_src: str, status: str = "运行中") -> int:
    rows = store.rows("merge_plan")
    entry_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    rows.append({
        "id": entry_id, "计划编号": plan_no, "流式来源": stream_src, "批式来源": batch_src,
        "合并键列": "id", "冲突解决": "stream_win", "输出路径": "/out",
        "合并窗口": "30", "去重策略": "unique",
        "status": status, "pending": True, "abnormal": False,
    })
    return entry_id


def _add_batch_slice(batch_no: str, table: str) -> int:
    rows = store.rows("batch_slice")
    entry_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
    rows.append({
        "id": entry_id, "批次编号": batch_no, "源表名称": table, "分片键列": "id",
        "分片起始值": "0", "分片结束值": "10", "预计行数": "10", "并行线程": "1", "完成时间": "",
        "status": "待执行", "pending": True, "abnormal": False,
    })
    return entry_id


def _latest_merge_rows() -> list[dict]:
    pipeline = store.pipeline("stream_batch_merge")
    publications = pipeline.publications() if pipeline else []
    return publications[-1]["rows"] if publications else []


class TestMergeProvenance:
    def test_merge_reads_only_own_attributed_inputs(self):
        """甲乙都含键 7：先运行乙，再从甲发起合并，甲只能读到自己的 A。"""
        bs = service_for("batch_slice")
        mp = service_for("merge_plan")

        b_slice = _add_batch_slice("BATC-ACC-B", "ods_acc_b")
        plan_b = _add_merge_plan("MERG-ACC-B", "stream_acc_b", "ods_acc_b")
        a_slice = _add_batch_slice("BATC-ACC-A", "ods_acc_a")
        plan_a = _add_merge_plan("MERG-ACC-A", "stream_acc_a", "ods_acc_a")

        # 先运行乙：投递乙的分片（键 7 = B）并执行乙的合并
        delivered_b = bs.publish_slice(b_slice, [{"id": 7, "payload": "B"}])
        assert delivered_b["ok"] is True
        result_b = mp.execute_merge(plan_b)
        assert result_b["ok"] is True
        assert {r["id"]: r for r in _latest_merge_rows()}[7]["payload"] == "B"

        # 甲的旧版分片：键 7 = A，另有键 8
        delivered_a = bs.publish_slice(a_slice, [
            {"id": 7, "payload": "A"},
            {"id": 8, "payload": "A_old"},
        ])
        assert delivered_a["ok"] is True

        # 从甲发起合并：全局流水线里乙的产物在前，但甲只能读到归属甲的输入
        result_a = mp.execute_merge(plan_a)
        assert result_a["ok"] is True
        merged = {r["id"]: r for r in _latest_merge_rows()}
        assert merged[7]["payload"] == "A"
        assert merged[8]["payload"] == "A_old"
        assert result_a["used_versions"]["分片版本"] == delivered_a["version"]

    def test_preview_proof_and_publish_records_actual_versions(self):
        """旧版键 8 + 预览后投递只含键 7 的新版：发布采用新版，待补范围为键 8。"""
        bs = service_for("batch_slice")
        mp = service_for("merge_plan")

        a_slice = _add_batch_slice("BATC-ACC-A2", "ods_acc_a2")
        plan_a = _add_merge_plan("MERG-ACC-A2", "stream_acc_a2", "ods_acc_a2")

        # 旧版分片：键 7、8
        old = bs.publish_slice(a_slice, [
            {"id": 7, "payload": "A"},
            {"id": 8, "payload": "A_old"},
        ])
        old_version = old["version"]

        # 打开预览：来源证明覆盖键 7、8
        preview = mp.preview_merge(plan_a)
        assert preview["ok"] is True
        proof = preview["proof"]
        assert proof["分片版本"] == old_version
        assert set(proof["分片覆盖键"]) == {7, 8}

        # 投递归属明确但只含键 7 的新版分片；已打开的预览仍保留旧来源证明
        new = bs.publish_slice(a_slice, [{"id": 7, "payload": "A_v2"}])
        new_version = new["version"]
        assert new_version != old_version
        kept = mp.get_preview(plan_a)
        assert kept["ok"] is True
        assert kept["proof"]["证明编号"] == proof["证明编号"]
        assert kept["proof"]["分片版本"] == old_version

        # 执行发布：整轮采用最新已核验版本，回写实际版本与待补范围
        published = mp.execute_merge(plan_a)
        assert published["ok"] is True
        assert published["used_versions"]["分片版本"] == new_version
        assert published["待补范围"] == [8]
        merged = {r["id"]: r for r in _latest_merge_rows()}
        assert merged[7]["payload"] == "A_v2"
        assert 8 not in merged

        # 合并输入清单：分片与关联版本分别记录，发布尝试留痕
        manifest = published["manifest"]
        assert manifest["已发布分片版本"] == new_version
        assert manifest["已发布关联版本"] is None
        assert manifest["待补范围"] == [8]
        assert manifest["发布记录"][-1]["结果"] == "已发布"
        assert manifest["发布记录"][-1]["分片版本"] == new_version

        # 审计只读查看：已选输入可核对
        inputs = mp.merge_inputs(plan_a)
        assert inputs["ok"] is True
        assert inputs["manifest"]["输入"]["分片"]["已核验版本"] == new_version
        assert inputs["manifest"]["输入"]["分片"]["状态"] == "已核验"

    def test_historical_artifacts_pending_review_and_deferral(self):
        """历史产物按源名称兼容核验；重名/缺失留待核对且发布暂缓。"""
        bs = service_for("batch_slice")
        mp = service_for("merge_plan")
        pipeline = store.pipeline("batch_slicing")
        assert pipeline is not None

        # 历史产物：无来源证明，只有行内源名称
        pipeline.publish([{"id": 7, "source_table": "ods_legacy_dup", "payload": "x"}], "LEGACY_DUP")
        pipeline.publish([{"id": 9, "source_table": "ods_legacy_none", "payload": "y"}], "LEGACY_NONE")
        pipeline.publish([{"id": 5, "source_table": "ods_legacy_ok", "payload": "z"}], "LEGACY_OK")

        # 重名：两个分片条目登记同一源表名称，无法证明归属
        _add_batch_slice("BATC-LEG-DUP-1", "ods_legacy_dup")
        _add_batch_slice("BATC-LEG-DUP-2", "ods_legacy_dup")
        plan_dup = _add_merge_plan("MERG-LEG-DUP", "stream_legacy_none", "ods_legacy_dup")
        inputs = mp.merge_inputs(plan_dup)
        assert inputs["ok"] is True
        assert inputs["manifest"]["输入"]["分片"]["状态"] == "留待核对"
        assert any("重名" in item["原因"] for item in inputs["manifest"]["留待核对"])
        # 归属未确认 → 暂缓，不读取
        result = mp.execute_merge(plan_dup)
        assert result["ok"] is False
        assert result["deferred"] is True
        assert result["manifest"]["发布记录"][-1]["结果"] == "暂缓"

        # 缺失：没有任何登记条目对应该源名称
        plan_none = _add_merge_plan("MERG-LEG-NONE", "stream_legacy_none", "ods_legacy_none")
        inputs_none = mp.merge_inputs(plan_none)
        assert inputs_none["manifest"]["输入"]["分片"]["状态"] == "留待核对"
        assert any("缺失" in item["原因"] for item in inputs_none["manifest"]["留待核对"])
        result_none = mp.execute_merge(plan_none)
        assert result_none["ok"] is False
        assert result_none["deferred"] is True

        # 唯一归属的历史产物：兼容核验通过，可以读取
        _add_batch_slice("BATC-LEG-OK", "ods_legacy_ok")
        plan_ok = _add_merge_plan("MERG-LEG-OK", "stream_legacy_none", "ods_legacy_ok")
        inputs_ok = mp.merge_inputs(plan_ok)
        assert inputs_ok["manifest"]["输入"]["分片"]["状态"] == "已核验"
        result_ok = mp.execute_merge(plan_ok)
        assert result_ok["ok"] is True
        assert {r["id"]: r for r in _latest_merge_rows()}[5]["payload"] == "z"

    def test_stream_side_attribution(self):
        """流式关联侧同样按归属核验：甲读不到乙投递的关联版本。"""
        sl = service_for("stream_lookup")
        mp = service_for("merge_plan")

        def add_lookup(rule_no: str, left: str) -> int:
            rows = store.rows("stream_lookup")
            entry_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            rows.append({
                "id": entry_id, "规则编号": rule_no, "左流来源": left, "右表来源": "dim_x",
                "关联键列": "id", "关联类型": "inner", "窗口时长": "60000",
                "缓存策略": "LRU", "未匹配处理": "drop",
                "status": "已生效", "pending": False, "abnormal": False,
            })
            return entry_id

        lookup_b = add_lookup("STRE-ACC-B", "stream_acc_sb")
        lookup_a = add_lookup("STRE-ACC-A", "stream_acc_sa")
        plan_a = _add_merge_plan("MERG-ACC-SA", "stream_acc_sa", "ods_acc_sa_none")

        # 乙先投递关联结果（键 7 = B），甲再投递（键 7 = A）
        sl.publish_lookup(lookup_b, [{"id": 7, "payload": "B"}])
        sl.publish_lookup(lookup_a, [{"id": 7, "payload": "A"}])

        result = mp.execute_merge(plan_a)
        assert result["ok"] is True
        assert {r["id"]: r for r in _latest_merge_rows()}[7]["payload"] == "A"
        assert result["used_versions"]["关联版本"] is not None
        assert result["manifest"]["已发布关联版本"] == result["used_versions"]["关联版本"]
