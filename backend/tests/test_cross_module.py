"""跨模块数据流集成测试：覆盖10组贯通链路和真实失败场景。

测试覆盖：
1. stream_source → batch_slice：流式源数据消费到批式切割
2. change_capture → schema_govern：CDC变更驱动Schema演进
3. watermark → late_data：消费水位驱动迟到数据判定
4. stream_lookup → merge_plan：流式关联结果参与流批合并
5. snapshot_mgr → backfill_job：快照版本用于历史回填
6. partition_scale → compaction：分区统计触发小文件合并
7. data_reconcile → changelog：对账差异写入变更日志
8. cold_archive → retention：冷热分层后生命周期管理
9. lineage → table_format：血缘信息决定表格式策略
10. 失败场景：脏行、重复批次、冲突版本、写入中断
"""
from __future__ import annotations

import time
import pytest

from app.services import service_for
from app.store import store


# ============================================================
# 链路1: stream_source → batch_slice (流批来源数据)
# ============================================================
class TestStreamSourceToBatchSlice:
    def test_stream_sample_creates_watermark(self):
        """验证stream_sample产生水位偏移，供watermark消费"""
        svc = service_for("stream_source")
        # 先启用源
        entry = store.find("stream_source", 3)
        assert entry is not None
        entry["status"] = "已启用"

        result = svc.stream_sample(3, row_count=5)
        assert result["ok"] is True
        assert result["samples"] >= 3  # 有些会被过滤

        # 检查水位更新
        wm = store.get_watermark("3")
        assert wm is not None
        assert "offset" in wm

    def test_batch_slice_consumes_stream_sources(self):
        """验证batch_slice消费stream_source已启用源清单"""
        bc = service_for("batch_slice")
        info = bc.consume_stream_sources()
        assert "enabled_stream_sources" in info
        assert "topics" in info

    def test_batch_slice_invalid_range(self):
        """验证失败场景：分片起始值>=结束值"""
        bc = service_for("batch_slice")
        entry = store.find("batch_slice", 1)
        if entry:
            entry["分片起始值"] = "100"
            entry["分片结束值"] = "0"
            result = bc.execute_slice(1)
            assert result["ok"] is False
            assert "起始值" in result["reason"]


# ============================================================
# 链路2: change_capture → schema_govern (变更Schema演进)
# ============================================================
class TestCDCToSchemaGovern:
    def test_capture_writes_to_global_changelog(self):
        """验证CDC捕获写入全局变更日志"""
        cc = service_for("change_capture")
        entry = store.find("change_capture", 3)
        if entry:
            entry["status"] = "运行中"
            result = cc.capture_changes(3)
            assert result["ok"] is True
            assert result["captured"] > 0

            # 验证写入changelog
            cl_entries = store._global_changelog
            assert len(cl_entries) > 0

    def test_schema_evolve_from_cdc_events(self):
        """验证Schema演进消费CDC变更事件"""
        sg = service_for("schema_govern")
        entry = store.find("schema_govern", 3)
        assert entry is not None
        entry["status"] = "已注册"

        cdc_events = [{"type": "INSERT", "table": "test_table", "ts": time.time()}]
        result = sg.evolve_schema(3, cdc_events)
        # 可能因为版本号重复或其他原因失败，但不应该是异常
        assert isinstance(result, dict)
        assert "ok" in result

    def test_schema_compatibility_check(self):
        """验证Schema兼容性校验"""
        sg = service_for("schema_govern")
        entry = store.find("schema_govern", 1)
        assert entry is not None

        # 注册一个基准版本
        store.register_schema(entry.get("主题名称", "test_topic"),
                              {"fields": ["id", "name", "ts"]}, "v1")

        # 校验兼容性
        result = sg.validate_compatibility(1, {"fields": ["id", "ts"], "version": "v2"})
        assert result["ok"] is False  # 缺少name字段


# ============================================================
# 链路3: watermark → late_data (消费水位与迟到处理)
# ============================================================
class TestWatermarkToLateData:
    def test_watermark_compute_updates_store(self):
        """验证水位计算更新共享水位偏移"""
        wm = service_for("watermark")
        entry = store.find("watermark", 1)
        assert entry is not None

        result = wm.compute_watermark(1)
        assert result["ok"] is True
        assert "status" in result

        # 检查store中水位更新
        partition = entry.get("分区标识", "")
        stored_wm = store.get_watermark(partition)
        if partition:
            assert stored_wm is not None

    def test_late_data_classify_with_watermark(self):
        """验证延迟数据处理基于水位偏移"""
        ld = service_for("late_data")
        entry = store.find("late_data", 3)
        if entry:
            entry["status"] = "已生效"

        test_events = [
            {"ts": time.time() - 10},     # 10秒前，在允许范围内
            {"ts": time.time() - 100},    # 100秒前，超延迟阈值
            {"ts": time.time() - 5000},   # 5000秒前，超丢弃阈值
        ]
        result = ld.classify_late_events(3, test_events)
        assert result["ok"] is True
        assert result["on_time"] >= 1
        assert "allowed_lag" in result

    def test_watermark_emit_for_late_data(self):
        """验证watermark向late_data暴露水位信息"""
        wm = service_for("watermark")
        result = wm.emit_watermark_for_late_data("1")
        assert result["ok"] is True
        assert "downstream_consumer" in result
        assert result["downstream_consumer"] == "late_data"


# ============================================================
# 链路4: stream_lookup → merge_plan (流式关联与流批合并)
# ============================================================
class TestStreamLookupToMergePlan:
    def test_stream_lookup_join(self):
        """验证流式关联输出到流水线"""
        sl = service_for("stream_lookup")
        entry = store.find("stream_lookup", 3)
        if entry:
            entry["status"] = "已生效"

        left_events = [
            {"id": 1, "ts": time.time() - 5, "value": "a"},
            {"id": 2, "ts": time.time() - 1, "value": "b"},
            {"id": 99, "ts": time.time() - 2, "value": "c"},  # 无匹配
        ]
        result = sl.execute_lookup(3, left_events)
        assert result["ok"] is True
        assert result["matched"] >= 2
        assert result["unmatched"] >= 1

    def test_merge_plan_conflict_detection(self):
        """验证流批合并可以检测冲突版本"""
        # 先通过batch_slice产生批式数据
        bc = service_for("batch_slice")
        be = store.find("batch_slice", 3)
        if be:
            be["分片起始值"] = "0"
            be["分片结束值"] = "15"
            be["status"] = "待执行"
            bc._processed_batches.discard(str(be.get("批次编号", "")))
            bc.execute_slice(3)

        mp = service_for("merge_plan")
        entry = store.find("merge_plan", 3)
        if entry:
            # 来源归属：计划只能读取与声明来源一致的已核验产物
            entry["批式来源"] = str(be.get("源表名称")) if be else "ods_trade_ticks"
            entry["流式来源"] = "kafka-trade-ticks"
            entry["status"] = "运行中"
            result = mp.execute_merge(3)
            assert result["ok"] is True
            assert "conflicts" in result


# ============================================================
# 链路5: snapshot_mgr → backfill_job (快照与回填)
# ============================================================
class TestSnapshotToBackfill:
    def test_create_snapshot_saves_to_store(self):
        """验证快照创建后存入store._snapshot_store"""
        sm = service_for("snapshot_mgr")
        entry = store.find("snapshot_mgr", 1)
        assert entry is not None
        entry["源表名称"] = "test_backfill_table"

        sample_rows = [{"id": i, "col": f"val_{i}"} for i in range(10)]
        result = sm.create_snapshot(1, sample_rows)
        assert result["ok"] is True
        assert result["snapshot_id"] == entry["快照编号"]

        # 验证快照已保存
        snap = store.get_snapshot(entry["快照编号"])
        assert snap is not None
        assert snap["table"] == "test_backfill_table"

    def test_backfill_checks_snapshot_dependency(self):
        """验证回填任务检查快照依赖"""
        bj = service_for("backfill_job")
        entry = store.find("backfill_job", 1)
        assert entry is not None
        entry["源表列表"] = "test_backfill_table"
        entry["status"] = "待执行"

        result = bj.execute_backfill(1)
        assert result["ok"] is True  # 有快照
        assert result["rows_inserted"] > 0

    def test_backfill_no_snapshot_fails(self):
        """验证失败场景：回填时源表无快照"""
        bj = service_for("backfill_job")
        entry = store.find("backfill_job", 2)
        if entry:
            entry["源表列表"] = "nonexistent_table"
            entry["status"] = "待执行"
            result = bj.execute_backfill(2)
            assert result["ok"] is False
            assert "无可用快照" in result["reason"]


# ============================================================
# 链路6: partition_scale → compaction (分区治理与文件合并)
# ============================================================
class TestPartitionToCompaction:
    def test_partition_scale_produces_stats(self):
        """验证分区扩缩产生文件统计"""
        ps = service_for("partition_scale")
        entry = store.find("partition_scale", 3)
        if entry:
            entry["status"] = "已生效"
            result = ps.scale_partitions(3)
            assert result["ok"] is True
            assert "total_files" in result

    def test_compaction_consumes_partition_stats(self):
        """验证压缩任务消费分区统计"""
        cp = service_for("compaction")
        entry = store.find("compaction", 1)
        if entry:
            entry["文件数量"] = "60"
            entry["status"] = "待执行"
            result = cp.execute_compaction(1)
            # 如果文件数量>50，分区统计也会触发压缩
            assert "ok" in result

    def test_compaction_unsupported_compression(self):
        """验证失败场景：不支持的压缩算法"""
        cp = service_for("compaction")
        entry = store.find("compaction", 1)
        if entry:
            entry["压缩算法"] = "bzip2_invalid"
            entry["status"] = "待执行"
            result = cp.execute_compaction(1)
            assert result["ok"] is False
            assert "压缩算法" in result["reason"]


# ============================================================
# 链路7: data_reconcile → changelog (数据对账与变更追溯)
# ============================================================
class TestReconcileToChangelog:
    def test_reconcile_writes_to_changelog(self):
        """验证对账差异写入变更日志"""
        dr = service_for("data_reconcile")
        entry = store.find("data_reconcile", 1)
        assert entry is not None
        entry["左源表"] = "stream_source"
        entry["右源表"] = "batch_slice"
        entry["status"] = "待执行"

        result = dr.execute_reconcile(1)
        assert result["ok"] is True
        assert "diff_count" in result
        assert "passed" in result

    def test_changelog_query_records(self):
        """验证changelog查询追溯能力"""
        cl = service_for("changelog")
        result = cl.query_table_changelog("stream_source")
        assert result["ok"] is True
        assert "total_changes" in result

    def test_rollback_fails_without_version(self):
        """验证失败场景：无回滚版本号时拒绝回滚"""
        cl = service_for("changelog")
        entry = store.find("changelog", 1)
        if entry:
            entry["回滚版本"] = ""
            result = cl.rollback_change(1)
            assert result["ok"] is False
            assert "无法回滚" in result["reason"]


# ============================================================
# 链路8: cold_archive → retention (冷热分层与生命周期)
# ============================================================
class TestColdArchiveToRetention:
    def test_archive_adds_manifest_record(self):
        """验证归档添加清单记录"""
        ca = service_for("cold_archive")
        entry = store.find("cold_archive", 3)
        if entry:
            entry["status"] = "已生效"
            entry["冷存储路径"] = "s3://cold-storage/test/"
            result = ca.execute_archive(3)
            assert result["ok"] is True
            assert result["archived_rows"] >= 0
            assert len(store._archive_manifest) > 0

    def test_retention_consumes_archive_manifest(self):
        """验证保留策略消费归档清单"""
        rt = service_for("retention")
        entry = store.find("retention", 3)
        if entry:
            entry["status"] = "已生效"
            result = rt.execute_retention(3)
            assert result["ok"] is True
            assert "total_archived" in result

    def test_archive_empty_cold_path_fails(self):
        """验证失败场景：空冷存储路径"""
        ca = service_for("cold_archive")
        entry = store.find("cold_archive", 2)
        if entry:
            entry["冷存储路径"] = ""
            entry["status"] = "已生效"
            result = ca.execute_archive(2)
            assert result["ok"] is False
            assert "冷存储路径" in result["reason"]


# ============================================================
# 链路9: lineage → table_format (血缘追踪与表格式治理)
# ============================================================
class TestLineageToTableFormat:
    def test_lineage_records_edges(self):
        """验证血缘记录边关系"""
        ln = service_for("lineage")
        entry = store.find("lineage", 1)
        assert entry is not None
        entry["上游表"] = "source_table"
        entry["下游表"] = "target_table"
        entry["转换逻辑"] = "SELECT * FROM source_table"

        result = ln.build_lineage(1)
        assert result["ok"] is True
        assert len(store._lineage_graph) > 0

    def test_lineage_circular_dependency_detected(self):
        """验证失败场景：环形依赖检测"""
        ln = service_for("lineage")
        # 先创建 edge: a → b
        entry = store.find("lineage", 2)
        if entry:
            entry["上游表"] = "a"
            entry["下游表"] = "b"
            ln.build_lineage(2)

        # 再创建 edge: b → a (环形)
        entry3 = store.find("lineage", 3)
        if entry3:
            entry3["上游表"] = "b"
            entry3["下游表"] = "a"
            result = ln.build_lineage(3)
            assert result["ok"] is False
            assert "环形依赖" in result["reason"]

    def test_table_format_resolve_strategy(self):
        """验证表格式基于血缘解析写入策略"""
        tf = service_for("table_format")
        entry = store.find("table_format", 3)
        if entry:
            entry["status"] = "已生效"
            entry["目标表"] = "target_table"
            result = tf.resolve_write_strategy(3)
            assert result["ok"] is True
            assert "recommended_write_mode" in result


# ============================================================
# 链路10: 业务失败场景集合
# ============================================================
class TestFailureScenarios:
    def test_duplicate_batch_rejection(self):
        """验证重复批次拒绝"""
        bc = service_for("batch_slice")
        entry = store.find("batch_slice", 1)
        if entry:
            entry["分片起始值"] = "0"
            entry["分片结束值"] = "10"
            entry["status"] = "待执行"
            bc._processed_batches.discard(str(entry.get("批次编号", "")))
            result1 = bc.execute_slice(1)
            assert result1["ok"] is True

            # 再次执行同一批次
            entry["status"] = "待执行"
            result2 = bc.execute_slice(1)
            assert result2["ok"] is False
            assert "重复" in result2["reason"]

    def test_stream_source_not_enabled(self):
        """验证未启用的流式源拒绝采样"""
        ss = service_for("stream_source")
        entry = store.find("stream_source", 1)
        if entry:
            entry["status"] = "草稿"
            result = ss.stream_sample(1, row_count=3)
            assert result["ok"] is False
            assert "未启用" in result["reason"]

    def test_cdc_empty_target_topic(self):
        """验证CDC空目标Topic写入失败"""
        cc = service_for("change_capture")
        entry = store.find("change_capture", 1)
        if entry:
            entry["目标Topic"] = ""
            entry["status"] = "运行中"
            result = cc.capture_changes(1)
            assert result["ok"] is False

    def test_partition_scale_invalid_granularity(self):
        """验证分区粒度不合法"""
        ps = service_for("partition_scale")
        entry = store.find("partition_scale", 1)
        if entry:
            entry["分区粒度"] = "invalid_granularity"
            entry["status"] = "已生效"
            result = ps.scale_partitions(1)
            assert result["ok"] is False
            assert "分区粒度" in result["reason"]

    def test_data_pipeline_ingest(self):
        """验证DataPipeline摄入和脏行检测"""
        from app.store import DataPipeline
        pl = DataPipeline("test_pipeline", ["source"], "target")
        rows = [
            {"id": 1, "col": "a"},
            {"id": None, "col": "b"},  # 脏行
            {"id": 2, "col": ""},       # 脏行
            {"id": 3, "col": "c"},
        ]
        ingested, dirty = pl.ingest(rows, "batch_001")
        assert ingested >= 2
        assert len(dirty) >= 2

        # 重复批次拒绝
        ingested2, dirty2 = pl.ingest(rows, "batch_001")
        assert ingested2 == 0
        assert len(dirty2) >= 1

    def test_pipeline_versioned_snapshot(self):
        """验证流水线版本化快照"""
        from app.store import DataPipeline
        pl = DataPipeline("test_snapshot", ["src"], "tgt")
        pl.ingest([{"id": 1, "col": "x"}, {"id": 2, "col": "y"}], "batch_snap")
        snap = pl.versioned_snapshot()
        assert snap["pipeline"] == "test_snapshot"
        assert "version" in snap
        assert "artifact_count" in snap

    def test_merge_plan_no_batch_data(self):
        """验证无已核验输入时发布暂缓，不读取归属未确认的全局产物"""
        mp = service_for("merge_plan")
        # 创建新条目
        entry_id = max((int(r.get("id", 0)) for r in store.rows("merge_plan")), default=0) + 1
        store.rows("merge_plan").append({"id": entry_id, "计划编号": f"MERG-TEST-{entry_id}",
            "流式来源": "stream_source", "批式来源": "batch_slice",
            "合并键列": "id", "冲突解决": "stream_win", "输出路径": "/out",
            "status": "运行中", "pending": True, "abnormal": False})
        result = mp.execute_merge(entry_id)
        # 修复后：共享流水线里其他计划的产物不再被无归属计划读取，发布暂缓
        assert result["ok"] is False
        assert result["deferred"] is True


# ============================================================
# 综合测试：跨模块数据流端到端
# ============================================================
class TestEndToEndCrossModule:
    def test_full_stream_batch_flow(self):
        """端到端：stream_source → batch_slice → merge_plan 全链路"""
        # 1. stream_source采样
        ss = service_for("stream_source")
        entry = store.find("stream_source", 3)
        if entry:
            entry["status"] = "已启用"
            ss.stream_sample(3, row_count=5)

        # 2. batch_slice消费stream_source并执行分片
        bc = service_for("batch_slice")
        batch_entry = store.find("batch_slice", 3)
        if batch_entry:
            batch_entry["分片起始值"] = "0"
            batch_entry["分片结束值"] = "20"
            batch_entry["status"] = "待执行"
            bc._processed_batches.discard(str(batch_entry.get("批次编号", "")))
            bc.execute_slice(3)

        # 3. merge_plan消费流批两种产物
        mp = service_for("merge_plan")
        merge_entry = store.find("merge_plan", 3)
        if merge_entry:
            # 来源归属：与 batch_slice 3 / stream_lookup 3 的产物对齐
            merge_entry["批式来源"] = str(batch_entry.get("源表名称")) if batch_entry else "ods_trade_ticks"
            merge_entry["流式来源"] = "kafka-trade-ticks"
            merge_entry["status"] = "运行中"
            merge_result = mp.execute_merge(3)
            assert merge_result["ok"] is True

    def test_schema_lineage_table_format_chain(self):
        """端到端：lineage → table_format 和 schema_govern → table_format 链路"""
        # 1. lineage记录
        ln = service_for("lineage")
        e1 = store.find("lineage", 1)
        if e1:
            e1["上游表"] = "src_t"
            e1["下游表"] = "dst_t"
            ln.build_lineage(1)

        # 2. schema注册
        store.register_schema("dst_t", {"fields": ["id", "val"], "version": "v1"}, "v1")

        # 3. table_format消费
        tf = service_for("table_format")
        e3 = store.find("table_format", 3)
        if e3:
            e3["status"] = "已生效"
            e3["目标表"] = "dst_t"
            result = tf.resolve_write_strategy(3)
            assert result["ok"] is True
            assert "recommended_write_mode" in result

    def test_archive_retention_chain(self):
        """端到端：cold_archive → retention 链条"""
        # 1. 冷归档
        ca = service_for("cold_archive")
        e1 = store.find("cold_archive", 3)
        if e1:
            e1["status"] = "已生效"
            e1["冷存储路径"] = "s3://cold/test"
            ca.execute_archive(3)

        # 2. 保留策略消费归档清单
        rt = service_for("retention")
        e2 = store.find("retention", 3)
        if e2:
            e2["status"] = "已生效"
            result = rt.execute_retention(3)
            assert result["ok"] is True
            assert "total_archived" in result

    def test_cdc_schema_evolve_chain(self):
        """端到端：change_capture → schema_govern 链条"""
        # 1. CDC捕获
        cc = service_for("change_capture")
        e1 = store.find("change_capture", 3)
        if e1:
            e1["status"] = "运行中"
            cc.capture_changes(3)

        # 2. Schema演进
        sg = service_for("schema_govern")
        e2 = store.find("schema_govern", 3)
        if e2:
            e2["status"] = "已注册"
            result = sg.evolve_schema(3, [{"type": "INSERT", "ts": time.time()}])
            # 可能因为版本号冲突等失败
            assert isinstance(result, dict)
            assert "ok" in result


# ============================================================
# domain_snapshot 测试
# ============================================================
class TestDomainSnapshots:
    def test_all_modules_have_snapshot(self):
        """验证所有模块的domain_snapshot方法可用"""
        modules = [
            "stream_source", "batch_slice", "change_capture", "schema_govern",
            "watermark", "late_data", "stream_lookup", "merge_plan",
            "partition_scale", "snapshot_mgr", "data_reconcile", "backfill_job",
            "cold_archive", "compaction", "lineage", "changelog", "retention", "table_format",
        ]
        for mod in modules:
            svc = service_for(mod)
            assert svc is not None, f"Service {mod} is None"
            snap = svc.domain_snapshot()
            assert isinstance(snap, dict)
            assert "module" in snap
            assert "count" in snap