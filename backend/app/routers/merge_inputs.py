"""合并输入与发布路由：合并输入查询面板、合并预览、发布入口与上游投递。

这些路径都带三段结构（/api/{module}/{id}/xxx），与通用资源路由不冲突。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import RowsPayload
from app.services import service_for

router = APIRouter(prefix="/api", tags=["合并输入"])


def _service(module: str):
    service = service_for(module)
    if service is None:
        raise HTTPException(status_code=404, detail=f"未知业务模块：{module}")
    return service


def _unwrap(result: dict[str, Any], not_found: bool = True) -> dict[str, Any]:
    if not result.get("ok") and not_found and "不存在" in str(result.get("reason", "")):
        raise HTTPException(status_code=404, detail=result.get("reason"))
    return result


@router.get("/merge_plan/{entry_id}/inputs")
def merge_inputs(entry_id: int) -> dict[str, Any]:
    """合并输入查询面板：审计只读查看已选输入、核验结论与发布回写。"""
    return _unwrap(_service("merge_plan").merge_inputs(entry_id))


@router.post("/merge_plan/{entry_id}/preview")
def open_merge_preview(entry_id: int) -> dict[str, Any]:
    """打开合并预览：为当前已核验输入留下来源证明。"""
    return _unwrap(_service("merge_plan").preview_merge(entry_id))


@router.get("/merge_plan/{entry_id}/preview")
def get_merge_preview(entry_id: int) -> dict[str, Any]:
    """查看已打开的合并预览；上游新投递不会改变已留下的旧来源证明。"""
    return _unwrap(_service("merge_plan").get_preview(entry_id))


@router.post("/merge_plan/{entry_id}/publish")
def publish_merge(entry_id: int) -> dict[str, Any]:
    """发布入口：整轮采用一组已核验输入；继续执行或暂缓都会留下实际版本与待补范围。"""
    return _service("merge_plan").execute_merge(entry_id)


@router.post("/batch_slice/{entry_id}/publish")
def publish_slice(entry_id: int, payload: RowsPayload) -> dict[str, Any]:
    """投递归属明确的新版分片。"""
    return _unwrap(_service("batch_slice").publish_slice(entry_id, payload.rows))


@router.post("/stream_lookup/{entry_id}/publish")
def publish_lookup(entry_id: int, payload: RowsPayload) -> dict[str, Any]:
    """投递归属明确的新版关联结果。"""
    return _unwrap(_service("stream_lookup").publish_lookup(entry_id, payload.rows))
