"""死信复核数据包路由：导出/导入/质检串联/待办结案的对外接口。"""
from __future__ import annotations

import json
from typing import Any

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.persistence import json_store
from app.services.dlq_review import DlqReviewService
from app.services.dlq_handler import DlqHandlerService

router = APIRouter(prefix="/api/dlq-review", tags=["死信复核数据包"])


class ExportRequest(BaseModel):
    dlq_ids: list[int] | None = None
    operator: str = "值班同事"


class ImportRequest(BaseModel):
    package: dict[str, Any]
    operator: str = "值班同事"


class QualityCheckRequest(BaseModel):
    package_no: str


class CloseTodoRequest(BaseModel):
    resolution: str = "字段证据确认结案"
    operator: str = "值班同事"


@router.post("/export")
def export_package(request: ExportRequest) -> StreamingResponse:
    """导出死信复核数据包，以 JSON 附件形式供值班同事下载。"""
    svc = DlqReviewService()
    result = svc.export_review_package(request.dlq_ids, operator=request.operator)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result["message"])
    package = result["package"]
    body = json.dumps(package, ensure_ascii=False, indent=2, default=str)
    filename = f"{package['package_no']}.json"
    return StreamingResponse(
        iter([body.encode("utf-8")]),
        media_type="application/json; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/import")
def import_package(request: ImportRequest) -> dict[str, Any]:
    """导入复核包（并发幂等：一条死信只登记一份有效复核，不触发重新投递）。"""
    svc = DlqReviewService()
    return svc.import_review_package(request.package, operator=request.operator)


@router.get("/packages")
def list_packages() -> dict[str, Any]:
    svc = DlqReviewService()
    items = svc.list_packages()
    return {"items": items, "total": len(items)}


@router.post("/quality-check")
def quality_check(request: QualityCheckRequest) -> dict[str, Any]:
    """串联接入质量校验：批量校验复核包原始载荷并回写待办与台账。"""
    svc = DlqReviewService()
    result = svc.run_quality_check(request.package_no)
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result["message"])
    return result


@router.get("/todos")
def list_todos(status: str | None = None) -> dict[str, Any]:
    """质检待办列表；字段证据与待复验外部结论分列返回。"""
    svc = DlqReviewService()
    items = svc.list_quality_todos(status=status)
    return {"items": items, "total": len(items)}


@router.get("/todos/{todo_id}")
def get_todo(todo_id: int) -> dict[str, Any]:
    svc = DlqReviewService()
    todo = svc.get_quality_todo(todo_id)
    if todo is None:
        raise HTTPException(status_code=404, detail=f"质检待办 {todo_id} 不存在")
    return todo


@router.post("/todos/{todo_id}/close")
def close_todo(todo_id: int, request: CloseTodoRequest) -> dict[str, Any]:
    """凭当前有效包字段证据结案；仅有旧包待复验外部结论时拒绝。"""
    svc = DlqReviewService()
    result = svc.close_quality_todo(todo_id, resolution=request.resolution,
                                    operator=request.operator)
    if not result.get("ok"):
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.get("/cases/{dlq_id}")
def case_view(dlq_id: int) -> dict[str, Any]:
    """单条死信复核工作台：双处置依据、证据可用性、载荷版本与台账。"""
    svc = DlqReviewService()
    result = svc.get_case_view(dlq_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result["message"])
    return result


@router.get("/cases/{dlq_id}/payload-versions")
def payload_versions(dlq_id: int) -> dict[str, Any]:
    """回查某条死信在各复核包（含已取代旧包）上的原始载荷版本。"""
    svc = DlqReviewService()
    return {"dlq_id": dlq_id, "items": svc.list_payload_versions(dlq_id)}


@router.get("/cases/{dlq_id}/ledger")
def disposition_ledger(dlq_id: int) -> dict[str, Any]:
    """读取死信处置台账：重放凭证、复核导入、质量校验、结案全链路依据。"""
    dlq_svc = DlqHandlerService()
    if json_store.get_by_id("dlq_handler", dlq_id) is None:
        raise HTTPException(status_code=404, detail=f"死信 {dlq_id} 不存在")
    return {"dlq_id": dlq_id, "items": dlq_svc.get_disposition_ledger(dlq_id)}
