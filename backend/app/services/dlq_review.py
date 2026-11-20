"""死信复核数据包领域服务。

补齐死信队列的审核交付物能力，覆盖值班复核的完整闭环：

1. 导出复核包：从待审死信的原始消息数据生成失败复核记录，随包携带
   完整原始载荷与处置历史（含已重放记录当时的消费凭证）。
2. 并发导入幂等：同一条死信在任意并发下只登记一份有效复核；后导入
   的新包取代旧包成为有效复核，旧包及其载荷版本仍可回查。
3. 质量校验串联：对复核包内原始载荷批量跑接入质量校验，结论回写
   质检待办（data_quality_todo）与死信处置台账（dlq_disposition_ledger）。
4. 双处置依据：重放消费确认与源字段质量复核是两份并列依据；旧包的
   阻断结论标记为「待复验外部结论」，只有当前有效包的字段证据可结案。

存储分区（均走 json_store，随服务重启持久化）：
- dlq_review_package      复核包元数据与状态
- dlq_review_record       每条死信一份的有效复核记录（幂等锚点）
- dlq_payload_snapshot    按 (dlq_id, package_id) 留存的载荷版本
- data_quality_todo       质检待办（字段证据/外部结论分列）
- 台账复用 DlqHandlerService.LEDGER_MODULE
"""
from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService
from app.services.dlq_handler import DlqHandlerService

PACKAGE_MODULE = "dlq_review_package"
RECORD_MODULE = "dlq_review_record"
SNAPSHOT_MODULE = "dlq_payload_snapshot"
TODO_MODULE = "data_quality_todo"

# 复核/待办生命周期
PACKAGE_STATES = ("已导入", "已取代")
RECORD_STATES = ("有效", "已取代")
TODO_STATES = ("待处理", "待复验", "已结案")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_payload(raw: Any) -> dict[str, Any]:
    """把死信原始载荷解析成可做字段校验的 dict；非 JSON 时退化为包裹结构。"""
    if isinstance(raw, dict):
        return dict(raw)
    try:
        parsed = json.loads(str(raw))
    except (json.JSONDecodeError, TypeError):
        return {"_raw_message": "" if raw is None else str(raw)}
    if isinstance(parsed, dict):
        return parsed
    return {"_raw_message": parsed}


class DlqReviewService(BaseModuleService):
    MODULE = "dlq_review"
    ENTITY = "死信复核包"
    OPERATION_TERM = "复核"
    LIST_FIELDS = ["复核包编号", "来源死信数", "包状态", "导出时间", "导入时间", "载荷版本"]
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER = ["已导入", "已取代"]

    def __init__(self) -> None:
        super().__init__()
        # 导入临界区：保证并发导入下「同一条死信只有一份有效复核」
        self._import_lock = threading.Lock()

    # ──────────────────────────────────────────────────────────
    # 1. 导出复核包（审核交付物：原始载荷 + 处置历史 + 消费凭证）
    # ──────────────────────────────────────────────────────────
    def export_review_package(self, dlq_ids: list[int] | None = None,
                              operator: str = "值班同事") -> dict[str, Any]:
        """从待审死信原始消息数据导出失败复核记录，组装成可下载的复核数据包。"""
        dlq_svc = DlqHandlerService()
        all_rows = json_store.get_all(dlq_svc.MODULE)

        if dlq_ids:
            id_set = {int(i) for i in dlq_ids}
            rows = [r for r in all_rows if int(r.get("id", 0)) in id_set]
        else:
            # 默认导出全部待审（草稿/待处置）死信；已重放记录也可按需显式导出
            rows = [r for r in all_rows if r.get("status") in ("草稿", "待处置")]

        if not rows:
            return {"ok": False, "message": "没有可导出的待审死信记录", "items": []}

        exported_at = _now()
        package_no = f"DRP-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        payload_version = f"pv-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"

        items: list[dict[str, Any]] = []
        for row in rows:
            dlq_id = int(row["id"])
            full_payload = dlq_svc.get_full_payload(dlq_id) or ""
            ack_receipt = row.get("_ack_receipt")
            items.append({
                "dlq_id": dlq_id,
                "记录编号": row.get("记录编号", ""),
                "原始队列": row.get("原始队列", ""),
                "失败原因": row.get("失败原因", ""),
                "入队时间": row.get("入队时间", ""),
                "处置状态": row.get("处置状态", row.get("status", "")),
                "消息指纹": row.get("_fingerprint", ""),
                # 审核交付物核心：完整原始载荷
                "原始载荷": full_payload,
                "载荷版本": payload_version,
                # 处置历史时间线
                "处置历史": list(row.get("_disposition_history", [])),
                # 已重放记录保留当时的消费凭证
                "消费凭证": ack_receipt,
            })

        package = {
            "package_no": package_no,
            "payload_version": payload_version,
            "exported_at": exported_at,
            "exported_by": operator,
            "item_count": len(items),
            "items": items,
            "manifest_version": "1.0",
        }
        return {
            "ok": True,
            "message": f"复核包 {package_no} 已生成，含 {len(items)} 条失败复核记录",
            "package": package,
        }

    # ──────────────────────────────────────────────────────────
    # 2. 导入复核包（并发幂等：一条死信只留一份有效复核）
    # ──────────────────────────────────────────────────────────
    def import_review_package(self, package: dict[str, Any],
                              operator: str = "值班同事") -> dict[str, Any]:
        """导入值班同事下载/回流的复核数据包。

        幂等规则：
        - 同一 package_no 重复导入直接返回既有结果，不产生重复复核；
        - 每条死信任意时刻只有一份「有效」复核，新包导入使旧复核转「已取代」；
        - 旧包载荷快照永久留存，可按版本回查；
        - 导入只做登记，绝不触发重放/重新投递（已确认的消费回执保持有效）。
        """
        if not isinstance(package, dict) or not package.get("package_no"):
            return {"ok": False, "message": "复核包格式无效：缺少 package_no", "registered": 0}

        package_no = str(package["package_no"])
        payload_version = str(package.get("payload_version", package_no))
        items = package.get("items") or []

        with self._import_lock:
            # 幂等：同包重复导入
            existing_pkg = self._find_package(package_no)
            if existing_pkg is not None:
                existing_records = [r for r in json_store.get_all(RECORD_MODULE)
                                    if r.get("package_no") == package_no]
                return {
                    "ok": True,
                    "message": f"复核包 {package_no} 已导入过，本次为幂等返回，未重复登记",
                    "package_id": existing_pkg["id"],
                    "package_no": package_no,
                    "registered": 0,
                    "duplicate": True,
                    "effective_records": [
                        {"dlq_id": r["dlq_id"], "record_id": r["id"], "state": r["复核状态"]}
                        for r in existing_records
                    ],
                }

            imported_at = _now()
            pkg_row = {
                "复核包编号": package_no,
                "来源死信数": len(items),
                "包状态": "已导入",
                "导出时间": package.get("exported_at", ""),
                "导入时间": imported_at,
                "载荷版本": payload_version,
                "导入人": operator,
                "package_no": package_no,
                "payload_version": payload_version,
                "status": "已导入",
                "pending": True,
                "abnormal": False,
            }
            pkg_inserted = json_store.insert(PACKAGE_MODULE, pkg_row)

            registered: list[dict[str, Any]] = []
            superseded: list[dict[str, Any]] = []
            skipped: list[dict[str, Any]] = []
            dlq_svc = DlqHandlerService()

            for item in items:
                dlq_id = int(item.get("dlq_id", 0) or 0)
                dlq_entry = json_store.get_by_id(dlq_svc.MODULE, dlq_id)
                if dlq_id <= 0 or dlq_entry is None:
                    skipped.append({"dlq_id": dlq_id, "reason": "死信记录不存在或已归档"})
                    continue

                # 载荷版本快照：旧包原载荷版本可回查
                snapshot = {
                    "dlq_id": dlq_id,
                    "package_no": package_no,
                    "payload_version": str(item.get("载荷版本", payload_version)),
                    "原始载荷": item.get("原始载荷", ""),
                    "消息指纹": item.get("消息指纹", ""),
                    "captured_at": imported_at,
                }
                json_store.insert(SNAPSHOT_MODULE, snapshot)

                # 使该死信既有的有效复核转为「已取代」（旧包保留可回查，不删除）
                for old in json_store.get_all(RECORD_MODULE):
                    if int(old.get("dlq_id", 0)) == dlq_id and old.get("复核状态") == "有效":
                        json_store.update(RECORD_MODULE, int(old["id"]), {"复核状态": "已取代"})
                        superseded.append({"dlq_id": dlq_id, "record_id": old["id"],
                                           "old_package": old.get("package_no")})
                        # 旧复核包整体转为「已取代」，但包与载荷快照都保留可回查
                        old_pkg = self._find_package(str(old.get("package_no", "")))
                        if old_pkg is not None and old_pkg.get("包状态") != "已取代":
                            json_store.update(PACKAGE_MODULE, int(old_pkg["id"]),
                                              {"包状态": "已取代", "status": "已取代",
                                               "pending": False})

                # 登记当前唯一有效复核（同包再导入在前面已短路，不会插第二份）
                record_row = {
                    "复核编号": f"DRR-{dlq_id:05d}-{uuid.uuid4().hex[:6]}",
                    "dlq_id": dlq_id,
                    "记录编号": item.get("记录编号", dlq_entry.get("记录编号", "")),
                    "package_no": package_no,
                    "package_id": pkg_inserted["id"],
                    "payload_version": str(item.get("载荷版本", payload_version)),
                    "复核状态": "有效",
                    "原始载荷": item.get("原始载荷", ""),
                    "导入时间": imported_at,
                    "质检状态": "未校验",
                    "status": "有效",
                    "pending": True,
                    "abnormal": False,
                }
                rec = json_store.insert(RECORD_MODULE, record_row)
                registered.append({"dlq_id": dlq_id, "record_id": rec["id"], "state": "有效"})

                # 回找死信挂接复核引用；不改变死信处置状态，绝不重新投递
                review_refs = list(dlq_entry.get("_review_refs", []))
                review_refs.append({"package_no": package_no, "at": imported_at})
                json_store.update(dlq_svc.MODULE, dlq_id, {"_review_refs": review_refs})

                dlq_svc.append_disposition(
                    dlq_id, basis_type="复核导入",
                    conclusion=f"复核包 {package_no} 导入，登记有效复核（载荷版本 {payload_version}）",
                    detail={"package_no": package_no, "payload_version": payload_version,
                            "record_id": rec["id"]},
                )

            # 若整包没有任何有效承载记录，包本身也标记取代之外的孤立态（这里保留已导入并提示）
            return {
                "ok": True,
                "message": (f"复核包 {package_no} 导入完成：登记有效复核 {len(registered)} 条，"
                            f"取代旧复核 {len(superseded)} 条，跳过 {len(skipped)} 条；未触发任何重新投递"),
                "package_id": pkg_inserted["id"],
                "package_no": package_no,
                "registered": len(registered),
                "superseded": superseded,
                "skipped": skipped,
                "duplicate": False,
                "effective_records": registered,
            }

    def get_effective_review(self, dlq_id: int) -> dict[str, Any] | None:
        """获取一条死信当前唯一有效的复核记录。"""
        for row in json_store.get_all(RECORD_MODULE):
            if int(row.get("dlq_id", 0)) == dlq_id and row.get("复核状态") == "有效":
                return row
        return None

    def list_payload_versions(self, dlq_id: int) -> list[dict[str, Any]]:
        """回查某条死信在各复核包上的原始载荷版本（含已取代旧包）。"""
        rows = [r for r in json_store.get_all(SNAPSHOT_MODULE)
                if int(r.get("dlq_id", 0)) == dlq_id]
        return sorted(rows, key=lambda r: r.get("created_at", ""))

    def list_packages(self) -> list[dict[str, Any]]:
        return sorted(json_store.get_all(PACKAGE_MODULE),
                      key=lambda r: r.get("created_at", ""), reverse=True)

    def _find_package(self, package_no: str) -> dict[str, Any] | None:
        for row in json_store.get_all(PACKAGE_MODULE):
            if row.get("package_no") == package_no:
                return row
        return None

    # ──────────────────────────────────────────────────────────
    # 3. 接入质量校验串联：批量校验原始载荷，结论回写待办与台账
    # ──────────────────────────────────────────────────────────
    def run_quality_check(self, package_no: str) -> dict[str, Any]:
        """对复核包内全部原始载荷执行质量校验并回写结论。

        - 校验基于当前已生效的接入质量规则（DataQualityRuleService）。
        - 每条死信形成一份字段级结论，写入/更新质检待办与处置台账。
        - 阻断策略命中的规则形成「外部结论」；仅当前有效包的字段证据可用于结案。
        """
        from app.services.data_quality_rule import DataQualityRuleService

        package = self._find_package(package_no)
        if package is None:
            return {"ok": False, "message": f"复核包 {package_no} 不存在"}

        records = [r for r in json_store.get_all(RECORD_MODULE)
                   if r.get("package_no") == package_no]
        if not records:
            return {"ok": False, "message": f"复核包 {package_no} 没有承载任何复核记录"}

        quality_svc = DataQualityRuleService()
        dlq_svc = DlqHandlerService()
        checked_at = _now()
        total = passed = failed = blocked = 0
        todo_results: list[dict[str, Any]] = []

        for record in records:
            dlq_id = int(record["dlq_id"])
            payload = _parse_payload(record.get("原始载荷", ""))
            result = quality_svc.validate_record(payload, source_module="dlq_review")
            total += 1
            is_blocked = bool(result.get("blocked"))
            if is_blocked:
                blocked += 1
                failed += 1
            elif result.get("passed"):
                passed += 1
            else:
                failed += 1

            field_evidence, external_conclusions = self._split_evidence(result, record)
            todo = self._upsert_quality_todo(
                dlq_id=dlq_id, record=record, result=result,
                field_evidence=field_evidence, external_conclusions=external_conclusions,
                is_blocked=is_blocked, checked_at=checked_at,
            )

            # 回写复核记录的质检状态（按本条载荷自身结论，而非批次累计计数）
            json_store.update(RECORD_MODULE, int(record["id"]), {
                "质检状态": "阻断" if is_blocked else ("通过" if result.get("passed") else "不通过"),
                "质检时间": checked_at,
            })

            # 回写死信处置台账（质量复核是与重放凭证并列的处置依据）
            dlq_svc.append_disposition(
                dlq_id, basis_type="质量校验",
                conclusion=("阻断" if is_blocked else "通过" if result.get("passed") else "不通过"),
                detail={
                    "package_no": package_no,
                    "payload_version": record.get("payload_version"),
                    "todo_id": todo["id"],
                    "score": result.get("score"),
                    "failed_rules": [d.get("rule_no") for d in result.get("details", [])
                                     if not d.get("passed")],
                    "checked_at": checked_at,
                },
            )
            todo_results.append({"dlq_id": dlq_id, "todo_id": todo["id"],
                                 "todo_status": todo["待办状态"], "blocked": is_blocked})

        return {
            "ok": True,
            "message": f"复核包 {package_no} 质量校验完成：通过 {passed}，不通过 {failed}（阻断 {blocked}）",
            "package_no": package_no,
            "total": total,
            "passed": passed,
            "failed": failed,
            "blocked": blocked,
            "todos": todo_results,
        }

    def _split_evidence(self, result: dict[str, Any],
                        record: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """把校验明细拆成「字段证据」与「外部结论」。

        - 字段证据：规则对源字段的逐条判定，是质检待办可据以结案的依据；
        - 外部结论：阻断策略产生的处置性结论，若来自已被取代的旧包则只能待复验。
        """
        field_evidence: list[dict[str, Any]] = []
        external_conclusions: list[dict[str, Any]] = []
        is_effective = record.get("复核状态") == "有效"
        for detail in result.get("details", []):
            evidence = {
                "rule_no": detail.get("rule_no"),
                "field": detail.get("field"),
                "expression": detail.get("expression"),
                "rule_version": detail.get("rule_version", "v1"),
                "passed": detail.get("passed"),
                "message": self._detail_message(detail),
                "source_package": record.get("package_no"),
                "payload_version": record.get("payload_version"),
            }
            field_evidence.append(evidence)
            if not detail.get("passed") and detail.get("block_policy") == "阻断":
                # 阻断是处置性的外部结论：无论来自当前有效包还是已取代旧包，
                # 都必须人工复验闭环后才能结案，不能直接用于结案。
                external_conclusions.append({
                    **evidence,
                    "conclusion_state": "待复验",
                    "from_superseded_package": not is_effective,
                    "usable_for_close": False,
                })
        return field_evidence, external_conclusions

    @staticmethod
    def _detail_message(detail: dict[str, Any]) -> str:
        for part in (detail.get("expr_result"), detail.get("range_result")):
            if isinstance(part, dict) and not part.get("passed", True):
                return str(part.get("message", ""))
        return "通过"

    def _upsert_quality_todo(self, *, dlq_id: int, record: dict[str, Any],
                             result: dict[str, Any],
                             field_evidence: list[dict[str, Any]],
                             external_conclusions: list[dict[str, Any]],
                             is_blocked: bool, checked_at: str) -> dict[str, Any]:
        """创建或更新一条死信对应的质检待办。

        证据锚定规则：
        - 字段证据与待办主身份只认当前「有效」复核包；对已取代旧包质检时
          不得冲掉有效包已建立的字段证据；
        - 外部阻断结论按包累积、同规则同包去重，旧包结论一律保留为待复验。
        """
        existing = [t for t in json_store.get_all(TODO_MODULE)
                    if int(t.get("dlq_id", 0)) == dlq_id and t.get("待办状态") != "已结案"]
        is_effective = record.get("复核状态") == "有效"
        effective_review = self.get_effective_review(dlq_id)

        # 合并历史外部结论（旧包阻断轨迹保留，维持待复验、不可结案）
        merged_external = self._merge_external(
            list(existing[0].get("外部结论", [])) if existing else [],
            external_conclusions,
        )
        has_pending_external = any(not c.get("usable_for_close", True)
                                   for c in merged_external)

        # 字段证据：本次为有效包则刷新；旧包质检时沿用既有有效证据，绝不置空覆盖
        if is_effective:
            resolved_evidence = field_evidence
            source_package = record.get("package_no")
            payload_version = record.get("payload_version")
        elif existing:
            resolved_evidence = list(existing[0].get("字段证据", []))
            source_package = existing[0].get("来源复核包")
            payload_version = existing[0].get("载荷版本")
        else:
            resolved_evidence = []
            source_package = record.get("package_no")
            payload_version = record.get("payload_version")

        # 待办状态：存在阻断（含旧包待复验外部结论）即待复验；否则待处理
        todo_blocked = is_blocked or has_pending_external
        can_close = bool(resolved_evidence) and effective_review is not None and not todo_blocked

        todo_payload = {
            "待办编号": f"QTD-{dlq_id:05d}",
            "dlq_id": dlq_id,
            "记录编号": record.get("记录编号", ""),
            "来源复核包": source_package,
            "载荷版本": payload_version,
            "待办状态": "待复验" if todo_blocked else "待处理",
            "质检结论": "阻断" if is_blocked else ("通过" if result.get("passed") else "不通过"),
            "质检分数": result.get("score"),
            # 可用于结案的字段证据（仅当前有效包提供，旧包质检不会清空）
            "字段证据": resolved_evidence,
            # 还待复验的外部结论（旧包阻断结论归此列，不能用于结案）
            "外部结论": merged_external,
            "证据可用性": "可结案" if can_close else (
                "仅外部结论待复验" if not resolved_evidence else "阻断待复验"),
            "最近校验时间": checked_at,
            "status": "待复验" if todo_blocked else "待处理",
            "pending": True,
            "abnormal": todo_blocked or not result.get("passed"),
        }

        if existing:
            todo_id = int(existing[0]["id"])
            updated = json_store.update(TODO_MODULE, todo_id, todo_payload)
            return updated or todo_payload

        return json_store.insert(TODO_MODULE, todo_payload)

    @staticmethod
    def _merge_external(old: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """合并外部结论：同规则同包去重，旧包结论保留并维持待复验。"""
        seen = {(c.get("source_package"), c.get("rule_no")) for c in new}
        merged = list(new)
        for conclusion in old:
            key = (conclusion.get("source_package"), conclusion.get("rule_no"))
            if key not in seen:
                conclusion = dict(conclusion)
                conclusion["conclusion_state"] = "待复验"
                conclusion["usable_for_close"] = False
                merged.append(conclusion)
        return merged

    # ──────────────────────────────────────────────────────────
    # 4. 质检待办结案：区分字段证据与待复验外部结论
    # ──────────────────────────────────────────────────────────
    def list_quality_todos(self, status: str | None = None) -> list[dict[str, Any]]:
        rows = json_store.get_all(TODO_MODULE)
        if status:
            rows = [r for r in rows if r.get("待办状态") == status]
        return sorted(rows, key=lambda r: r.get("created_at", ""), reverse=True)

    def get_quality_todo(self, todo_id: int) -> dict[str, Any] | None:
        return json_store.get_by_id(TODO_MODULE, todo_id)

    def close_quality_todo(self, todo_id: int, resolution: str = "字段证据确认结案",
                           operator: str = "值班同事") -> dict[str, Any]:
        """依据当前有效复核包的字段证据结案质检待办。

        约束：
        - 只能用当前有效包的字段证据结案；只有待复验外部结论（旧包阻断）时拒绝结案；
        - 结案记录回链原消息、检查规则及规则版本、载荷版本；
        - 不触碰死信消费回执，已确认的重放仍然有效，导入旧包不会重新投递。
        """
        todo = json_store.get_by_id(TODO_MODULE, todo_id)
        if todo is None:
            return {"ok": False, "message": f"质检待办 {todo_id} 不存在"}
        if todo.get("待办状态") == "已结案":
            return {"ok": False, "message": "该待办已结案，不可重复结案"}
        if todo.get("待办状态") == "待复验" or todo.get("质检结论") == "阻断":
            return {
                "ok": False,
                "message": "存在待复验的阻断外部结论，字段证据尚不能结案，请先完成复验闭环",
            }

        field_evidence = list(todo.get("字段证据", []))
        effective_review = self.get_effective_review(int(todo.get("dlq_id", 0)))
        if not field_evidence or effective_review is None:
            return {
                "ok": False,
                "message": "缺少当前有效复核包的字段证据，仅有待复验的外部结论，不能结案，请先复验",
            }
        if effective_review.get("package_no") != todo.get("来源复核包"):
            return {
                "ok": False,
                "message": "待办依据的复核包已被新包取代，请基于当前有效包重新质检后再结案",
            }

        dlq_svc = DlqHandlerService()
        dlq_id = int(todo["dlq_id"])
        dlq_entry = json_store.get_by_id(dlq_svc.MODULE, dlq_id) or {}
        closed_at = _now()

        # 结案记录：原消息 / 检查规则及版本 / 载荷版本均可回查
        closure = {
            "结案编号": f"QCL-{todo_id:05d}-{uuid.uuid4().hex[:6]}",
            "dlq_id": dlq_id,
            "原消息编号": todo.get("记录编号", dlq_entry.get("记录编号", "")),
            "原始载荷": dlq_svc.get_full_payload(dlq_id),
            "来源复核包": todo.get("来源复核包"),
            "载荷版本": todo.get("载荷版本"),
            "检查规则": [
                {"rule_no": e.get("rule_no"), "field": e.get("field"),
                 "rule_version": e.get("rule_version"), "passed": e.get("passed")}
                for e in field_evidence
            ],
            "处置结论": resolution,
            "结案人": operator,
            "结案时间": closed_at,
            "消费凭证": dlq_entry.get("_ack_receipt"),
        }
        closure_row = json_store.insert("quality_closure_record", closure)

        json_store.update(TODO_MODULE, todo_id, {
            "待办状态": "已结案",
            "status": "已结案",
            "pending": False,
            "abnormal": False,
            "结案时间": closed_at,
            "结案记录号": closure_row.get("结案编号"),
        })

        dlq_svc.append_disposition(
            dlq_id, basis_type="质检结案",
            conclusion=f"依据字段证据结案：{resolution}",
            detail={"closure_no": closure_row.get("结案编号"),
                    "todo_id": todo_id,
                    "package_no": todo.get("来源复核包"),
                    "rules": closure["检查规则"]},
        )

        return {
            "ok": True,
            "message": f"质检待办 {todo_id} 已结案（{closure_row.get('结案编号')}），消费回执保持有效",
            "closure": closure_row,
        }

    def get_case_view(self, dlq_id: int) -> dict[str, Any]:
        """单条死信的复核工作台视图：两份处置依据 + 证据可用性 + 载荷版本历史。"""
        dlq_svc = DlqHandlerService()
        dlq_entry = json_store.get_by_id(dlq_svc.MODULE, dlq_id)
        if dlq_entry is None:
            return {"ok": False, "message": f"死信 {dlq_id} 不存在"}

        todos = [t for t in json_store.get_all(TODO_MODULE) if int(t.get("dlq_id", 0)) == dlq_id]
        closures = [c for c in json_store.get_all("quality_closure_record")
                    if int(c.get("dlq_id", 0)) == dlq_id]
        return {
            "ok": True,
            "dlq": {
                "dlq_id": dlq_id,
                "记录编号": dlq_entry.get("记录编号"),
                "处置状态": dlq_entry.get("处置状态", dlq_entry.get("status")),
                "原始队列": dlq_entry.get("原始队列"),
            },
            # 处置依据一：重放消费确认（不可变，导入旧包不影响其效力）
            "replay_basis": {
                "ack_receipt": dlq_entry.get("_ack_receipt"),
                "replayed_at": dlq_entry.get("replayed_at"),
                "effective": bool(dlq_entry.get("_ack_receipt")),
            },
            # 处置依据二：源字段质量复核
            "quality_basis": {
                "effective_review": self.get_effective_review(dlq_id),
                "todos": todos,
                "closures": closures,
            },
            "payload_versions": self.list_payload_versions(dlq_id),
            "ledger": dlq_svc.get_disposition_ledger(dlq_id),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        packages = json_store.get_all(PACKAGE_MODULE)
        records = json_store.get_all(RECORD_MODULE)
        todos = json_store.get_all(TODO_MODULE)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "死信复核数据包数据集",
            "count": len(packages),
            "by_status": {
                "已导入": sum(1 for p in packages if p.get("包状态") == "已导入"),
                "已取代": sum(1 for p in packages if p.get("包状态") == "已取代"),
            },
            "abnormal": sum(1 for t in todos if t.get("abnormal")),
            "effective_reviews": sum(1 for r in records if r.get("复核状态") == "有效"),
            "superseded_reviews": sum(1 for r in records if r.get("复核状态") == "已取代"),
            "open_quality_todos": sum(1 for t in todos if t.get("待办状态") != "已结案"),
            "operation": self.OPERATION_TERM,
        }
