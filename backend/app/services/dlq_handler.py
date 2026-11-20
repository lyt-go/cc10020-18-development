"""死信队列管理领域服务：失败消息隔离、原因分类分析、批量重放与处置策略。

提供完整的死信处理流程：消息入队→原因归类→处置决策→批量重放→结果验证。
跨模块关联：mq_consumer（来源队列）、call_trace_log（重放追踪）、data_quality_rule（质量校验）。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from collections import Counter
from datetime import datetime, timezone
from typing import Any

from app.persistence import json_store
from app.services.base import BaseModuleService


class DlqHandlerService(BaseModuleService):
    MODULE = "dlq_handler"
    ENTITY = "死信记录"
    OPERATION_TERM = "存储"
    LIST_FIELDS = ["记录编号", "原始队列", "失败原因", "原始消息体", "重试次数", "入队时间", "处置状态", "重放批次"]
    REQUIRED_FIELDS = ["记录编号", "原始队列", "失败原因"]
    STATUS_ORDER = ["草稿", "待处置", "已重放", "已丢弃"]
    ACTION_RULES = {"加入处置": "待处置", "标记重放": "已重放", "标记丢弃": "已丢弃"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        super().__init__()
        self._replay_history: list[dict[str, Any]] = []

    # ──────────────────────────────────────────────────────────
    # 处置台账：死信全生命周期处置依据（重放/复核/质检/结案统一留痕）
    # ──────────────────────────────────────────────────────────
    LEDGER_MODULE = "dlq_disposition_ledger"

    def append_disposition(self, dlq_id: int, basis_type: str, conclusion: str,
                           detail: dict[str, Any] | None = None) -> dict[str, Any]:
        """向死信处置台账追加一条不可变处置依据。

        basis_type 取值：重放凭证 / 复核导入 / 质量校验 / 质检结案。
        台账只追加不改写，保证同一条死信的多份处置依据都可回查。
        """
        entry = {
            "dlq_id": dlq_id,
            "basis_type": basis_type,
            "conclusion": conclusion,
            "detail": detail or {},
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        }
        return json_store.insert(self.LEDGER_MODULE, entry)

    def get_disposition_ledger(self, dlq_id: int) -> list[dict[str, Any]]:
        """读取单条死信的全部处置依据，按登记时间排序。"""
        rows = [r for r in json_store.get_all(self.LEDGER_MODULE) if r.get("dlq_id") == dlq_id]
        return sorted(rows, key=lambda r: r.get("created_at", ""))

    def enqueue_dead_letter(self, source_queue: str, original_message: str,
                            failure_reason: str, consumer_id: int = 0) -> dict[str, Any]:
        """将消费失败的消息加入死信队列。

        自动生成记录编号、消息指纹、入队时间戳和失败原因分类。
        """
        existing = json_store.get_all(self.MODULE)
        max_id = max((int(e.get("id", 0)) for e in existing), default=0)

        msg_fingerprint = hashlib.sha256(original_message.encode()).hexdigest()[:16]

        # 检查是否有相同指纹的已有死信（避免重复入队）
        duplicate = [e for e in existing if e.get("_fingerprint") == msg_fingerprint and e.get("status") in ("草稿", "待处置")]
        if duplicate:
            existing_dlq = duplicate[0]
            retry_count = int(existing_dlq.get("重试次数", "0") or "0") + 1
            json_store.update(self.MODULE, int(existing_dlq["id"]), {
                "重试次数": str(retry_count),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            return {
                "ok": True,
                "message": f"消息已在死信队列中（retry#{retry_count}），合并为已有记录 #{existing_dlq['id']}",
                "dlq_id": existing_dlq["id"],
                "is_duplicate": True,
            }

        record_no = f"DLQ-{source_queue[:4].upper()}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{max_id + 1:05d}"

        dlq_entry = {
            "id": max_id + 1,
            "记录编号": record_no,
            "原始队列": source_queue,
            "失败原因": failure_reason,
            "原始消息体": original_message[:200],
            "重试次数": "0",
            "入队时间": datetime.now(timezone.utc).isoformat(),
            "处置状态": "草稿",
            "重放批次": "",
            "status": "草稿",
            "pending": True,
            "abnormal": True,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "_fingerprint": msg_fingerprint,
            "_consumer_id": consumer_id,
            # 完整原始载荷单独留存：复核包导出与质检都以此为准，不随列表预览截断
            "_full_payload": original_message,
            # 处置历史（轻量时间线，完整依据见 dlq_disposition_ledger）
            "_disposition_history": [{
                "action": "入队",
                "basis": failure_reason,
                "at": datetime.now(timezone.utc).isoformat(),
            }],
        }

        # 填充LIST_FIELDS中的其他字段
        for field in self.LIST_FIELDS:
            if field not in dlq_entry:
                dlq_entry[field] = dlq_entry.get("记录编号", "") if field == "记录编号" else (
                    dlq_entry.get("原始队列", "") if field == "原始队列" else
                    dlq_entry.get("失败原因", "") if field == "失败原因" else
                    dlq_entry.get("原始消息体", "") if field == "原始消息体" else
                    dlq_entry.get("重试次数", "") if field == "重试次数" else
                    dlq_entry.get("入队时间", "") if field == "入队时间" else
                    dlq_entry.get("处置状态", "") if field == "处置状态" else
                    dlq_entry.get("重放批次", "") if field == "重放批次" else ""
                )

        json_store.insert(self.MODULE, dlq_entry)
        return {"ok": True, "message": f"死信已入队，编号 {record_no}", "dlq_id": dlq_entry["id"], "is_duplicate": False}

    def analyze_failure_reasons(self) -> dict[str, Any]:
        """分析所有死信记录的失败原因分布，提供分类统计和主要问题识别。"""
        rows = json_store.get_all(self.MODULE)
        if not rows:
            return {"total": 0, "categories": {}, "top_reasons": []}

        reason_counter = Counter()
        source_counter = Counter()

        for row in rows:
            reason = str(row.get("失败原因", "未知"))
            source = str(row.get("原始队列", "未知"))
            reason_counter[reason] += 1
            source_counter[source] += 1

        # 失败原因分类
        categories = {
            "超时类": sum(1 for r, c in reason_counter.items() if "超时" in r or "timeout" in r),
            "格式类": sum(1 for r, c in reason_counter.items() if "格式" in r or "format" in r or "序列化" in r),
            "数据类": sum(1 for r, c in reason_counter.items() if "数据" in r or "data" in r or "空" in r),
            "连接类": sum(1 for r, c in reason_counter.items() if "连接" in r or "网络" in r or "connect" in r),
            "认证类": sum(1 for r, c in reason_counter.items() if "认证" in r or "auth" in r or "权限" in r),
            "其他": sum(1 for r, c in reason_counter.items() if not any(kw in r for kw in ["超时", "timeout", "格式", "format", "序列化", "数据", "data", "空", "连接", "网络", "connect", "认证", "auth", "权限"])),
        }

        return {
            "total": len(rows),
            "categories": {k: v for k, v in categories.items() if v > 0},
            "top_reasons": reason_counter.most_common(5),
            "top_sources": source_counter.most_common(5),
            "pending_count": sum(1 for r in rows if r.get("status") in ("草稿", "待处置")),
        }

    def batch_replay(self, dlq_ids: list[int]) -> dict[str, Any]:
        """批量重放死信队列中的消息。

        流程：
        1. 验证每条死信记录是否存在且处于可重放状态
        2. 生成重放批次ID
        3. 逐条尝试重放到原始队列
        4. 记录重放结果和追踪日志

        返回重放统计和每条消息的明细结果。
        """
        if not dlq_ids:
            return {"ok": False, "message": "未指定待重放的死信记录", "replayed": 0, "failed": 0}

        batch_id = f"REPLAY-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        results = []
        replayed = 0
        replay_failed = 0

        import random
        for dlq_id in dlq_ids:
            entry = json_store.get_by_id(self.MODULE, dlq_id)
            if entry is None:
                results.append({"dlq_id": dlq_id, "ok": False, "reason": "死信记录不存在"})
                replay_failed += 1
                continue

            if entry.get("status") in ("已重放", "已丢弃"):
                results.append({"dlq_id": dlq_id, "ok": False, "reason": f"记录已{entry['status']}，不可重复重放"})
                replay_failed += 1
                continue

            # 模拟重放：70%成功率
            success = random.random() < 0.7

            if success:
                ack_at = datetime.now(timezone.utc)
                # 消费凭证：重放成功即视为消息已被下游确认消费，后续任何复核包导入都不得重新投递
                ack_receipt = {
                    "receipt_no": f"ACK-{batch_id.split('-', 1)[1]}-{dlq_id:05d}",
                    "batch_id": batch_id,
                    "dlq_id": dlq_id,
                    "record_no": entry.get("记录编号", ""),
                    "source_queue": entry.get("原始队列", ""),
                    "consumer_group": f"replay-{entry.get('_consumer_id', 0)}",
                    "acknowledged_at": ack_at.isoformat(),
                    "message_fingerprint": entry.get("_fingerprint", ""),
                }
                history = list(entry.get("_disposition_history", []))
                history.append({
                    "action": "重放确认",
                    "basis": f"批次 {batch_id} 已获消费确认 {ack_receipt['receipt_no']}",
                    "at": ack_at.isoformat(),
                })
                json_store.update(self.MODULE, dlq_id, {
                    "status": "已重放",
                    "处置状态": "已重放",
                    "pending": False,
                    "abnormal": False,
                    "重放批次": batch_id,
                    "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                    "replayed_at": ack_at.isoformat(),
                    "_ack_receipt": ack_receipt,
                    "_disposition_history": history,
                })
                # 台账留痕：重放凭证是与质量复核并列的第一类处置依据
                self.append_disposition(
                    dlq_id, basis_type="重放凭证",
                    conclusion=f"重放批次 {batch_id} 已获消费确认",
                    detail=ack_receipt,
                )
                replayed += 1
                results.append({"dlq_id": dlq_id, "ok": True, "message": "重放成功",
                                "ack_receipt": ack_receipt})
            else:
                # 重放失败：重试计数+1，保持待处置
                history = list(entry.get("_disposition_history", []))
                history.append({
                    "action": "重放失败",
                    "basis": "重放执行失败，保持待处置",
                    "at": datetime.now(timezone.utc).isoformat(),
                })
                json_store.update(self.MODULE, dlq_id, {
                    "重试次数": str(int(entry.get("重试次数", "0") or "0") + 1),
                    "status": "待处置",
                    "失败原因": entry.get("失败原因", "") + " [重放重试]",
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "_disposition_history": history,
                })
                replay_failed += 1
                results.append({"dlq_id": dlq_id, "ok": False, "reason": "重放执行失败"})

        # 记录重放历史
        self._replay_history.append({
            "batch_id": batch_id,
            "total": len(dlq_ids),
            "replayed": replayed,
            "failed": replay_failed,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

        # 跨模块：记录重放到调用追踪
        try:
            from app.services.call_trace_log import CallTraceLogService
            tracer = CallTraceLogService()
            tracer._record_trace(
                endpoint_id=0,
                action="死信批量重放",
                latency_ms=random.randint(100, 500),
                status="正常" if replay_failed == 0 else "异常",
                record_count=replayed,
            )
        except Exception:
            pass

        return {
            "ok": True,
            "message": f"批量重放完成: 成功 {replayed}/{len(dlq_ids)}, 失败 {replay_failed}",
            "batch_id": batch_id,
            "replayed": replayed,
            "failed": replay_failed,
            "total": len(dlq_ids),
            "results": results,
        }

    def get_full_payload(self, dlq_id: int) -> str | None:
        """读取死信的完整原始消息载荷（复核导出/质检校验的权威数据源）。"""
        entry = json_store.get_by_id(self.MODULE, dlq_id)
        if entry is None:
            return None
        return entry.get("_full_payload") or entry.get("原始消息体", "")

    def get_dlq_stats(self) -> dict[str, Any]:
        """获取死信队列的全局统计信息。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {}
        by_queue = {}
        for row in rows:
            s = row.get("status", "未知")
            q = row.get("原始队列", "未知")
            by_status[s] = by_status.get(s, 0) + 1
            by_queue[q] = by_queue.get(q, 0) + 1

        return {
            "total_dlq": len(rows),
            "by_status": by_status,
            "by_queue": dict(sorted(by_queue.items(), key=lambda x: x[1], reverse=True)[:10]),
            "pending_dispatch": by_status.get("草稿", 0) + by_status.get("待处置", 0),
            "replayed_count": by_status.get("已重放", 0),
            "discarded_count": by_status.get("已丢弃", 0),
            "analysis": self.analyze_failure_reasons(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        """输出死信队列管理的数据质量快照。"""
        rows = json_store.get_all(self.MODULE)
        by_status = {status: 0 for status in self.STATUS_ORDER}
        for row in rows:
            s = row.get("status", "")
            if s in by_status:
                by_status[s] += 1

        analysis = self.analyze_failure_reasons()
        ledger_rows = json_store.get_all(self.LEDGER_MODULE)
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": "死信队列记录数据集",
            "count": len(rows),
            "by_status": by_status,
            "abnormal": sum(1 for r in rows if r.get("abnormal")),
            "total_replays": len(self._replay_history),
            "failure_categories": analysis.get("categories", {}),
            "disposition_ledger_entries": len(ledger_rows),
            "acknowledged_dlq": sum(1 for r in rows if r.get("_ack_receipt")),
            "operation": self.OPERATION_TERM,
        }