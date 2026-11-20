"""死信复核数据包测试：覆盖审核交付物的完整闭环。

覆盖需求要点：
1. 从待审死信原始消息导出复核包（含完整原始载荷 + 处置历史 + 消费凭证）
2. 并发/重复导入只登记一份有效复核，旧包载荷版本可回查
3. 已重放记录保留消费凭证，导入旧包不重新投递
4. 质量校验串联：批量校验原始载荷，结论回写质检待办与处置台账
5. 同一条死信两份处置依据；旧包阻断结论只能待复验，不能用于结案
6. 结案记录可回查原消息、检查规则及版本；消费回执始终有效
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.persistence import JsonStore
import app.persistence
from app.services.base import BaseModuleService
from app.services.dlq_handler import DlqHandlerService
from app.services.dlq_review import (
    DlqReviewService, PACKAGE_MODULE, RECORD_MODULE, SNAPSHOT_MODULE, TODO_MODULE,
)


class _IsolatedCase:
    """每个用例独立临时存储，并把所有服务模块的 json_store 重绑过去。"""

    def setup_method(self):
        import app.services.base as base_mod
        import app.services.dlq_handler as dlq_mod
        import app.services.dlq_review as review_mod
        import app.services.data_quality_rule as dq_mod
        import app.services.mq_consumer as mq_mod
        import app.services.call_trace_log as ctl_mod

        self._tmp = tempfile.TemporaryDirectory()
        self.store = JsonStore(base_dir=self._tmp.name)
        self._orig = app.persistence.json_store
        app.persistence.json_store = self.store
        self._mods = [base_mod, dlq_mod, review_mod, dq_mod, mq_mod, ctl_mod]
        for mod in self._mods:
            mod.json_store = self.store

        self.dlq = DlqHandlerService()
        self.review = DlqReviewService()

    def teardown_method(self):
        for mod in self._mods:
            mod.json_store = self._orig
        app.persistence.json_store = self._orig
        self._tmp.cleanup()

    def _make_dlq(self, payload: str, reason: str = "数据格式异常", queue: str = "order_queue"):
        result = self.dlq.enqueue_dead_letter(queue, payload, reason, consumer_id=7)
        return int(result["dlq_id"])

    def _make_quality_rule(self, field: str, expression: str, policy: str = "阻断",
                           source: str = "dlq_review", version: str = "v3"):
        svc = self.review  # 仅用于占位；规则直接写存储
        from app.services.data_quality_rule import DataQualityRuleService
        quality = DataQualityRuleService()
        entry, missing = quality.create_entry({
            "规则编号": f"QR-{field}",
            "数据来源": source,
            "校验字段": field,
            "校验表达式": expression,
            "阈值下界": "0",
            "阈值上界": "0",
            "阻断策略": policy,
            "规则版本": version,
        })
        assert not missing
        quality.run_action(int(entry["id"]), "标记生效")
        return entry


class TestExportPackage(_IsolatedCase):
    def test_export_contains_full_payload_and_history(self):
        payload = '{"order_id": "O-1", "amount": 100}'
        dlq_id = self._make_dlq(payload)
        result = self.review.export_review_package([dlq_id])
        assert result["ok"]
        package = result["package"]
        assert package["item_count"] == 1
        item = package["items"][0]
        # 完整原始载荷（未被 200 字截断）
        assert item["原始载荷"] == payload
        assert item["处置历史"] and item["处置历史"][0]["action"] == "入队"
        assert item["消息指纹"]

    def test_export_default_only_pending(self):
        self._make_dlq('{"a":1}')
        dlq_id_2 = self._make_dlq('{"a":2}')
        # 第二条标记丢弃后不应进入默认导出
        self.dlq.run_action(dlq_id_2, "标记丢弃")
        result = self.review.export_review_package()
        assert result["ok"]
        assert result["package"]["item_count"] == 1

    def test_export_empty_returns_not_ok(self):
        result = self.review.export_review_package()
        assert not result["ok"]


class TestImportIdempotency(_IsolatedCase):
    def _package(self, dlq_id: int, tag: str = "A"):
        return self.review.export_review_package([dlq_id])["package"]

    def test_duplicate_package_import_is_idempotent(self):
        dlq_id = self._make_dlq('{"order_id": "O-9"}')
        package = self._package(dlq_id)
        first = self.review.import_review_package(package)
        second = self.review.import_review_package(package)
        assert first["registered"] == 1
        assert second["duplicate"] is True
        assert second["registered"] == 0
        # 仍只有一份有效复核
        records = [r for r in self.store.get_all(RECORD_MODULE) if r["dlq_id"] == dlq_id]
        assert len(records) == 1
        assert records[0]["复核状态"] == "有效"

    def test_concurrent_imports_leave_single_effective_review(self):
        dlq_id = self._make_dlq('{"order_id": "O-CONC"}')
        packages = [self._package(dlq_id, str(i)) for i in range(6)]
        # 同一内容导出多个不同包号，并发导入
        errors = []

        def worker(pkg):
            try:
                self.review.import_review_package(pkg)
            except Exception as exc:  # pragma: no cover - 并发不应抛错
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(p,)) for p in packages]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
        records = [r for r in self.store.get_all(RECORD_MODULE) if r["dlq_id"] == dlq_id]
        effective = [r for r in records if r["复核状态"] == "有效"]
        # 任意并发下只有一份有效复核
        assert len(effective) == 1
        # 其余都转为已取代
        assert len([r for r in records if r["复核状态"] == "已取代"]) == len(records) - 1

    def test_old_package_payload_versions_remain_queryable(self):
        dlq_id = self._make_dlq('{"order_id": "O-VER"}')
        old_pkg = self._package(dlq_id)
        self.review.import_review_package(old_pkg)
        new_pkg = self._package(dlq_id)
        self.review.import_review_package(new_pkg)

        versions = self.review.list_payload_versions(dlq_id)
        # 新旧两个包的载荷版本都可回查
        assert len(versions) == 2
        pkg_nos = {v["package_no"] for v in versions}
        assert old_pkg["package_no"] in pkg_nos
        assert new_pkg["package_no"] in pkg_nos

        # 旧包整体标记为已取代
        packages = {p["package_no"]: p for p in self.review.list_packages()}
        assert packages[old_pkg["package_no"]]["包状态"] == "已取代"
        assert packages[new_pkg["package_no"]]["包状态"] == "已导入"


class TestAcknowledgementPreserved(_IsolatedCase):
    def test_replay_creates_ack_receipt_and_import_never_redelivers(self):
        import random
        random.seed(1)  # 让重放稳定成功
        dlq_id = self._make_dlq('{"order_id": "O-ACK"}')
        replay = self.dlq.batch_replay([dlq_id])
        assert replay["replayed"] == 1
        receipt = replay["results"][0]["ack_receipt"]
        assert receipt["receipt_no"].startswith("ACK-")

        entry = self.store.get_by_id("dlq_handler", dlq_id)
        assert entry["_ack_receipt"]["receipt_no"] == receipt["receipt_no"]
        assert entry["status"] == "已重放"

        # 已重放记录导出的复核包携带当时的消费凭证
        package = self.review.export_review_package([dlq_id])["package"]
        assert package["items"][0]["消费凭证"]["receipt_no"] == receipt["receipt_no"]

        # 导入旧包：登记复核，但死信状态/回执不变，不重新投递
        before = self.store.get_by_id("dlq_handler", dlq_id)
        self.review.import_review_package(package)
        after = self.store.get_by_id("dlq_handler", dlq_id)
        assert after["status"] == "已重放"
        assert after["_ack_receipt"] == before["_ack_receipt"]
        assert after["重放批次"] == before["重放批次"]

        # 台账含重放凭证依据
        ledger = self.dlq.get_disposition_ledger(dlq_id)
        assert any(e["basis_type"] == "重放凭证" for e in ledger)


class TestQualityChain(_IsolatedCase):
    def test_quality_check_writes_todo_and_ledger(self):
        # 字段 name 非空 + 阻断规则
        self._make_quality_rule("name", "not_empty", policy="阻断")
        dlq_id = self._make_dlq('{"order_id": "O-Q"}')  # 缺少 name 字段
        package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(package)

        result = self.review.run_quality_check(package["package_no"])
        assert result["ok"]
        assert result["blocked"] == 1

        todos = self.review.list_quality_todos()
        assert len(todos) == 1
        todo = todos[0]
        assert todo["质检结论"] == "阻断"
        assert todo["待办状态"] == "待复验"

        # 台账写入质量校验依据
        ledger = self.dlq.get_disposition_ledger(dlq_id)
        assert any(e["basis_type"] == "质量校验" and e["conclusion"] == "阻断" for e in ledger)

    def test_passing_payload_closeable_with_field_evidence(self):
        self._make_quality_rule("name", "not_empty", policy="警告", version="v5")
        dlq_id = self._make_dlq('{"name": "有效名称"}')
        package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(package)
        self.review.run_quality_check(package["package_no"])

        todo = self.review.list_quality_todos()[0]
        assert todo["证据可用性"] == "可结案"
        assert todo["字段证据"] and todo["字段证据"][0]["rule_version"] == "v5"

        closed = self.review.close_quality_todo(int(todo["id"]))
        assert closed["ok"]
        closure = closed["closure"]
        # 结案记录可回查原消息、检查规则及版本
        assert closure["原消息编号"]
        assert closure["检查规则"][0]["rule_version"] == "v5"
        assert closure["载荷版本"] == package["payload_version"]
        assert self.review.list_quality_todos()[0]["待办状态"] == "已结案"

    def test_old_package_blocking_conclusion_cannot_close_case(self):
        # 阻断规则：name 非空
        self._make_quality_rule("name", "not_empty", policy="阻断")
        dlq_id = self._make_dlq('{"order_id": "O-OLD"}')

        # 旧包导入并质检 -> 阻断待复验
        old_package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(old_package)
        self.review.run_quality_check(old_package["package_no"])
        old_todo = self.review.list_quality_todos()[0]
        assert old_todo["待办状态"] == "待复验"
        # 阻断结论属于待复验外部结论，不能结案
        refused = self.review.close_quality_todo(int(old_todo["id"]))
        assert not refused["ok"]

        # 新包导入取代旧包；旧包阻断结论保留在外部结论列且仍待复验
        new_package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(new_package)
        self.review.run_quality_check(new_package["package_no"])
        todo = self.review.list_quality_todos()[0]
        # 外部结论保留旧包阻断记录
        assert any(c["source_package"] == old_package["package_no"]
                   and c["conclusion_state"] == "待复验"
                   and c["usable_for_close"] is False
                   for c in todo["外部结论"])

    def test_two_basis_distinguished_in_case_view(self):
        import random
        random.seed(1)
        self._make_quality_rule("name", "not_empty", policy="警告")
        dlq_id = self._make_dlq('{"name": "双依据"}')
        # 依据一：重放消费确认
        self.dlq.batch_replay([dlq_id])
        # 依据二：质量复核
        package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(package)
        self.review.run_quality_check(package["package_no"])

        view = self.review.get_case_view(dlq_id)
        assert view["ok"]
        # 重放依据有效
        assert view["replay_basis"]["effective"] is True
        assert view["replay_basis"]["ack_receipt"]["receipt_no"]
        # 质量依据存在有效复核
        assert view["quality_basis"]["effective_review"]["package_no"] == package["package_no"]
        # 台账同时含两类处置依据
        basis_types = {e["basis_type"] for e in view["ledger"]}
        assert "重放凭证" in basis_types
        assert "质量校验" in basis_types

    def test_close_todo_rejects_when_package_superseded(self):
        self._make_quality_rule("name", "not_empty", policy="警告")
        dlq_id = self._make_dlq('{"name": "x"}')
        old_package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(old_package)
        self.review.run_quality_check(old_package["package_no"])
        todo = self.review.list_quality_todos()[0]

        # 新包取代旧包后，旧待办不能再结案
        new_package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(new_package)
        refused = self.review.close_quality_todo(int(todo["id"]))
        assert not refused["ok"]
        assert "取代" in refused["message"]

    def test_replay_acked_then_old_blocking_conclusion_distinguished(self):
        """完整剧情：重放已确认 → 源字段复核未结案 → 旧包阻断结论到达。

        值班同事从质检待办继续处理时，必须区分可结案的字段证据与待复验外部结论；
        已确认的消费回执始终有效，导入旧包不会重新投递。
        """
        import random
        random.seed(1)
        # name 非空、阻断
        self._make_quality_rule("name", "not_empty", policy="阻断", version="v7")
        # 载荷缺 name，会触发阻断
        dlq_id = self._make_dlq('{"order_id": "O-STORY"}')

        # ① 重放已获消费确认（消费凭证固定下来）
        replay = self.dlq.batch_replay([dlq_id])
        assert replay["replayed"] == 1
        receipt = self.store.get_by_id("dlq_handler", dlq_id)["_ack_receipt"]

        # ② 第一个复核包导入并质检（源字段质量复核尚未结案，阻断待复验）
        pkg_one = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(pkg_one)
        self.review.run_quality_check(pkg_one["package_no"])
        todo = self.review.list_quality_todos()[0]
        assert todo["待办状态"] == "待复验"
        assert todo["字段证据"], "有效包提供字段证据"
        assert all(c["usable_for_close"] is False for c in todo["外部结论"]), "阻断结论不可结案"

        # ③ 收到旧包（已被新包取代）的阻断结论
        pkg_two = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(pkg_two)  # pkg_one 变已取代，pkg_two 有效
        # 对旧包 pkg_one 再跑一次质检，制造「旧包阻断结论」
        self.review.run_quality_check(pkg_one["package_no"])

        todo = self.review.list_quality_todos()[0]
        # 字段证据仍锚定当前有效包，未被旧包清空
        assert todo["字段证据"], "旧包质检不得清空有效包字段证据"
        # 外部结论同时含两个包的阻断，且全部待复验、不可结案
        ext_pkgs = {c["source_package"] for c in todo["外部结论"]}
        assert pkg_one["package_no"] in ext_pkgs
        assert all(c["conclusion_state"] == "待复验" for c in todo["外部结论"])
        assert all(c["usable_for_close"] is False for c in todo["外部结论"])

        # 阻断未闭环，不能结案
        assert not self.review.close_quality_todo(int(todo["id"]))["ok"]

        # 消费回执仍有效，死信保持已重放，未被重新投递
        entry = self.store.get_by_id("dlq_handler", dlq_id)
        assert entry["_ack_receipt"] == receipt
        assert entry["status"] == "已重放"

        # 工作台视图同时呈现两份处置依据
        view = self.review.get_case_view(dlq_id)
        assert view["replay_basis"]["ack_receipt"]["receipt_no"] == receipt["receipt_no"]
        assert view["quality_basis"]["effective_review"]["package_no"] == pkg_two["package_no"]


class TestSnapshot(_IsolatedCase):
    def test_domain_snapshot_counts(self):
        dlq_id = self._make_dlq('{"name": "s"}')
        package = self.review.export_review_package([dlq_id])["package"]
        self.review.import_review_package(package)
        snap = self.review.domain_snapshot()
        assert snap["module"] == "dlq_review"
        assert snap["effective_reviews"] == 1
        assert snap["count"] == 1
