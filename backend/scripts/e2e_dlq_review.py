"""端到端 HTTP 验证：死信复核数据包完整闭环（通过 FastAPI 路由）。"""
from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, "/workspace/backend")

from app.persistence import JsonStore
import app.persistence as persistence
from app.services import base as base_mod
from app.services import dlq_handler as dlq_mod
from app.services import dlq_review as review_mod
from app.services import data_quality_rule as dq_mod
from app.services import mq_consumer as mq_mod
from app.services import call_trace_log as ctl_mod

# 隔离存储
_tmp = tempfile.TemporaryDirectory()
_store = JsonStore(base_dir=_tmp.name)
_orig = persistence.json_store
persistence.json_store = _store
for m in (base_mod, dlq_mod, review_mod, dq_mod, mq_mod, ctl_mod):
    m.json_store = _store

from fastapi.testclient import TestClient  # noqa: E402
from app.main import app  # noqa: E402

client = TestClient(app)


def check(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print("  ✓", msg)


print("== 健康检查 ==")
r = client.get("/api/health")
check(r.status_code == 200, "服务健康")

print("== 1. 准备死信与质量规则 ==")
dlq = dlq_mod.DlqHandlerService()
d1 = int(dlq.enqueue_dead_letter("order_queue", '{"order_id":"O-1"}', "数据格式异常", 3)["dlq_id"])
d2 = int(dlq.enqueue_dead_letter("order_queue", '{"name":"完整记录","order_id":"O-2"}', "处理超时", 3)["dlq_id"])
quality = dq_mod.DataQualityRuleService()
rule, missing = quality.create_entry({
    "规则编号": "QR-NAME", "数据来源": "dlq_review", "校验字段": "name",
    "校验表达式": "not_empty", "阈值下界": "0", "阈值上界": "0",
    "阻断策略": "阻断", "规则版本": "v9",
})
check(not missing, "创建阻断质量规则")
quality.run_action(int(rule["id"]), "标记生效")

print("== 2. 导出复核包（下载物） ==")
r = client.post("/api/dlq-review/export", json={"dlq_ids": [d1, d2]})
check(r.status_code == 200, "导出接口返回 200")
check("attachment" in r.headers.get("content-disposition", ""), "以附件形式下载")
package_a = r.json()
check(package_a["item_count"] == 2, "包含两条失败复核记录")
check(all(it["原始载荷"] for it in package_a["items"]), "每条带完整原始载荷")
check(all(it["处置历史"] for it in package_a["items"]), "每条带处置历史")
pkg_a_no = package_a["package_no"]

print("== 3. 并发导入（幂等：一条死信一份有效复核） ==")
# 同包重复导入
r1 = client.post("/api/dlq-review/packages")
r_imp1 = client.post("/api/dlq-review/import", json={"package": package_a})
r_imp2 = client.post("/api/dlq-review/import", json={"package": package_a})
check(r_imp1.status_code == 200 and r_imp1.json()["registered"] == 2, "首次导入登记 2 条")
check(r_imp2.json()["duplicate"] is True and r_imp2.json()["registered"] == 0, "同包重复导入幂等返回")

# 生成新包并导入，旧包被取代
package_b = client.post("/api/dlq-review/export", json={"dlq_ids": [d1]}).json()
r_imp3 = client.post("/api/dlq-review/import", json={"package": package_b})
check(r_imp3.json()["registered"] == 1, "新包登记 1 条有效复核")
r_pkgs = client.get("/api/dlq-review/packages").json()
old_pkg = next(p for p in r_pkgs["items"] if p["package_no"] == pkg_a_no)
check(old_pkg["包状态"] == "已取代", "旧包标记为已取代但保留")

# 旧包载荷版本可回查
r_ver = client.get(f"/api/dlq-review/cases/{d1}/payload-versions").json()
pkg_nos = {v["package_no"] for v in r_ver["items"]}
check(pkg_a_no in pkg_nos and package_b["package_no"] in pkg_nos, "新旧包载荷版本均可回查")

print("== 4. 质量校验串联，结论回写待办与台账 ==")
r_qc = client.post("/api/dlq-review/quality-check", json={"package_no": package_b["package_no"]})
check(r_qc.status_code == 200, "质量校验接口成功")
qc = r_qc.json()
check(qc["blocked"] >= 1, "缺 name 字段的载荷被阻断")

todos = client.get("/api/dlq-review/todos").json()["items"]
blocked_todo = next(t for t in todos if t["dlq_id"] == d1)
check(blocked_todo["待办状态"] == "待复验", "阻断结论进入待复验")
check(any(c["usable_for_close"] is False for c in blocked_todo["外部结论"]), "外部结论标记不可结案")

ledger = client.get(f"/api/dlq-review/cases/{d1}/ledger").json()["items"]
check(any(e["basis_type"] == "质量校验" for e in ledger), "处置台账写入质量校验依据")

print("== 5. 阻断待办不可仅凭外部结论结案 ==")
r_close = client.post(f"/api/dlq-review/todos/{blocked_todo['id']}/close", json={})
check(r_close.status_code == 409, "阻断待复验状态拒绝结案（409）")

print("== 6. 可结案证据：通过校验的载荷正常结案 ==")
pkg_c = client.post("/api/dlq-review/export", json={"dlq_ids": [d2]}).json()
client.post("/api/dlq-review/import", json={"package": pkg_c})
client.post("/api/dlq-review/quality-check", json={"package_no": pkg_c["package_no"]})
todo2 = next(t for t in client.get("/api/dlq-review/todos").json()["items"] if t["dlq_id"] == d2)
check(todo2["证据可用性"] == "可结案", "通过载荷具备可结案字段证据")
r_closed = client.post(f"/api/dlq-review/todos/{todo2['id']}/close", json={"resolution": "字段核验无误"})
check(r_closed.status_code == 200, "字段证据结案成功")
closure = r_closed.json()["closure"]
check(closure["检查规则"][0]["rule_version"] == "v9", "结案记录回查规则及版本")
check(closure["原消息编号"], "结案记录回链原消息")

print("== 7. 重放消费凭证保留，导入旧包不重投 ==")
import random
random.seed(1)
replay = dlq.batch_replay([d2])
check(replay["replayed"] == 1, "死信重放成功")
receipt_before = _store.get_by_id("dlq_handler", d2)["_ack_receipt"]
# 再导入一个旧包
pkg_d = client.post("/api/dlq-review/export", json={"dlq_ids": [d2]}).json()
client.post("/api/dlq-review/import", json={"package": pkg_d})
entry_after = _store.get_by_id("dlq_handler", d2)
check(entry_after["_ack_receipt"] == receipt_before, "消费回执在导入后保持有效")
check(entry_after["status"] == "已重放", "导入旧包未改变死信状态、未重新投递")

print("== 8. 双处置依据工作台视图 ==")
view = client.get(f"/api/dlq-review/cases/{d2}").json()
check(view["replay_basis"]["effective"] is True, "处置依据一：重放消费确认有效")
check(view["quality_basis"]["closures"], "处置依据二：质量复核已结案")
basis = {e["basis_type"] for e in view["ledger"]}
check({"重放凭证", "复核导入", "质量校验", "质检结案"} <= basis, "台账含四类处置依据")

print("\n全部端到端断言通过 ✅")
_tmp.cleanup()
persistence.json_store = _orig
