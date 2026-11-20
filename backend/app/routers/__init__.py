"""业务路由汇总：所有模块共用一套资源路由，由领域服务完成分派。"""
from __future__ import annotations

from app.routers import resource, pipeline, dlq_review

# 专用复核路由含 /api/dlq-review/... 固定路径，必须先于通用 /api/{module} 注册，
# 否则固定路径会被资源路由的动态 {entry_id} 抢先匹配。
ROUTERS = [dlq_review, resource, pipeline]
