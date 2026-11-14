"""数据定时刷新业务规则：状态流转、字段校验与刷新水位都收在这里。"""
from __future__ import annotations

from typing import Any

from app.store import store

MODULE = "refresh"
REQUIRED_FIELDS = ["策略编号", "目标数据集", "刷新周期"]
OPTIONAL_FIELDS = ["刷新范围", "增量字段", "全量标记", "下一次刷新", "刷新水位"]
STATUS_ORDER = ["已创建", "生效中", "已暂停", "已关闭"]
FINAL_STATUS = STATUS_ORDER[-1]

# 动作 -> (允许执行的当前状态, 目标状态)：越级动作一律挡下并回执
ACTION_RULES: dict[str, tuple[set[str], str]] = {
    "启用刷新": ({"已创建"}, "生效中"),
    "暂停刷新": ({"生效中"}, "已暂停"),
    "关闭刷新": ({"已暂停"}, "已关闭"),
}


def find_policy_by_dataset(dataset: str) -> dict[str, Any] | None:
    """按目标数据集查找刷新策略：准备流水线读取刷新水位走这里。"""
    for row in store.rows(MODULE):
        if str(row.get("目标数据集", "")) == dataset:
            return row
    return None


class RefreshService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("策略编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, f"缺少必填字段：{'、'.join(missing)}"
        code = str(values["策略编号"]).strip()
        if any(str(row.get("策略编号", "")) == code for row in store.rows(MODULE)):
            return None, f"策略编号 {code} 已存在，登记被拒绝"
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        for field in REQUIRED_FIELDS + OPTIONAL_FIELDS:
            value = values.get(field)
            entry[field] = str(value).strip() if value is not None else ""
        entry["status"] = STATUS_ORDER[0]
        entry["策略状态"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, ""

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据定时刷新可执行范围"
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"刷新策略 {entry_id} 不存在或已归档"
        allowed_from, target = ACTION_RULES[action]
        current = str(entry.get("status", ""))
        if current not in allowed_from:
            return None, f"刷新策略当前状态「{current}」不允许执行「{action}」，越级操作已拦截"
        entry["status"] = target
        entry["策略状态"] = target
        entry["pending"] = target != FINAL_STATUS
        entry["abnormal"] = False
        return entry, f"刷新策略已{action}"
