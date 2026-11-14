"""数据准备流水线业务规则：状态流转、依赖判断、刷新水位与批次实例都收在这里。

调用链：准备任务动作 → 依赖判断 → 刷新水位读取 → 批次实例登记 → 状态提交。
任一环节失败，任务状态与刷新水位一起回退，不留半成品。
"""
from __future__ import annotations

import re
import threading
from datetime import date, datetime
from typing import Any

from app.services.refresh import find_policy_by_dataset
from app.store import store

MODULE = "etl"
REQUIRED_FIELDS = ["任务编号", "任务名称", "输入源"]
OPTIONAL_FIELDS = ["转换步骤", "输出目标", "调度周期", "依赖关系"]
STATUS_ORDER = ["草稿", "运行中", "已暂停", "已完成"]
FINAL_STATUS = STATUS_ORDER[-1]

# 动作 -> (允许执行的当前状态, 目标状态)：越级动作一律挡下并返回失败回执。
# 启动入口只对草稿开放；已完成可由调度再开一轮（新批次，不覆盖旧批次）。
ACTION_RULES: dict[str, tuple[set[str], str]] = {
    "启动任务": ({"草稿", "已完成"}, "运行中"),
    "暂停任务": ({"运行中"}, "已暂停"),
    "归档任务": ({"已暂停"}, "已完成"),
}

_NO_DEP_VALUES = {"", "无", "-", "—"}
_DEP_SPLIT = re.compile(r"[，,、;；\s]+")


class BatchRegistry:
    """运行实例（批次）登记处：批次号全局递增发放，历史批次只增不改。

    同一把锁保护「查状态 → 依赖判断 → 读水位 → 建批次 → 改状态」整条临界区，
    两个调度请求并发争抢同一任务时只会有一个创建实例，重复启动只认首次实例。
    """

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self._seq = 0
        self._batches: list[dict[str, Any]] = []

    def reset(self) -> None:
        with self.lock:
            self._seq = 0
            self._batches = []

    def create(self, task_code: str, start_watermark: str | None) -> dict[str, Any]:
        with self.lock:
            self._seq += 1
            batch = {
                "批次号": f"J{self._seq}",
                "任务编号": task_code,
                "状态": "运行中",
                "开始水位": start_watermark,
                "结束水位": None,
                "创建时间": datetime.now().isoformat(timespec="seconds"),
                "完成时间": None,
            }
            self._batches.append(batch)
            return batch

    def find_open(self, task_code: str) -> dict[str, Any] | None:
        with self.lock:
            for batch in self._batches:
                if batch["任务编号"] == task_code and batch["状态"] == "运行中":
                    return batch
            return None

    def close(self, batch: dict[str, Any], end_watermark: str | None) -> None:
        with self.lock:
            batch["状态"] = "已完成"
            batch["结束水位"] = end_watermark
            batch["完成时间"] = datetime.now().isoformat(timespec="seconds")

    def list_all(self) -> list[dict[str, Any]]:
        with self.lock:
            return [dict(batch) for batch in self._batches]


batch_registry = BatchRegistry()


def parse_dependencies(entry: dict[str, Any]) -> list[str]:
    """把「依赖关系」解析成上游任务编号列表；空、无、横线都视为无依赖。"""
    raw = str(entry.get("依赖关系") or "").strip()
    if raw in _NO_DEP_VALUES:
        return []
    return [token for token in _DEP_SPLIT.split(raw) if token and token not in _NO_DEP_VALUES]


class EtlService:
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
            rows = [row for row in rows if keyword in str(row.get("任务编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def list_batches(self) -> list[dict[str, Any]]:
        return batch_registry.list_all()

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, f"缺少必填字段：{'、'.join(missing)}"
        code = str(values["任务编号"]).strip()
        if self._find_by_code(code) is not None:
            return None, f"任务编号 {code} 已存在，登记被拒绝"
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        # 转换步骤、输出目标、调度周期、依赖关系全部落库，不再静默丢弃
        for field in REQUIRED_FIELDS + OPTIONAL_FIELDS:
            value = values.get(field)
            entry[field] = str(value).strip() if value is not None else ""
        entry["status"] = STATUS_ORDER[0]
        entry["任务状态"] = STATUS_ORDER[0]
        entry["当前批次"] = None
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, ""

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于数据准备流水线可执行范围"
        with batch_registry.lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"准备任务 {entry_id} 不存在或已归档"
            current = str(entry.get("status", ""))
            if action == "启动任务" and current == "运行中":
                return self._repeat_start(entry)
            allowed_from, target = ACTION_RULES[action]
            if current not in allowed_from:
                return None, f"准备任务当前状态「{current}」不允许执行「{action}」，越级操作已拦截"
            if action == "启动任务":
                return self._start(entry, target)
            if action == "暂停任务":
                self._commit_status(entry, target, entry.get("当前批次"))
                return entry, f"准备任务已暂停，在途实例 {entry.get('当前批次') or '无'} 保留"
            return self._archive(entry, target)

    def _repeat_start(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """重复启动只返回首次运行实例；旧数据运行中却没有在途批次时补建首个实例。"""
        batch = batch_registry.find_open(str(entry.get("任务编号", "")))
        if batch is None:
            _, watermark, error = self._read_watermark(entry)
            if error:
                return None, error
            batch = batch_registry.create(str(entry.get("任务编号", "")), watermark)
            entry["当前批次"] = batch["批次号"]
        return entry, f"准备任务已在运行中，重复启动返回首次运行实例 {batch['批次号']}"

    def _start(self, entry: dict[str, Any], target: str) -> tuple[dict[str, Any] | None, str]:
        # 依赖判断：上游未完成，下游不得运行
        error = self._check_dependencies(entry)
        if error:
            return None, error
        # 刷新水位读取
        _, watermark, error = self._read_watermark(entry)
        if error:
            return None, error
        # 全部校验通过后才提交：批次实例与任务状态一起生效
        batch = batch_registry.create(str(entry.get("任务编号", "")), watermark)
        self._commit_status(entry, target, batch["批次号"])
        return entry, f"准备任务已启动，运行实例 {batch['批次号']}（开始水位：{watermark or '无'}）"

    def _archive(self, entry: dict[str, Any], target: str) -> tuple[dict[str, Any] | None, str]:
        dataset = str(entry.get("输出目标") or "").strip()
        policy = None
        if dataset:
            policy = find_policy_by_dataset(dataset)
            if policy is None:
                return None, f"输出目标 {dataset} 未配置刷新策略，无法推进刷新水位，归档已取消，任务状态未变更"
        batch = batch_registry.find_open(str(entry.get("任务编号", "")))
        end_watermark = date.today().isoformat()
        # 快照：失败时上游任务状态与刷新水位一起回退
        entry_snapshot = dict(entry)
        policy_watermark = str(policy.get("刷新水位", "")) if policy is not None else None
        batch_snapshot = dict(batch) if batch is not None else None
        try:
            if batch is not None:
                batch_registry.close(batch, end_watermark if policy is not None else None)
            if policy is not None:
                policy["刷新水位"] = end_watermark
            self._commit_status(entry, target, None)
        except Exception:
            entry.clear()
            entry.update(entry_snapshot)
            if policy is not None and policy_watermark is not None:
                policy["刷新水位"] = policy_watermark
            if batch is not None and batch_snapshot is not None:
                batch.clear()
                batch.update(batch_snapshot)
            raise
        message = f"准备任务已归档，状态流转至已完成，刷新水位推进至 {end_watermark}"
        if batch is not None:
            message += f"，运行实例 {batch['批次号']} 关闭"
        return entry, message

    def _check_dependencies(self, entry: dict[str, Any]) -> str | None:
        for dep_code in parse_dependencies(entry):
            dep = self._find_by_code(dep_code)
            if dep is None:
                return f"依赖任务 {dep_code} 不存在，下游不得运行"
            if dep.get("status") != FINAL_STATUS:
                return f"依赖任务 {dep_code} 未完成（当前状态：{dep.get('status')}），下游不得运行"
        return None

    def _read_watermark(self, entry: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None, str | None]:
        """刷新水位读取：按输出目标匹配刷新策略，返回 (策略, 水位, 错误)。"""
        dataset = str(entry.get("输出目标") or "").strip()
        if not dataset:
            return None, None, None
        policy = find_policy_by_dataset(dataset)
        if policy is None:
            return None, None, f"输出目标 {dataset} 未配置刷新策略，无法读取刷新水位"
        return policy, str(policy.get("刷新水位") or ""), None

    def _commit_status(self, entry: dict[str, Any], status: str, batch_no: str | None) -> None:
        entry["status"] = status
        entry["任务状态"] = status  # 列表与详情都读这个字段，必须与 status 同步
        entry["当前批次"] = batch_no
        entry["pending"] = status != FINAL_STATUS
        entry["abnormal"] = False

    def _find_by_code(self, code: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if str(row.get("任务编号", "")) == code:
                return row
        return None
