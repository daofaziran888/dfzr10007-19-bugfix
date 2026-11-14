"""数据准备流水线验收测试：状态机、依赖闸门、刷新水位、批次实例与并发争抢。"""
from __future__ import annotations

import threading
from datetime import date

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.services.etl import batch_registry
from app.store import store

client = TestClient(app)


@pytest.fixture(autouse=True)
def fresh_state():
    store.reset()
    batch_registry.reset()
    yield
    store.reset()
    batch_registry.reset()


def act(entry_id: int, action: str) -> dict:
    response = client.post(f"/api/etl/{entry_id}/actions", json={"values": {"action": action}})
    assert response.status_code == 200
    return response.json()


def get_entry(entry_id: int) -> dict:
    response = client.get(f"/api/etl/{entry_id}")
    assert response.status_code == 200
    return response.json()


def get_batches() -> list[dict]:
    response = client.get("/api/etl/batches")
    assert response.status_code == 200
    return response.json()["items"]


def get_refresh(entry_id: int) -> dict:
    response = client.get(f"/api/refresh/{entry_id}")
    assert response.status_code == 200
    return response.json()


# ---------- 状态机：入口按阶段开放，越级动作被挡下 ----------

def test_start_moves_draft_to_running_and_opens_batch_j1():
    result = act(1, "启动任务")
    assert result["ok"] is True
    entry = result["entry"]
    assert entry["status"] == "运行中"
    assert entry["任务状态"] == "运行中"
    assert entry["当前批次"] == "J1"
    batches = get_batches()
    assert len(batches) == 1
    assert batches[0]["批次号"] == "J1"
    assert batches[0]["任务编号"] == "ETL-0001"
    # 刷新水位读取：批次开始水位取自输出目标对应的刷新策略
    assert batches[0]["开始水位"] == get_refresh(1)["刷新水位"] == "2026-11-13"


def test_out_of_order_actions_are_blocked_without_residue():
    # 草稿不允许暂停、归档
    assert act(1, "暂停任务")["ok"] is False
    assert act(1, "归档任务")["ok"] is False
    # 已暂停不允许启动、暂停
    assert act(3, "启动任务")["ok"] is False
    assert act(3, "暂停任务")["ok"] is False
    # 越级后无残留：状态、批次、水位都保持原样
    assert get_entry(1)["status"] == "草稿"
    assert get_entry(3)["status"] == "已暂停"
    assert get_batches() == []
    assert get_refresh(3)["刷新水位"] == "2026-11-12"


def test_completed_task_rejects_pause_and_archive():
    act(3, "归档任务")
    assert get_entry(3)["status"] == "已完成"
    # 完成态关闭全部入口：暂停、归档都被挡下
    assert act(3, "暂停任务")["ok"] is False
    assert act(3, "归档任务")["ok"] is False
    assert get_entry(3)["status"] == "已完成"


def test_full_status_flow_follows_refresh_watermark():
    today = date.today().isoformat()
    act(1, "启动任务")
    act(1, "暂停任务")
    assert get_entry(1)["status"] == "已暂停"
    result = act(1, "归档任务")
    assert result["ok"] is True
    entry = get_entry(1)
    assert entry["status"] == "已完成"
    assert entry["任务状态"] == "已完成"
    assert entry["当前批次"] is None
    # 状态流转跟随刷新水位：归档后水位推进、批次关闭
    assert get_refresh(1)["刷新水位"] == today
    batch = get_batches()[0]
    assert batch["状态"] == "已完成"
    assert batch["结束水位"] == today


# ---------- 依赖判断：上游未完成，下游不得运行 ----------

def test_downstream_blocked_until_dependency_completed():
    # ETL-0003 依赖 ETL-0002（运行中），归档到已完成后启动仍被依赖闸门挡下
    act(3, "归档任务")
    result = act(3, "启动任务")
    assert result["ok"] is False
    assert "ETL-0002" in result["message"]
    assert get_entry(3)["status"] == "已完成"  # 未产生残留
    assert get_batches() == []


def test_pipeline_chain_end_to_end_and_batches_never_overwritten():
    today = date.today().isoformat()
    # ETL-0001 启动批次 J1
    assert act(1, "启动任务")["ok"] is True
    # ETL-0001 走完全程，ETL-0002 解除依赖阻塞
    act(1, "暂停任务")
    act(1, "归档任务")
    # ETL-0002 是运行中的旧数据（无在途批次），先归位再走完全程
    act(2, "暂停任务")
    act(2, "归档任务")
    assert get_entry(2)["status"] == "已完成"
    # ETL-0003 完成后新启动只能生成 J2，不能覆盖 J1
    act(3, "归档任务")
    result = act(3, "启动任务")
    assert result["ok"] is True
    assert result["entry"]["当前批次"] == "J2"
    batches = get_batches()
    assert [b["批次号"] for b in batches] == ["J1", "J2"]
    j1 = batches[0]
    assert j1["任务编号"] == "ETL-0001"
    assert j1["状态"] == "已完成"
    assert j1["开始水位"] == "2026-11-13"
    assert j1["结束水位"] == today
    j2 = batches[1]
    assert j2["任务编号"] == "ETL-0003"
    assert j2["状态"] == "运行中"
    assert j2["开始水位"] == today  # ETL-0003 归档时已把 REFR-0003 水位推进到今天


# ---------- 幂等与并发：重复启动只认首次实例，争抢只有一个成功 ----------

def test_repeated_start_returns_first_instance():
    act(1, "启动任务")
    result = act(1, "启动任务")
    assert result["ok"] is True
    assert "J1" in result["message"]
    assert result["entry"]["当前批次"] == "J1"
    assert len(get_batches()) == 1


def test_concurrent_start_only_one_batch_created():
    from app.routers.etl import service

    results: list[tuple[dict | None, str]] = []
    lock = threading.Lock()

    def worker():
        outcome = service.run_action(1, "启动任务")
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    batches = batch_registry.list_all()
    assert len(batches) == 1  # 并发争抢只建了一个实例
    assert batches[0]["批次号"] == "J1"
    assert all(entry is not None for entry, _ in results)  # 重复方拿到首次实例而非报错
    assert sum(1 for _, message in results if "已启动" in message) == 1  # 只有一个真正启动成功
    assert all("J1" in message for _, message in results)


# ---------- 登记：转换步骤、输出目标等字段全部保存 ----------

def test_register_saves_transform_steps_and_output_target():
    response = client.post("/api/etl", json={"values": {
        "任务编号": "ETL-0004",
        "任务名称": "退款明细准备",
        "输入源": "CRM.退款表",
        "转换步骤": "清洗→关联订单",
        "输出目标": "ODS_退款明细",
        "调度周期": "每日 05:00",
        "依赖关系": "ETL-0001",
    }})
    assert response.status_code == 200
    assert response.json()["ok"] is True
    entry = get_entry(4)
    assert entry["转换步骤"] == "清洗→关联订单"
    assert entry["输出目标"] == "ODS_退款明细"
    assert entry["调度周期"] == "每日 05:00"
    assert entry["依赖关系"] == "ETL-0001"
    assert entry["status"] == "草稿" and entry["任务状态"] == "草稿"
    # 新任务依赖 ETL-0001（草稿），启动被依赖闸门挡下
    blocked = act(4, "启动任务")
    assert blocked["ok"] is False and "ETL-0001" in blocked["message"]


def test_register_rejects_duplicate_code():
    response = client.post("/api/etl", json={"values": {
        "任务编号": "ETL-0001", "任务名称": "重复编号", "输入源": "X",
    }})
    assert response.json()["ok"] is False


# ---------- 失败回退：任务状态与刷新水位一起回退 ----------

def test_start_fails_when_output_target_has_no_refresh_policy():
    response = client.post("/api/etl", json={"values": {
        "任务编号": "ETL-0005",
        "任务名称": "无刷新策略任务",
        "输入源": "X",
        "输出目标": "不存在的数据集",
    }})
    entry_id = response.json()["entry"]["id"]
    result = act(entry_id, "启动任务")
    assert result["ok"] is False
    assert "刷新水位" in result["message"]
    entry = get_entry(entry_id)
    assert entry["status"] == "草稿"  # 状态回退（未变更）
    assert entry["当前批次"] is None
    assert get_batches() == []  # 没有留下半成品批次


def test_archive_rolls_back_status_and_watermark_together():
    # 构造一个输出目标没有刷新策略的已暂停任务，归档必须整体失败
    response = client.post("/api/etl", json={"values": {
        "任务编号": "ETL-0006",
        "任务名称": "回退验证任务",
        "输入源": "X",
        "输出目标": "不存在的数据集",
    }})
    entry_id = response.json()["entry"]["id"]
    entry = store.find("etl", entry_id)
    entry["status"] = "已暂停"
    entry["任务状态"] = "已暂停"
    result = act(entry_id, "归档任务")
    assert result["ok"] is False
    entry = get_entry(entry_id)
    assert entry["status"] == "已暂停"  # 上游状态回退
    assert entry["任务状态"] == "已暂停"
    assert get_batches() == []  # 水位与批次均无残留


# ---------- 列表与详情状态一致 ----------

def test_list_and_detail_status_are_aligned():
    act(1, "启动任务")
    response = client.get("/api/etl")
    assert response.status_code == 200
    for row in response.json()["items"]:
        assert row["任务状态"] == row["status"]
    detail = get_entry(1)
    assert detail["任务状态"] == detail["status"] == "运行中"


def test_export_route_is_not_shadowed_by_detail_route():
    response = client.get("/api/etl/export")
    assert response.status_code == 200
    assert response.json()["module"] == "etl"


# ---------- 刷新策略自身的状态机 ----------

def test_refresh_action_rules_block_out_of_order():
    response = client.post("/api/refresh/1/actions", json={"values": {"action": "关闭刷新"}})
    assert response.json()["ok"] is False  # 已创建不能直接关闭
    response = client.post("/api/refresh/1/actions", json={"values": {"action": "启用刷新"}})
    assert response.json()["ok"] is True
    assert response.json()["entry"]["策略状态"] == "生效中"
