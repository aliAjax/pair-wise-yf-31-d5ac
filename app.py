#!/usr/bin/env python3
"""Airline disruption recovery engine using standard-library SQLite and HTTP.

修订链路（revision chain）：
- 中断事件 disruptions.revision 是链路源头；恢复方案记录 disruption_revision 表示基于哪个中断版本。
- 中断信息更新后版本号前进，基于旧版本的锁定方案变为 stale（过期失效），相关未执行航班转 pending_review。
- 方案 lock 只做校验与资源占位，process 才把调整写入航班；写入时保存航班原值快照并登记写入者，
  失败或恢复时只撤掉本方案写入、且之后没有被别人改写的航班值，重试幂等、不重复占用资源。
- 航班自身 revision 用于取消/恢复/执行/复核的乐观并发，后提交方按最新状态收到 revision_conflict。
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, time, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

PORT = 8202
ROLES = {"viewer", "scheduler", "ops_manager", "auditor"}

FLIGHT_SCHEDULED = "scheduled"
FLIGHT_PENDING_REVIEW = "pending_review"
FLIGHT_EXECUTED = "executed"
FLIGHT_CANCELED = "canceled"


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime:
    if not value:
        raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ApiError(400, "invalid_clock", f"时刻格式应为 HH:MM: {value}") from exc


def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


def revision_conflict(entity: str, expected: int, current: dict[str, Any]) -> ApiError:
    """后到的提交方按最新状态收到冲突。"""
    return ApiError(409, "revision_conflict", f"{entity}版本已变化，请基于最新状态重试",
                    {"expected_revision": expected, "current_revision": current["revision"],
                     "current_status": current.get("status"), "current": current})


class Repository:
    def __init__(self, db_path: str | Path):
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        # 多线程共享同一连接，写事务串行化，保证两人同时提交时后到者看到最新版本。
        self._tx_lock = threading.Lock()
        self._init()

    @contextmanager
    def tx(self):
        with self._tx_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _init(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0, UNIQUE(origin,destination,valid_from,valid_to));
            CREATE TABLE IF NOT EXISTS flights(
                id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL,
                std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                passenger_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0,
                revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, writer_assignment_id INTEGER, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS disruptions(
                id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL,
                starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
                revision INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, updated_by TEXT, updated_at TEXT
            );
            CREATE TABLE IF NOT EXISTS recovery_plans(
                id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL REFERENCES disruptions(id), name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft', revision INTEGER NOT NULL DEFAULT 1,
                disruption_revision INTEGER NOT NULL DEFAULT 1, apply_status TEXT NOT NULL DEFAULT 'pending',
                score_json TEXT, metrics_json TEXT, result_json TEXT, staled_at TEXT,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT
            );
            CREATE TABLE IF NOT EXISTS assignments(
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id) ON DELETE CASCADE,
                flight_id INTEGER NOT NULL REFERENCES flights(id), aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0,
                missed_connections INTEGER NOT NULL DEFAULT 0,
                applied INTEGER NOT NULL DEFAULT 0, applied_at TEXT, reverted_at TEXT,
                prev_std TEXT, prev_sta TEXT, prev_aircraft_id TEXT, prev_crew_id TEXT,
                prev_status TEXT, prev_delay_minutes INTEGER, applied_flight_revision INTEGER,
                UNIQUE(plan_id,flight_id)
            );
            CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
            """
        )
        self._migrate()

    def _migrate(self) -> None:
        """为旧库补列（原型环境 SQLite 单库）。"""
        additions = {
            "flights": [("writer_assignment_id", "INTEGER")],
            "disruptions": [("revision", "INTEGER NOT NULL DEFAULT 1"), ("updated_by", "TEXT"), ("updated_at", "TEXT")],
            "recovery_plans": [("disruption_revision", "INTEGER NOT NULL DEFAULT 1"),
                               ("apply_status", "TEXT NOT NULL DEFAULT 'pending'"),
                               ("result_json", "TEXT"), ("staled_at", "TEXT")],
            "assignments": [("applied", "INTEGER NOT NULL DEFAULT 0"), ("applied_at", "TEXT"), ("reverted_at", "TEXT"),
                            ("prev_std", "TEXT"), ("prev_sta", "TEXT"), ("prev_aircraft_id", "TEXT"),
                            ("prev_crew_id", "TEXT"), ("prev_status", "TEXT"),
                            ("prev_delay_minutes", "INTEGER"), ("applied_flight_revision", "INTEGER")],
        }
        for table, columns in additions.items():
            existing = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
            for name, decl in columns:
                if name not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
        # 旧数据补齐 updated_at。
        self.conn.execute("UPDATE disruptions SET updated_at=created_at WHERE updated_at IS NULL")

    @staticmethod
    def audit(conn: sqlite3.Connection, plan_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?)",
                     (plan_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class AirlineRecoveryService:
    def __init__(self, db_path: str | Path):
        self.repo = Repository(db_path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str]:
        actor, role = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip()
        if not actor or role not in ROLES:
            raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        return actor, role

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    @staticmethod
    def _expected_revision(body: dict[str, Any], label: str) -> int:
        expected = body.get("expected_revision")
        if not isinstance(expected, int):
            raise ApiError(400, "revision_required", f"{label} 的 expected_revision 必须是整数")
        return expected

    def seed_airport(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机场数据")
        code = str(body.get("code", "")).upper().strip()
        country = str(body.get("country", "")).upper().strip()
        if not code or not country:
            raise ApiError(400, "missing_fields", "code 和 country 必填")
        start = str(body.get("curfew_start", "23:00")); end = str(body.get("curfew_end", "06:00"))
        parse_clock(start); parse_clock(end)
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO airports(code,country,curfew_start,curfew_end) VALUES(?,?,?,?)", (code, country, start, end))
            return dict(conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone())

    def seed_aircraft(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护飞机")
        ident, model = str(body.get("id", "")).strip(), str(body.get("model", "")).strip()
        if not ident or not model: raise ApiError(400, "missing_fields", "id 和 model 必填")
        due = iso(parse_time(body.get("maintenance_due")))
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO aircraft(id,model,maintenance_due,status) VALUES(?,?,?,?)", (ident, model, due, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM aircraft WHERE id=?", (ident,)).fetchone())

    def seed_crew(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机组")
        ident, name, base = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("base", "")).upper().strip()
        duty = parse_time(body.get("duty_start")); maximum = body.get("max_duty_minutes")
        if not ident or not name or not base or not isinstance(maximum, int) or maximum <= 0:
            raise ApiError(400, "invalid_crew", "id、name、base 和正整数 max_duty_minutes 必填")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO crew(id,name,base,duty_start,max_duty_minutes,status) VALUES(?,?,?,?,?,?)", (ident, name, base, iso(duty), maximum, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM crew WHERE id=?", (ident,)).fetchone())

    def create_permit(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "permit_forbidden", "只有运行经理可以维护航线许可")
        origin, destination = str(body.get("origin", "")).upper(), str(body.get("destination", "")).upper()
        if not origin or not destination: raise ApiError(400, "missing_fields", "origin 和 destination 必填")
        valid_from, valid_to = parse_time(body.get("valid_from")), parse_time(body.get("valid_to"))
        if valid_to <= valid_from: raise ApiError(400, "invalid_permit", "许可结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            try:
                cur = conn.execute("INSERT INTO permits(origin,destination,valid_from,valid_to,curfew_exempt) VALUES(?,?,?,?,?)",
                                   (origin, destination, iso(valid_from), iso(valid_to), int(bool(body.get("curfew_exempt")))))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "permit_exists", "相同航线与有效期的许可已存在") from exc
            return dict(conn.execute("SELECT * FROM permits WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_flight(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "flight_forbidden", "当前角色不能创建航班")
        required = ("flight_no", "origin", "destination", "std", "sta", "aircraft_id", "crew_id")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        std, sta = parse_time(body["std"]), parse_time(body["sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
        passengers = body.get("passenger_count", 0)
        if not isinstance(passengers, int) or passengers < 0: raise ApiError(400, "invalid_passengers", "passenger_count 必须是非负整数")
        with self.repo.tx() as conn:
            for table, ident in (("aircraft", body["aircraft_id"]), ("crew", body["crew_id"])):
                row = conn.execute(f"SELECT status FROM {table} WHERE id=?", (ident,)).fetchone()
                if not row or row["status"] != "active": raise ApiError(409, "resource_unavailable", f"{table} {ident} 不可用")
            try:
                cur = conn.execute("""INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,passenger_count,updated_at)
                                      VALUES(?,?,?,?,?,?,?,?,?)""",
                                   (body["flight_no"].upper(), body["origin"].upper(), body["destination"].upper(), iso(std), iso(sta),
                                    body["aircraft_id"], body["crew_id"], passengers, iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "flight_exists", "航班号已存在") from exc
            return dict(conn.execute("SELECT * FROM flights WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_disruption(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "disruption_forbidden", "当前角色不能登记中断")
        kind, resource = str(body.get("kind", "")).strip(), str(body.get("resource_id", "")).strip()
        if kind not in {"airport_closure", "aircraft_fault", "crew_timeout"} or not resource:
            raise ApiError(400, "invalid_disruption", "kind 或 resource_id 无效")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_times", "中断结束时间必须晚于开始时间")
        now = iso()
        with self.repo.tx() as conn:
            cur = conn.execute("""INSERT INTO disruptions(kind,resource_id,starts_at,ends_at,created_at,updated_by,updated_at)
                                  VALUES(?,?,?,?,?,?,?)""",
                               (kind, resource.upper() if kind == "airport_closure" else resource,
                                iso(start), iso(end), now, actor, now))
            return dict(conn.execute("SELECT * FROM disruptions WHERE id=?", (cur.lastrowid,)).fetchone())

    def update_disruption(self, disruption_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """更新中断窗口/信息：修订链路前进，基于旧版本的锁定方案过期，未执行航班转待复核。"""
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "disruption_forbidden", "当前角色不能修改中断")
        expected = self._expected_revision(body, "中断事件")
        start = parse_time(body["starts_at"]) if body.get("starts_at") else None
        end = parse_time(body["ends_at"]) if body.get("ends_at") else None
        if start and end and end <= start: raise ApiError(400, "invalid_times", "中断结束时间必须晚于开始时间")
        kind = str(body.get("kind", "")).strip()
        if kind and kind not in {"airport_closure", "aircraft_fault", "crew_timeout"}:
            raise ApiError(400, "invalid_disruption", "kind 无效")
        resource = str(body.get("resource_id", "")).strip()
        with self.repo.tx() as conn:
            row = conn.execute("SELECT * FROM disruptions WHERE id=?", (disruption_id,)).fetchone()
            if not row: raise ApiError(404, "disruption_not_found", "中断事件不存在")
            current = dict(row)
            if current["revision"] != expected:
                raise revision_conflict("中断事件", expected, current)
            new_kind = kind or current["kind"]
            new_resource = (resource.upper() if new_kind == "airport_closure" else resource) if resource else current["resource_id"]
            new_start, new_end = iso(start) if start else current["starts_at"], iso(end) if end else current["ends_at"]
            if (new_kind, new_resource, new_start, new_end) == (current["kind"], current["resource_id"], current["starts_at"], current["ends_at"]):
                raise ApiError(400, "no_changes", "中断信息没有变化")
            now = iso()
            conn.execute("""UPDATE disruptions SET kind=?,resource_id=?,starts_at=?,ends_at=?,revision=revision+1,updated_by=?,updated_at=?
                            WHERE id=?""", (new_kind, new_resource, new_start, new_end, actor, now, disruption_id))
            new_revision = current["revision"] + 1
            staled_plan_ids, review_flight_ids = self._stale_locked_plans(conn, disruption_id)
            Repository.audit(conn, None, actor, role, "disruption_updated",
                             {"disruption_id": disruption_id, "old_revision": expected, "new_revision": new_revision,
                              "staled_plan_ids": staled_plan_ids, "pending_review_flight_ids": review_flight_ids})
            return {"disruption": dict(conn.execute("SELECT * FROM disruptions WHERE id=?", (disruption_id,)).fetchone()),
                    "staled_plan_ids": staled_plan_ids, "pending_review_flight_ids": review_flight_ids}

    def _stale_locked_plans(self, conn: sqlite3.Connection, disruption_id: int) -> tuple[list[int], list[int]]:
        """把依赖旧版本的锁定方案置为过期，其未执行航班转待复核。"""
        staled_plan_ids, review_flight_ids = [], []
        plans = conn.execute("SELECT id FROM recovery_plans WHERE disruption_id=? AND status='locked'", (disruption_id,)).fetchall()
        for plan_row in plans:
            plan_id = plan_row["id"]
            conn.execute("UPDATE recovery_plans SET status='stale', staled_at=? WHERE id=?", (iso(), plan_id))
            staled_plan_ids.append(plan_id)
            for a in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled'", (plan_id,)).fetchall():
                flight = conn.execute("SELECT * FROM flights WHERE id=?", (a["flight_id"],)).fetchone()
                if not flight or flight["status"] in (FLIGHT_EXECUTED, FLIGHT_CANCELED, FLIGHT_PENDING_REVIEW):
                    continue
                # 已被其他生效方案接管的航班不再属于本过期方案。
                writer_id = flight["writer_assignment_id"]
                if writer_id:
                    owner = conn.execute("""SELECT p.status FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                                            WHERE a.id=?""", (writer_id,)).fetchone()
                    if owner and writer_id != a["id"] and owner["status"] == "locked":
                        continue
                conn.execute("UPDATE flights SET status=?,revision=revision+1,updated_at=? WHERE id=?",
                             (FLIGHT_PENDING_REVIEW, iso(), flight["id"]))
                review_flight_ids.append(flight["id"])
        return staled_plan_ids, review_flight_ids

    def create_plan(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "plan_forbidden", "当前角色不能创建恢复方案")
        disruption_id, name = body.get("disruption_id"), str(body.get("name", "")).strip()
        assignments = body.get("assignments", [])
        if not isinstance(disruption_id, int) or not name or not isinstance(assignments, list):
            raise ApiError(400, "invalid_plan", "disruption_id、name 和 assignments 必填")
        with self.repo.tx() as conn:
            disruption = conn.execute("SELECT * FROM disruptions WHERE id=?", (disruption_id,)).fetchone()
            if not disruption: raise ApiError(404, "disruption_not_found", "中断事件不存在")
            cur = conn.execute("""INSERT INTO recovery_plans(disruption_id,name,disruption_revision,created_by,created_at)
                                  VALUES(?,?,?,?,?)""",
                               (disruption_id, name, disruption["revision"], actor, iso()))
            plan_id = cur.lastrowid
            for item in assignments:
                self._insert_assignment(conn, plan_id, item, replace=False)
            Repository.audit(conn, plan_id, actor, role, "plan_created",
                             {"disruption_id": disruption_id, "disruption_revision": disruption["revision"],
                              "assignment_count": len(assignments)})
            return self.get_plan(plan_id)

    def _insert_assignment(self, conn: sqlite3.Connection, plan_id: int, item: dict[str, Any], replace: bool) -> None:
        required = ("flight_id", "aircraft_id", "crew_id", "new_std", "new_sta")
        if any(item.get(k) in (None, "") for k in required): raise ApiError(400, "invalid_assignment", f"飞行调整缺少字段: {', '.join(k for k in required if item.get(k) in (None, ''))}")
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
        std, sta = parse_time(item["new_std"]), parse_time(item["new_sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "新到达时间必须晚于新起飞时间")
        flight = conn.execute("SELECT * FROM flights WHERE id=?", (item["flight_id"],)).fetchone()
        if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
        if flight["status"] == "canceled" and item.get("status", "planned") != "canceled":
            raise ApiError(409, "canceled_flight", "已取消航班不能安排执行")
        delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
        missed = int(item.get("missed_connections", 0))
        if missed < 0: raise ApiError(400, "invalid_connections", "missed_connections 不能为负")
        try:
            if replace:
                conn.execute("""UPDATE assignments SET aircraft_id=?,crew_id=?,new_std=?,new_sta=?,status=?,delay_minutes=?,missed_connections=?
                                WHERE plan_id=? AND flight_id=?""",
                             (item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed, plan_id, item["flight_id"]))
                if conn.execute("SELECT changes()").fetchone()[0] == 0:
                    raise KeyError
            else:
                conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                             (plan_id, item["flight_id"], item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, missed))
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "assignment_conflict", "方案中该航班已存在或资源无效") from exc
        except KeyError as exc:
            raise ApiError(404, "assignment_not_found", "待替换的航班调整不存在") from exc

    def add_assignment(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "assignment_forbidden", "当前角色不能修改方案")
        expected = self._expected_revision(body, "方案")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
            if plan["revision"] != expected: raise revision_conflict("方案", expected, self.get_plan(plan_id))
            self._insert_assignment(conn, plan_id, body, replace=True)
            conn.execute("UPDATE recovery_plans SET revision=revision+1 WHERE id=?", (plan_id,))
            Repository.audit(conn, plan_id, actor, role, "assignment_reassigned", {"flight_id": body.get("flight_id")})
            return self.get_plan(plan_id)

    def rebase_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """草案重新基于最新中断修订（旧修订上不能锁定）。"""
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "plan_forbidden", "当前角色不能维护方案")
        expected = self._expected_revision(body, "方案")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "draft": raise ApiError(409, "plan_not_draft", "只有草案可以重新对齐中断版本")
            if plan["revision"] != expected: raise revision_conflict("方案", expected, self.get_plan(plan_id))
            disruption = conn.execute("SELECT * FROM disruptions WHERE id=?", (plan["disruption_id"],)).fetchone()
            old_based = plan["disruption_revision"]
            conn.execute("UPDATE recovery_plans SET disruption_revision=?,revision=revision+1 WHERE id=?",
                         (disruption["revision"], plan_id))
            Repository.audit(conn, plan_id, actor, role, "plan_rebased",
                             {"from_revision": old_based, "to_revision": disruption["revision"]})
            return self.get_plan(plan_id)

    def _validate_plan(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        rows = [dict(r) for r in conn.execute("""SELECT a.*, f.flight_no, f.origin, f.destination, f.passenger_count, f.status flight_status
                                                  FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        if not rows: raise ApiError(409, "empty_plan", "方案没有飞行调整")
        problems: list[dict[str, Any]] = []
        by_aircraft: dict[str, list[dict[str, Any]]] = {}
        by_crew: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if row["status"] == "canceled": continue
            std, sta = parse_time(row["new_std"]), parse_time(row["new_sta"])
            aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (row["aircraft_id"],)).fetchone()
            crew = conn.execute("SELECT * FROM crew WHERE id=?", (row["crew_id"],)).fetchone()
            origin = conn.execute("SELECT * FROM airports WHERE code=?", (row["origin"],)).fetchone()
            destination = conn.execute("SELECT * FROM airports WHERE code=?", (row["destination"],)).fetchone()
            if not aircraft or aircraft["status"] != "active": problems.append({"assignment_id": row["id"], "code": "aircraft_unavailable"})
            elif parse_time(aircraft["maintenance_due"]) < sta: problems.append({"assignment_id": row["id"], "code": "maintenance_due", "resource": aircraft["id"]})
            if not crew or crew["status"] != "active": problems.append({"assignment_id": row["id"], "code": "crew_unavailable"})
            if not origin or not destination: problems.append({"assignment_id": row["id"], "code": "airport_unknown"})
            if crew:
                duty_start, max_duty = parse_time(crew["duty_start"]), crew["max_duty_minutes"]
                if (sta - duty_start).total_seconds() / 60 > max_duty: problems.append({"assignment_id": row["id"], "code": "duty_limit", "resource": crew["id"]})
            if destination:
                curfew_start, curfew_end = parse_clock(destination["curfew_start"]), parse_clock(destination["curfew_end"])
                permit = conn.execute("""SELECT * FROM permits WHERE origin=? AND destination=? AND valid_from<=? AND valid_to>=?""",
                                      (row["origin"], row["destination"], row["new_sta"], row["new_sta"])).fetchone()
                arrival_clock = sta.timetz().replace(tzinfo=None)
                inside = arrival_clock >= curfew_start or arrival_clock < curfew_end if curfew_start > curfew_end else curfew_start <= arrival_clock < curfew_end
                if inside and not (permit and permit["curfew_exempt"]): problems.append({"assignment_id": row["id"], "code": "airport_curfew"})
                if row["origin"] != row["destination"] and not permit: problems.append({"assignment_id": row["id"], "code": "route_permit_missing"})
                elif row["origin"] != row["destination"] and not (parse_time(permit["valid_from"]) <= std <= parse_time(permit["valid_to"])):
                    problems.append({"assignment_id": row["id"], "code": "route_permit_window"})
            by_aircraft.setdefault(row["aircraft_id"], []).append(row)
            by_crew.setdefault(row["crew_id"], []).append(row)
        for bucket_name, buckets in (("aircraft", by_aircraft), ("crew", by_crew)):
            for resource, items in buckets.items():
                for i, left in enumerate(items):
                    for right in items[i + 1:]:
                        if overlaps(parse_time(left["new_std"]), parse_time(left["new_sta"]), parse_time(right["new_std"]), parse_time(right["new_sta"])):
                            problems.append({"code": f"{bucket_name}_overlap", "resource": resource, "assignments": [left["id"], right["id"]]})
        return problems

    def validate_plan(self, plan_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager", "auditor"}: raise ApiError(403, "validate_forbidden", "当前角色不能校验方案")
        with self.repo.tx() as conn:
            problems = self._validate_plan(conn, plan_id)
            if not problems:
                metrics = self._metrics(conn, plan_id)
                conn.execute("UPDATE recovery_plans SET metrics_json=?,score_json=? WHERE id=?", (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), plan_id))
            return {"valid": not problems, "problems": problems, "plan": self.get_plan(plan_id)}

    def _metrics(self, conn: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
        rows = conn.execute("""SELECT a.*,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=?""", (plan_id,)).fetchall()
        canceled = sum(1 for row in rows if row["status"] == "canceled")
        return {"flight_count": len(rows), "canceled": canceled, "total_delay_minutes": sum(max(0, row["delay_minutes"]) for row in rows),
                "affected_passengers": sum(row["passenger_count"] for row in rows), "missed_connections": sum(row["missed_connections"] for row in rows)}

    @staticmethod
    def _score(metrics: dict[str, Any]) -> dict[str, int]:
        score = metrics["canceled"] * 100000 + metrics["missed_connections"] * 5000 + metrics["total_delay_minutes"] * 100 + metrics["affected_passengers"]
        return {"cost_score": score, "lower_is_better": 1}

    def _locked_resource_conflicts(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        """与其他锁定且未过期方案的资源占位冲突；过期方案不再占用资源，本方案自身排除。"""
        conflicts = []
        for row in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled'", (plan_id,)).fetchall():
            conflicting = conn.execute("""SELECT a.*,p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                WHERE p.id!=? AND p.status='locked' AND a.status!='canceled' AND (a.aircraft_id=? OR a.crew_id=?)
                AND a.new_std<? AND a.new_sta>?""",
                (plan_id, row["aircraft_id"], row["crew_id"], row["new_sta"], row["new_std"])).fetchall()
            conflicts.extend({"assignment_id": row["id"], "conflict_plan_id": item["plan_id"], "conflict_plan": item["plan_name"],
                              "resource": item["aircraft_id"] if item["aircraft_id"] == row["aircraft_id"] else item["crew_id"]} for item in conflicting)
        return conflicts

    def lock_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "lock_forbidden", "只有运行经理可以锁定恢复方案")
        expected = self._expected_revision(body, "方案")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "locked":
                # 同版本重复提交视为客户端幂等重发；版本已前进则后到一方按最新状态报冲突。
                if plan["revision"] == expected: return self.get_plan(plan_id)
                raise revision_conflict("方案", expected, self.get_plan(plan_id))
            if plan["status"] == "stale": raise ApiError(409, "plan_stale", "方案基于过期中断修订，不能锁定")
            if plan["revision"] != expected: raise revision_conflict("方案", expected, self.get_plan(plan_id))
            disruption = conn.execute("SELECT * FROM disruptions WHERE id=?", (plan["disruption_id"],)).fetchone()
            if plan["disruption_revision"] != disruption["revision"]:
                raise ApiError(409, "plan_outdated", "方案基于旧版中断事件，请先 rebase 到最新修订",
                               {"plan_revision": plan["disruption_revision"], "current_revision": disruption["revision"]})
            problems = self._validate_plan(conn, plan_id)
            if problems: raise ApiError(409, "plan_invalid", "方案未通过约束校验", problems)
            conflicts = self._locked_resource_conflicts(conn, plan_id)
            if conflicts: raise ApiError(409, "locked_resource_conflict", "与已锁定方案存在飞机或机组冲突", conflicts)
            metrics = self._metrics(conn, plan_id)
            # 锁定只做资源占位并前进修订（并发锁定时后到一方据此收到冲突）；航班值在 process 时按快照写入。
            conn.execute("""UPDATE recovery_plans SET status='locked',revision=revision+1,metrics_json=?,score_json=?,locked_at=?,locked_by=? WHERE id=?""",
                         (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), iso(), actor, plan_id))
            Repository.audit(conn, plan_id, actor, role, "plan_locked", {"metrics": metrics})
            return self.get_plan(plan_id)

    def _apply_assignments(self, conn: sqlite3.Connection, plan_id: int) -> list[int]:
        """把尚未落值的调整写入航班；快照原值并登记写入者。已 applied 的跳过，保证重试不重复改。"""
        applied = []
        now = iso()
        rows = conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled' AND applied=0", (plan_id,)).fetchall()
        for row in rows:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (row["flight_id"],)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["status"] == FLIGHT_CANCELED:
                raise ApiError(409, "flight_canceled", f"航班 {flight['flight_no']} 已取消，不能按方案执行", {"flight_id": flight["id"]})
            if flight["status"] == FLIGHT_EXECUTED:
                raise ApiError(409, "flight_executed", f"航班 {flight['flight_no']} 已执行，不能再调整", {"flight_id": flight["id"]})
            if flight["status"] == FLIGHT_PENDING_REVIEW:
                raise ApiError(409, "review_required",
                               f"航班 {flight['flight_no']} 因方案过期待复核，必须先复核才能纳入新方案", {"flight_id": flight["id"]})
            conn.execute("""UPDATE assignments SET prev_std=?,prev_sta=?,prev_aircraft_id=?,prev_crew_id=?,prev_status=?,
                            prev_delay_minutes=?,applied_flight_revision=? WHERE id=?""",
                         (flight["std"], flight["sta"], flight["aircraft_id"], flight["crew_id"], flight["status"],
                          flight["delay_minutes"], flight["revision"], row["id"]))
            conn.execute("""UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,delay_minutes=?,revision=revision+1,
                            writer_assignment_id=?,updated_at=? WHERE id=?""",
                         (row["new_std"], row["new_sta"], row["aircraft_id"], row["crew_id"], max(0, row["delay_minutes"]),
                          row["id"], now, flight["id"]))
            conn.execute("UPDATE assignments SET applied=1,applied_at=?,status='active' WHERE id=?", (now, row["id"]))
            applied.append(flight["id"])
        return applied

    def _revert_plan_writes(self, conn: sqlite3.Connection, plan_id: int) -> tuple[list[int], list[dict[str, Any]]]:
        """恢复原方案：只撤掉本方案写入、且之后未被执行或被他人改写的航班值。"""
        restored, skipped = [], []
        now = iso()
        for row in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND applied=1 AND status!='canceled'", (plan_id,)).fetchall():
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (row["flight_id"],)).fetchone()
            if not flight:
                continue
            if flight["status"] == FLIGHT_EXECUTED:
                skipped.append({"flight_id": flight["id"], "reason": "executed"})
                continue
            if flight["status"] == FLIGHT_CANCELED:
                # 人工取消是方案落值之后的他人动作，不能由恢复动作复活。
                skipped.append({"flight_id": flight["id"], "reason": "canceled"})
                continue
            if flight["writer_assignment_id"] != row["id"]:
                skipped.append({"flight_id": flight["id"], "reason": "superseded"})
                continue
            target_status = FLIGHT_SCHEDULED if flight["status"] == FLIGHT_PENDING_REVIEW else (row["prev_status"] or FLIGHT_SCHEDULED)
            conn.execute("""UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,status=?,delay_minutes=?,revision=?,
                            writer_assignment_id=NULL,updated_at=? WHERE id=?""",
                         (row["prev_std"], row["prev_sta"], row["prev_aircraft_id"], row["prev_crew_id"], target_status,
                          row["prev_delay_minutes"] or 0, row["applied_flight_revision"], now, flight["id"]))
            conn.execute("UPDATE assignments SET applied=0,status='planned',reverted_at=? WHERE id=?", (now, row["id"]))
            restored.append(flight["id"])
        return restored, skipped

    @staticmethod
    def _last_result(plan: sqlite3.Row) -> dict[str, Any]:
        return json.loads(plan["result_json"]) if plan["result_json"] else {"attempts": 0}

    def process_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """处理（落值执行）锁定方案。失败自动恢复原方案；重试不重复改航班、不重复占用资源。"""
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "process_forbidden", "当前角色不能处理方案")
        expected = self._expected_revision(body, "方案")
        force_fail = bool(body.get("force_fail"))
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "stale": raise ApiError(409, "plan_stale", "方案已过期，请基于最新中断修订重做")
            if plan["status"] != "locked": raise ApiError(409, "plan_not_locked", "方案尚未锁定")
            if plan["revision"] != expected: raise revision_conflict("方案", expected, self.get_plan(plan_id))
            previous = self._last_result(plan)
            # 已成功处理：同版本重发为客户端幂等重试（直接返回，不重复改航班）；版本已前进则后到一方报冲突。
            if plan["apply_status"] == "succeeded" and not force_fail:
                if plan["revision"] != expected:
                    raise revision_conflict("方案", expected, self.get_plan(plan_id))
                result = dict(previous); result["idempotent"] = True
                conn.execute("UPDATE recovery_plans SET result_json=? WHERE id=?", (json.dumps(result, ensure_ascii=False), plan_id))
                payload = self.get_plan(plan_id)
                payload["idempotent"] = True
                return payload
            conflicts = self._locked_resource_conflicts(conn, plan_id)
            if conflicts: raise ApiError(409, "locked_resource_conflict", "处理时发现资源被其他锁定方案占用", conflicts)
            conn.execute("UPDATE recovery_plans SET apply_status='processing' WHERE id=?", (plan_id,))
            attempts = int(previous.get("attempts", 0)) + 1
            applied = self._apply_assignments(conn, plan_id)
            if force_fail:
                restored, skipped = self._revert_plan_writes(conn, plan_id)
                result = {"status": "failed", "attempts": attempts, "applied_then_restored": applied,
                          "restored": restored, "skipped": skipped,
                          "error": "模拟下游处理失败，已恢复原方案，可重试", "at": iso()}
                conn.execute("UPDATE recovery_plans SET apply_status='failed',result_json=? WHERE id=?",
                             (json.dumps(result, ensure_ascii=False), plan_id))
                Repository.audit(conn, plan_id, actor, role, "plan_process_failed", {"attempts": attempts, "restored": restored})
                payload = self.get_plan(plan_id)
                payload["processing_failed"] = True
                return payload
            result = {"status": "succeeded", "attempts": attempts, "applied": applied, "at": iso()}
            conn.execute("UPDATE recovery_plans SET apply_status='succeeded',revision=revision+1,result_json=? WHERE id=?",
                         (json.dumps(result, ensure_ascii=False), plan_id))
            Repository.audit(conn, plan_id, actor, role, "plan_processed", {"attempts": attempts, "applied": applied})
            return self.get_plan(plan_id)

    def restore_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """手动撤掉方案写入的航班值（过期方案或锁定方案均可），只动本方案写入的航班。"""
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "restore_forbidden", "当前角色不能恢复方案")
        expected = self._expected_revision(body, "方案")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "draft": raise ApiError(409, "plan_not_locked", "草案尚未写入航班，无需恢复")
            if plan["revision"] != expected: raise revision_conflict("方案", expected, self.get_plan(plan_id))
            restored, skipped = self._revert_plan_writes(conn, plan_id)
            next_apply = "reverted" if plan["status"] == "stale" else "pending"
            result = {"status": next_apply, "restored": restored, "skipped": skipped, "at": iso()}
            conn.execute("UPDATE recovery_plans SET apply_status=?,revision=revision+1,result_json=? WHERE id=?",
                         (next_apply, json.dumps(result, ensure_ascii=False), plan_id))
            Repository.audit(conn, plan_id, actor, role, "plan_restored", {"restored": restored, "skipped": skipped})
            payload = self.get_plan(plan_id)
            payload["restore_result"] = result
            return payload

    def cancel_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消航班")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["status"] == "canceled": return {"flight": dict(flight), "idempotent": True}
            conn.execute("UPDATE flights SET status='canceled',cancel_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_canceled", {"flight_id": flight_id, "reason": reason})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()), "idempotent": False}

    def recover_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "recover_forbidden", "当前角色不能恢复航班")
        expected = self._expected_revision(body, "航班")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["revision"] != expected: raise revision_conflict("航班", expected, dict(flight))
            if flight["status"] != "canceled": raise ApiError(409, "not_canceled", "只有取消航班可以恢复")
            std, sta = parse_time(body.get("new_std")), parse_time(body.get("new_sta"))
            if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
            aircraft_id, crew_id = body.get("aircraft_id", flight["aircraft_id"]), body.get("crew_id", flight["crew_id"])
            conn.execute("""UPDATE flights SET status='scheduled',std=?,sta=?,aircraft_id=?,crew_id=?,cancel_reason=NULL,
                            delay_minutes=0,revision=revision+1,writer_assignment_id=NULL,updated_at=? WHERE id=?""",
                         (iso(std), iso(sta), aircraft_id, crew_id, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_recovered", {"flight_id": flight_id})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone())}

    def execute_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """标记航班已执行（已飞出），过期方案不能再撤回它的值。"""
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "execute_forbidden", "当前角色不能标记执行")
        expected = self._expected_revision(body, "航班")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["revision"] != expected: raise revision_conflict("航班", expected, dict(flight))
            if flight["status"] == "executed": return {"flight": dict(flight), "idempotent": True}
            if flight["status"] == "canceled": raise ApiError(409, "flight_canceled", "已取消航班不能执行")
            if flight["status"] == "pending_review": raise ApiError(409, "review_required", "待复核航班必须先复核才能执行")
            conn.execute("UPDATE flights SET status='executed',revision=revision+1,updated_at=? WHERE id=?", (iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_executed", {"flight_id": flight_id})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone())}

    def review_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        """调度台复核待复核航班：keep 接受现值，restore 按过期方案快照恢复原值。"""
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "review_forbidden", "当前角色不能复核航班")
        expected = self._expected_revision(body, "航班")
        decision = str(body.get("decision", "keep")).strip()
        if decision not in {"keep", "restore"}: raise ApiError(400, "invalid_decision", "decision 只能是 keep 或 restore")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["revision"] != expected: raise revision_conflict("航班", expected, dict(flight))
            if flight["status"] != "pending_review": raise ApiError(409, "not_pending_review", "只有待复核航班需要复核")
            restored_from = None
            if decision == "restore":
                assignment = conn.execute("SELECT * FROM assignments WHERE id=?", (flight["writer_assignment_id"],)).fetchone() if flight["writer_assignment_id"] else None
                if not assignment or not assignment["applied"]:
                    raise ApiError(409, "nothing_to_restore", "该航班没有可回退的方案写入值，可改用 keep 接受现值")
                conn.execute("""UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,delay_minutes=?,status='scheduled',
                                revision=revision+1,writer_assignment_id=NULL,updated_at=? WHERE id=?""",
                             (assignment["prev_std"], assignment["prev_sta"], assignment["prev_aircraft_id"], assignment["prev_crew_id"],
                              assignment["prev_delay_minutes"] or 0, iso(), flight_id))
                conn.execute("UPDATE assignments SET applied=0,status='planned',reverted_at=? WHERE id=?", (iso(), assignment["id"]))
                restored_from = assignment["plan_id"]
            else:
                # 调度确认现值即为现行计划，解除方案写入者绑定，旧方案之后不能再撤值。
                conn.execute("UPDATE flights SET status='scheduled',revision=revision+1,writer_assignment_id=NULL,updated_at=? WHERE id=?",
                             (iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_reviewed",
                             {"flight_id": flight_id, "decision": decision, "restored_from_plan": restored_from})
            return {"flight": dict(conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()),
                    "decision": decision, "restored_from_plan": restored_from}

    def get_plan(self, plan_id: int) -> dict[str, Any]:
        conn = self.repo.conn
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        assignments = [dict(r) for r in conn.execute("""SELECT a.*,f.flight_no,f.origin,f.destination,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        disruption = conn.execute("SELECT revision AS d_revision,status AS d_status FROM disruptions WHERE id=?", (plan["disruption_id"],)).fetchone()
        result = dict(plan)
        result["metrics"] = json.loads(plan["metrics_json"]) if plan["metrics_json"] else self._metrics(conn, plan_id)
        result["score"] = json.loads(plan["score_json"]) if plan["score_json"] else None
        result["result"] = json.loads(plan["result_json"]) if plan["result_json"] else None
        result["stale"] = plan["status"] == "stale"
        result["disruption_current_revision"] = disruption["d_revision"] if disruption else None
        result["assignments"] = assignments
        return result

    def compare_plans(self, disruption_id: int) -> dict[str, Any]:
        plans = []
        for row in self.repo.conn.execute("SELECT id FROM recovery_plans WHERE disruption_id=? ORDER BY id", (disruption_id,)):
            plan = self.get_plan(row["id"])
            if not plan["score"]:
                problems = self._validate_plan(self.repo.conn, row["id"])
                plan["valid"] = not problems
            else:
                plan["valid"] = True
            plans.append(plan)
        plans.sort(key=lambda item: item["score"]["cost_score"] if item["score"] else 10**18)
        return {"disruption_id": disruption_id, "recommended_plan_id": plans[0]["id"] if plans else None, "plans": plans}

    def state(self) -> dict[str, Any]:
        conn = self.repo.conn
        flights = [dict(r) for r in conn.execute("SELECT * FROM flights ORDER BY std")]
        plans = [self.get_plan(r["id"]) for r in conn.execute("SELECT id FROM recovery_plans ORDER BY id DESC LIMIT 20")]
        return {"flights": flights, "disruptions": [dict(r) for r in conn.execute("SELECT * FROM disruptions ORDER BY id DESC")], "plans": plans, "server_time": iso()}


def respond(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode()
    handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: AirlineRecoveryService
    web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length: return {}
        try: value = json.loads(self.rfile.read(length))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "airline-recovery"}
        actor, role = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state()
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "plans"] and parts[2].isdigit(): return 200, self.service.get_plan(int(parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit() and parts[3] == "compare": return 200, self.service.compare_plans(int(parts[2]))
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        table = {
            "/api/airports": lambda: (201, self.service.seed_airport(actor, role, body)),
            "/api/aircraft": lambda: (201, self.service.seed_aircraft(actor, role, body)),
            "/api/crew": lambda: (201, self.service.seed_crew(actor, role, body)),
            "/api/permits": lambda: (201, self.service.create_permit(actor, role, body)),
            "/api/flights": lambda: (201, self.service.create_flight(actor, role, body)),
            "/api/disruptions": lambda: (201, self.service.create_disruption(actor, role, body)),
            "/api/recovery-plans": lambda: (201, self.service.create_plan(actor, role, body)),
        }
        if path in table: return table[path]()
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit():
            disruption_id, action = int(parts[2]), parts[3]
            if action == "update": return 200, self.service.update_disruption(disruption_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit():
            plan_id, action = int(parts[2]), parts[3]
            if action == "assignments": return 200, self.service.add_assignment(plan_id, actor, role, body)
            if action == "validate": return 200, self.service.validate_plan(plan_id, actor, role)
            if action == "lock": return 200, self.service.lock_plan(plan_id, actor, role, body)
            if action == "process": return 200, self.service.process_plan(plan_id, actor, role, body)
            if action == "restore": return 200, self.service.restore_plan(plan_id, actor, role, body)
            if action == "rebase": return 200, self.service.rebase_plan(plan_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "flights"] and parts[2].isdigit():
            flight_id, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_flight(flight_id, actor, role, body)
            if action == "recover": return 200, self.service.recover_flight(flight_id, actor, role, body)
            if action == "execute": return 200, self.service.execute_flight(flight_id, actor, role, body)
            if action == "review": return 200, self.service.review_flight(flight_id, actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path)
            respond(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            respond(self, exc.status, payload)
        except Exception as exc:
            print(f"unhandled error: {exc!r}"); respond(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = AirlineRecoveryService(db_path)
    handler = type("AirlineHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("AIRLINE_DB", "airline_recovery.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"airline-recovery listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
