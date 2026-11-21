"""并发执行引擎。

每一轮（round）：
  1. 在维护库上为本轮创建一个**独立数据库**（从 template0 复制），作为本轮的
     “初始数据空间”；两轮同时创建、互不共享任何表；
  2. 在本轮数据库上用控制连接执行“初始数据 SQL”；
  3. 会话 A、B 各开一条连接（连到本轮数据库），按本轮隔离级别 BEGIN；
  4. 按脚本顺序交错执行步骤：
     - complete 模式：等语句执行完毕（直到超时）；
     - blocked  模式：语句在后台执行，轮询 pg_blocking_pids()，
       一旦数据库确认它被另一会话阻塞就推进下一步（语句仍在飞），
       持锁会话的后续步骤（如 COMMIT）不受影响；
  5. 收尾：先回滚“没有在飞语句”的事务（可能借此解除另一会话的阻塞），
     再等在飞语句结束（等不到就 pg_cancel），最后回滚仍未提交的事务；
  6. 在本轮数据库的控制连接上执行核对查询（只读本轮的数据）；
  7. 关闭本轮所有连接，删除本轮数据库（只删自己的库）。

隔离：两轮各自使用独立数据库，数据库名带本次运行的 run_id 和随机后缀，
所以并发提交两次运行、甚至两次传入相同 run_id，重置与清理也不会交叉覆盖；
某一轮建库/初始化/清理失败只影响该轮（available=False），另一轮照常完成。

超时：任何一步等待超过本轮时限即抛 RoundTimeout，本轮结束并按第 5 步回滚。
重开一轮总是从第 2 步的初始数据重新开始。
"""
from __future__ import annotations

import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import psycopg2
from psycopg2 import extensions, sql

ISOLATION_LEVELS = ("READ COMMITTED", "SERIALIZABLE")

ISOLATION_SLUGS = {"READ COMMITTED": "rc", "SERIALIZABLE": "ser"}

DB_NAME_PREFIX = "obs"
MAX_DB_NAME_RUN_ID = 16
DB_CREATE_TIMEOUT_S = 30.0

MAX_ROWS = 50          # 单步 SELECT 结果最多带回前端的行数
POLL_INTERVAL = 0.05   # 阻塞确认轮询间隔（秒）
CLEANUP_GRACE = 2.0    # 收尾时等在飞语句完成的宽限（秒）


class RoundTimeout(Exception):
    """本轮超时：结束本轮，由收尾逻辑回滚未提交事务。"""


def _normalize(value):
    """把 psycopg2 返回的值转成 JSON 可序列化的形式。"""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def _err_info(exc):
    pgcode = getattr(exc, "pgcode", None)
    diag = getattr(exc, "diag", None)
    msg = getattr(diag, "message_primary", None) if diag else None
    return pgcode, (msg or str(exc).strip())


def query_blocking_pids(conn, pid):
    """在控制连接上查询指定后端的阻塞者 pid 列表；查询失败按“未阻塞”处理。"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT pg_blocking_pids(%s)", (pid,))
        row = cur.fetchone()
        cur.close()
        return list(row[0]) if row and row[0] else []
    except Exception:
        return []


class SessionRunner:
    """一个数据库会话：一条连接，同一时刻至多一条在飞语句。"""

    def __init__(self, name, conninfo, isolation, log):
        self.name = name
        self.log = log
        self.conn = psycopg2.connect(**conninfo)
        self.conn.autocommit = True  # 事务边界完全由 BEGIN/COMMIT 语句控制
        self.pid = self.conn.get_backend_pid()
        self.inflight = None   # (box, step)，仍在后台执行的语句
        self.outcome = None    # committed / rolled_back / aborted
        self.outcome_note = ""
        cur = self.conn.cursor()
        cur.execute(f"BEGIN ISOLATION LEVEL {isolation}")
        cur.close()
        log(self.name, "begin", detail=f"BEGIN ISOLATION LEVEL {isolation}", pid=self.pid)

    # ---- 内部 ----

    def _start(self, sql_text):
        """在工作线程里执行语句，主线程可以继续轮询阻塞状态。"""
        box = {
            "event": threading.Event(),
            "error": None,
            "columns": None,
            "rows": None,
            "status": None,
            "was_in_txn": self.conn.get_transaction_status()
            != extensions.TRANSACTION_STATUS_IDLE,
        }

        def work():
            try:
                cur = self.conn.cursor()
                cur.execute(sql_text)
                box["status"] = cur.statusmessage
                if cur.description:
                    box["columns"] = [d.name for d in cur.description]
                    box["rows"] = [
                        [_normalize(v) for v in row] for row in cur.fetchmany(MAX_ROWS)
                    ]
                cur.close()
            except Exception as exc:  # 任何错误都要带回时间线
                box["error"] = exc
            finally:
                box["event"].set()

        thread = threading.Thread(target=work, daemon=True)
        thread.start()
        box["thread"] = thread
        return box

    def _collect(self, box, step, after_blocked):
        """语句结束后记录结果，并维护事务结局。"""
        err = box["error"]
        if err is not None:
            pgcode, msg = _err_info(err)
            self.log(self.name, "step_error", step=step.index, sql=step.sql,
                     pgcode=pgcode, error=msg, after_blocked=after_blocked)
            if pgcode in ("40001", "40P01"):  # 序列化失败 / 死锁
                self.outcome = "aborted"
                self.outcome_note = f"{pgcode} {msg}"
            return
        self.log(self.name, "step_ok", step=step.index, sql=step.sql,
                 status=box["status"], columns=box["columns"], rows=box["rows"],
                 after_blocked=after_blocked)
        # 事务随这条语句结束了吗？以服务器返回的命令标签为准
        # （COMMIT 一个已失败的事务时，PG 实际返回 ROLLBACK）。
        if (box["was_in_txn"]
                and self.conn.get_transaction_status() == extensions.TRANSACTION_STATUS_IDLE
                and self.outcome != "aborted"):
            tag = (box["status"] or "").upper()
            if tag == "COMMIT":
                self.outcome = "committed"
            elif tag == "ROLLBACK":
                self.outcome = "rolled_back"

    def _cancel(self):
        try:
            self.conn.cancel()
        except Exception:
            pass

    # ---- 对外 ----

    def join_inflight(self, deadline):
        """执行同一会话的下一步前，先等上一条在飞语句结束。"""
        if self.inflight is None:
            return
        box, step = self.inflight
        self.inflight = None
        remaining = deadline - time.monotonic()
        if not box["event"].wait(max(remaining, 0.0)):
            self._cancel()
            box["event"].wait(1.0)
            self._collect(box, step, after_blocked=True)
            raise RoundTimeout()
        self._collect(box, step, after_blocked=True)

    def run_step(self, step, other, ctl, deadline):
        self.join_inflight(deadline)
        box = self._start(step.sql)
        self.log(self.name, "step_start", step=step.index, sql=step.sql, mode=step.mode)

        if step.mode == "complete":
            remaining = deadline - time.monotonic()
            if not box["event"].wait(max(remaining, 0.0)):
                self._cancel()
                box["event"].wait(1.0)
                self._collect(box, step, after_blocked=False)
                raise RoundTimeout()
            self._collect(box, step, after_blocked=False)
            return

        # blocked 模式：等到数据库确认被另一会话阻塞即推进，语句留在后台
        while True:
            if box["event"].wait(POLL_INTERVAL):
                self._collect(box, step, after_blocked=False)
                if box["error"] is None:
                    self.log(self.name, "no_block", step=step.index, sql=step.sql,
                             detail="语句直接完成，未发生阻塞")
                return
            blockers = query_blocking_pids(ctl, self.pid)
            if blockers:
                self.inflight = (box, step)
                self.log(self.name, "blocked", step=step.index, sql=step.sql,
                         blockers=blockers, by_other=other.pid in blockers,
                         detail="pg_blocking_pids → " + ",".join(map(str, blockers)))
                return
            if time.monotonic() >= deadline:
                self._cancel()
                box["event"].wait(1.0)
                self._collect(box, step, after_blocked=False)
                raise RoundTimeout()

    def rollback_if_open(self, timed_out):
        try:
            status = self.conn.get_transaction_status()
        except Exception:
            return
        if status == extensions.TRANSACTION_STATUS_IDLE:
            return
        try:
            cur = self.conn.cursor()
            cur.execute("ROLLBACK")
            cur.close()
            note = "超时回滚未提交事务" if timed_out else "脚本结束仍有未提交事务，引擎回滚"
            self.log(self.name, "rollback", detail=note)
            if self.outcome is None:
                self.outcome = "rolled_back"
                self.outcome_note = note
        except Exception as exc:
            pgcode, msg = _err_info(exc)
            self.log(self.name, "rollback_error", pgcode=pgcode, error=msg)

    def finish_inflight(self, grace):
        if self.inflight is None:
            return
        box, step = self.inflight
        self.inflight = None
        if not box["event"].wait(grace):
            self._cancel()
            box["event"].wait(1.0)
        if box["event"].is_set():
            self._collect(box, step, after_blocked=True)
        else:
            self.log(self.name, "step_error", step=step.index, sql=step.sql,
                     pgcode=None, error="语句无法取消，放弃连接", after_blocked=True)

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


def _run_check(ctl, check_sql):
    try:
        cur = ctl.cursor()
        cur.execute(check_sql)
        out = {"error": None, "pgcode": None, "columns": None, "rows": None,
               "status": cur.statusmessage}
        if cur.description:
            out["columns"] = [d.name for d in cur.description]
            out["rows"] = [[_normalize(v) for v in row] for row in cur.fetchmany(200)]
        cur.close()
        return out
    except Exception as exc:
        pgcode, msg = _err_info(exc)
        return {"error": msg, "pgcode": pgcode, "columns": None, "rows": None,
                "status": None}


def run_round(conninfo, init_sql, steps, check_sql, isolation, timeout_s):
    """在一个已经就绪的数据库里跑一轮：重置 → 交错执行 → 收尾回滚 → 核对查询。

    ``conninfo`` 的 dbname 必须指向本轮专用数据库。
    """
    events = []
    t0 = time.monotonic()

    def log(session, kind, **kw):
        events.append({"t": round(time.monotonic() - t0, 3),
                       "session": session, "kind": kind, **kw})

    result = {"isolation": isolation, "timed_out": False, "error": None,
              "error_phase": None, "setup_failed": False,
              "events": events, "outcomes": {}, "check": None, "duration": None}
    deadline = t0 + timeout_s
    ctl = None
    sessions = []
    phase = "reset"
    try:
        ctl = psycopg2.connect(**conninfo)
        ctl.autocommit = True
        log("SYS", "reset", detail="重置初始数据")
        cur = ctl.cursor()
        cur.execute(init_sql)
        cur.close()

        phase = "sessions"
        sess_a = SessionRunner("A", conninfo, isolation, log)
        sessions.append(sess_a)
        sess_b = SessionRunner("B", conninfo, isolation, log)
        sessions.append(sess_b)
        by_name = {"A": sess_a, "B": sess_b}

        phase = "script"
        try:
            for step in steps:
                if time.monotonic() >= deadline:
                    raise RoundTimeout()
                sess = by_name[step.session]
                other = by_name["B" if step.session == "A" else "A"]
                sess.run_step(step, other, ctl, deadline)
        except RoundTimeout:
            result["timed_out"] = True
            log("SYS", "timeout",
                detail=f"超过 {timeout_s:g} 秒未推进，结束本轮，回滚未提交事务")
        finally:
            # 收尾顺序：先回滚没有在飞语句的事务（可能解除另一会话的阻塞），
            # 再等在飞语句结束（等不到就取消），最后回滚仍未提交的事务。
            grace = 1.0 if result["timed_out"] else CLEANUP_GRACE
            for s in sessions:
                if s.inflight is None:
                    s.rollback_if_open(result["timed_out"])
            for s in sessions:
                s.finish_inflight(grace)
            for s in sessions:
                s.rollback_if_open(result["timed_out"])
            for s in sessions:
                result["outcomes"][s.name] = {
                    "outcome": s.outcome or "unknown",
                    "note": s.outcome_note,
                    "pid": s.pid,
                }
                s.close()

        phase = "check"
        log("SYS", "check", detail="执行核对查询")
        result["check"] = _run_check(ctl, check_sql)
    except Exception as exc:
        pgcode, msg = _err_info(exc)
        result["error"] = msg
        result["error_phase"] = phase
        # 建会话/执行初始数据失败 = 本轮初始化失败；核对查询失败只算本轮出错。
        result["setup_failed"] = phase in ("reset", "sessions")
        log("SYS", "round_error", pgcode=pgcode, error=msg, phase=phase)
        for s in sessions:
            s.close()
    finally:
        result["duration"] = round(time.monotonic() - t0, 3)
        if ctl is not None:
            try:
                ctl.close()
            except Exception:
                pass
    return result


# ---------------------------------------------------------------------------
# 独立数据空间：本轮专用数据库的创建 / 删除
# ---------------------------------------------------------------------------

def _open_admin(conninfo, application_name):
    cfg = dict(conninfo)
    cfg["application_name"] = application_name
    conn = psycopg2.connect(**cfg)
    conn.autocommit = True
    return conn


def create_round_database(conninfo, name):
    """从 template0 创建一个空数据库，作为本轮独占的数据空间。

    每次调用都必须使用互不相同的 ``name``；这里不检查/复用已有数据库，
    避免两次运行的初始化和清理相互覆盖。
    """
    conn = _open_admin(conninfo, "concurrency-observatory-create")
    try:
        cur = conn.cursor()
        cur.execute("SET statement_timeout = %s", (int(DB_CREATE_TIMEOUT_S * 1000),))
        cur.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0")
                    .format(sql.Identifier(name)))
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def drop_round_database(conninfo, name):
    """删除本轮专用数据库；只按完整数据库名删除，不触碰其他轮/其他运行。"""
    conn = _open_admin(conninfo, "concurrency-observatory-drop")
    try:
        cur = conn.cursor()
        try:
            # PostgreSQL 13+：强制断开残留连接再删除。
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)")
                        .format(sql.Identifier(name)))
        except psycopg2.errors.SyntaxError:
            # 旧版本没有 WITH (FORCE)，退化为普通 DROP。
            cur = conn.cursor()
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}")
                        .format(sql.Identifier(name)))
        cur.close()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _safe_run_id(run_id):
    safe = re.sub(r"[^a-z0-9_]", "", str(run_id or "").lower())
    safe = safe.strip("_")[:MAX_DB_NAME_RUN_ID]
    return safe or "run"


def round_database_name(run_id, isolation):
    """本轮数据库名：run_id + 隔离级别 + 每次唯一的随机后缀。

    随机后缀保证即使两次提交传入同一 run_id，也会落到两个不同的数据库，
    重置和清理不会交叉覆盖。
    """
    return f"{DB_NAME_PREFIX}_{_safe_run_id(run_id)}_{ISOLATION_SLUGS[isolation]}_{uuid.uuid4().hex[:6]}"


def _timeout_for(isolation, timeout_s, timeout_by_isolation):
    value = timeout_s
    if isinstance(timeout_by_isolation, dict):
        value = timeout_by_isolation.get(isolation, timeout_s)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(timeout_s)


def _run_isolated_round(conninfo, init_sql, steps, check_sql, isolation,
                        timeout_s, run_id):
    """建库 → 跑一轮 → 删库；任何阶段失败都只影响本轮的返回值。"""
    t0 = time.monotonic()
    started_at = time.time()
    database = round_database_name(run_id, isolation)
    res = {
        "isolation": isolation,
        "database": database,
        "available": True,
        "setup_failed": False,
        "cleanup_failed": False,
        "cleanup_error": None,
        "timed_out": False,
        "error": None,
        "error_phase": None,
        "events": [],
        "outcomes": {},
        "check": None,
        "duration": None,
        "started_at": started_at,
        "finished_at": None,
    }

    def log(kind, **kw):
        res["events"].append({"t": round(time.monotonic() - t0, 3),
                              "session": "SYS", "kind": kind, **kw})

    created = False
    try:
        log("db_create", detail=f"创建本轮独立数据库 {database}")
        create_round_database(conninfo, database)
        created = True
    except Exception as exc:
        pgcode, msg = _err_info(exc)
        res.update(available=False, setup_failed=True,
                   error_phase="create_database", error=msg)
        log("db_create_error", pgcode=pgcode, error=msg)

    if created:
        round_conninfo = dict(conninfo)
        round_conninfo["dbname"] = database
        run_start = time.monotonic()
        try:
            rr = run_round(round_conninfo, init_sql, steps, check_sql,
                           isolation, timeout_s)
            offset = run_start - t0
            for ev in rr["events"]:
                ev["t"] = round(ev["t"] + offset, 3)
            res["events"].extend(rr["events"])
            for key in ("timed_out", "error", "error_phase", "setup_failed",
                        "outcomes", "check"):
                res[key] = rr.get(key, res.get(key))
        except Exception as exc:  # 引擎自身意外错误；仍继续清理自己的库
            pgcode, msg = _err_info(exc)
            res.update(error=msg, error_phase="engine", available=False)
            log("round_error", pgcode=pgcode, error=msg, phase="engine")

        if res["setup_failed"]:
            res["available"] = False

        try:
            drop_round_database(conninfo, database)
            log("db_drop", detail=f"删除本轮数据库 {database}")
        except Exception as exc:
            pgcode, msg = _err_info(exc)
            res.update(available=False, cleanup_failed=True, cleanup_error=msg)
            log("db_drop_error", pgcode=pgcode, error=msg)

    res["duration"] = round(time.monotonic() - t0, 3)
    res["finished_at"] = time.time()
    return res


def run_parallel_experiment(conninfo, init_sql, steps, check_sql, timeout_s,
                            *, run_id=None, timeout_by_isolation=None):
    """两种隔离级别**并行**各跑一轮，每轮使用自己的数据库。

    ``conninfo`` 指向维护库（用于 CREATE/DROP DATABASE）；每一轮会从 template0
    建一个独立数据库并在结束后删除它。返回顺序固定为 READ COMMITTED、SERIALIZABLE。
    """
    run_id = run_id or uuid.uuid4().hex[:12]

    def one(isolation):
        return _run_isolated_round(
            conninfo, init_sql, steps, check_sql, isolation,
            _timeout_for(isolation, timeout_s, timeout_by_isolation), run_id,
        )

    results = {}
    with ThreadPoolExecutor(max_workers=len(ISOLATION_LEVELS),
                            thread_name_prefix="round") as pool:
        futures = {iso: pool.submit(one, iso) for iso in ISOLATION_LEVELS}
        for iso, future in futures.items():
            try:
                results[iso] = future.result()
            except Exception as exc:  # 理论上 one() 已兜底，这里保证另一轮结果仍可返回
                pgcode, msg = _err_info(exc)
                results[iso] = {
                    "isolation": iso,
                    "database": None,
                    "available": False,
                    "setup_failed": True,
                    "cleanup_failed": False,
                    "cleanup_error": None,
                    "timed_out": False,
                    "error": msg,
                    "error_phase": "engine",
                    "events": [{"t": 0.0, "session": "SYS", "kind": "round_error",
                                "pgcode": pgcode, "error": msg, "phase": "engine"}],
                    "outcomes": {},
                    "check": None,
                    "duration": 0.0,
                    "started_at": time.time(),
                    "finished_at": time.time(),
                }
    return [results[iso] for iso in ISOLATION_LEVELS], run_id


def run_experiment(conninfo, init_sql, steps, check_sql, timeout_s, **kwargs):
    """兼容旧调用：并行跑两轮，只返回 rounds 列表。"""
    rounds, _ = run_parallel_experiment(
        conninfo, init_sql, steps, check_sql, timeout_s, **kwargs)
    return rounds
