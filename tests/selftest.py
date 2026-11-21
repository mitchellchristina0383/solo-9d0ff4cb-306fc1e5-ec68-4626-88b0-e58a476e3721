"""端到端自测：并行双轮、独立数据空间、单轮超时、初始化/清理失败隔离、重置冲突。

用法：
  docker compose up -d --build 之后：
      docker compose exec web python tests/selftest.py
  或对着任意可达的 PostgreSQL（需有 CREATEDB 权限）：
      PGHOST=... PGPORT=... PGDATABASE=... PGUSER=... PGPASSWORD=... python tests/selftest.py

维护库（PGDATABASE）只用于 CREATE/DROP DATABASE；自测会在其中临时创建
obs_selftest_* 数据库并在结束时删除。所有用例都在维护库上跑。
"""
import os
import sys
import time
import traceback

import psycopg2

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "app"))

import engine  # noqa: E402
from engine import run_experiment, run_parallel_experiment  # noqa: E402
from presets import PRESETS  # noqa: E402
from script_parser import ScriptError, parse_script  # noqa: E402

FAILURES = []
PASSES = []


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "postgres"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


def preset(pid):
    return next(p for p in PRESETS if p["id"] == pid)


def run_preset(pid, timeout=6.0, **kwargs):
    p = preset(pid)
    steps = parse_script(p["script"])
    return run_experiment(conninfo(), p["init_sql"], steps, p["check_sql"], timeout, **kwargs)


def run_isolated(init_sql, script, check_sql, timeout=6.0, run_id="selftest", timeout_by_isolation=None):
    steps = parse_script(script)
    return run_parallel_experiment(
        conninfo(), init_sql, steps, check_sql, timeout,
        run_id=run_id, timeout_by_isolation=timeout_by_isolation)


def check(name, cond, detail=""):
    status = "PASS" if cond else "FAIL"
    print(f"  [{status}] {name}" + (f"  ({detail})" if detail and not cond else ""))
    if cond:
        PASSES.append(name)
    else:
        FAILURES.append(name)


def check_value(rows, col_name, columns):
    idx = columns.index(col_name)
    return rows[0][idx]


def by_iso(rounds):
    return {r["isolation"]: r for r in rounds}


def leftover_databases(prefix="obs_"):
    conn = psycopg2.connect(**conninfo())
    conn.autocommit = True
    try:
        cur = conn.cursor()
        cur.execute("SELECT datname FROM pg_database WHERE datname LIKE %s ORDER BY datname",
                    (prefix + "%",))
        return [row[0] for row in cur.fetchall()]
    finally:
        conn.close()


def drop_if_left(name):
    try:
        engine.drop_round_database(conninfo(), name)
    except Exception:
        pass


def test_parser():
    print("== 脚本解析器 ==")
    steps = parse_script(preset("lock-wait")["script"])
    check("解析行锁等待样例得到 4 步", len(steps) == 4, f"got {len(steps)}")
    check("第 2 步是 B/blocked", steps[1].session == "B" and steps[1].mode == "blocked")
    check("其余步骤是 complete 模式", all(s.mode == "complete" for s in steps if s.index != 1))
    try:
        parse_script("SELECT 1;")
        check("头部之外的 SQL 报错", False)
    except ScriptError:
        check("头部之外的 SQL 报错", True)
    try:
        parse_script("@A complete\n")
        check("空步骤报错", False)
    except ScriptError:
        check("空步骤报错", True)
    try:
        parse_script("@A turbo\nSELECT 1;")
        check("未知模式报错", False)
    except ScriptError:
        check("未知模式报错", True)


def test_write_skew():
    print("== 两人同时撤班（写偏斜，双轮并行） ==")
    t0 = time.monotonic()
    rounds = run_preset("shift-withdraw")
    wall = time.monotonic() - t0
    rc, ser = rounds
    check("RC 轮正常完成", not rc["timed_out"] and not rc["error"], rc.get("error"))
    check("RC 两事务都提交",
          rc["outcomes"]["A"]["outcome"] == "committed"
          and rc["outcomes"]["B"]["outcome"] == "committed",
          str(rc["outcomes"]))
    rc_count = check_value(rc["check"]["rows"], "on_duty_count", rc["check"]["columns"])
    check("RC 撤班后无人值守（on_duty_count = 0）", rc_count == 0, f"got {rc_count}")

    check("SER 轮正常完成", not ser["timed_out"] and not ser["error"], ser.get("error"))
    outcomes = sorted(o["outcome"] for o in ser["outcomes"].values())
    check("SER 恰一个提交、一个中止", outcomes == ["aborted", "committed"], str(ser["outcomes"]))
    has_40001 = any(e["kind"] == "step_error" and e.get("pgcode") == "40001"
                    for e in ser["events"])
    check("SER 时间线出现 40001 序列化失败", has_40001)
    ser_count = check_value(ser["check"]["rows"], "on_duty_count", ser["check"]["columns"])
    check("SER 仍有一人在岗（on_duty_count = 1）", ser_count == 1, f"got {ser_count}")

    return wall, rounds


def test_parallel_independent_spaces():
    print("== 双轮并行 + 独立初始数据空间 ==")
    p = preset("shift-withdraw")
    steps = parse_script(p["script"])

    def once():
        t = time.monotonic()
        rounds, run_id = run_parallel_experiment(
            conninfo(), p["init_sql"], steps, p["check_sql"], 6.0, run_id="selftest")
        return time.monotonic() - t, rounds, run_id

    (wall, rounds, run_id) = once()
    rc, ser = rounds
    check("两轮数据库名不同", rc["database"] != ser["database"],
          f"{rc['database']} / {ser['database']}")
    check("两轮数据库名都带本次 run_id",
          run_id in rc["database"] and run_id in ser["database"])
    check("两轮均标记为可用", rc["available"] and ser["available"])
    check("两轮均有自己的时间线事件", bool(rc["events"]) and bool(ser["events"]))
    check("两轮总墙钟明显小于串行耗时之和",
          wall < max(rc["duration"], ser["duration"]) * 1.8 + 0.3,
          f"wall={wall:.3f} rc={rc['duration']} ser={ser['duration']}")
    check("两轮的核对结果互不影响（RC=0，SER=1）",
          check_value(rc["check"]["rows"], "on_duty_count", rc["check"]["columns"]) == 0
          and check_value(ser["check"]["rows"], "on_duty_count", ser["check"]["columns"]) == 1)
    check("运行结束不残留本轮数据库", not leftover_databases("obs_selftest_"),
          str(leftover_databases("obs_selftest_")))

    # 重叠执行：同一 run_id 的两次提交并发进行，库名必须不同
    results = {}

    def submit(tag):
        results[tag] = run_parallel_experiment(
            conninfo(), p["init_sql"], steps, p["check_sql"], 6.0, run_id="dup")

    import threading
    threads = [threading.Thread(target=submit, args=(i,)) for i in range(2)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    dbs = [r["database"] for _, (rs, _) in results.items() for r in rs]
    check("同一 run_id 的两次提交得到 4 个不同数据库", len(set(dbs)) == 4, str(dbs))
    ok = all(r["available"] for _, (rs, _) in results.items() for r in rs)
    check("同一 run_id 的两次提交都完整可用", ok)
    values = sorted(
        check_value(r["check"]["rows"], "on_duty_count", r["check"]["columns"])
        for _, (rs, _) in results.items() for r in rs
    )
    check("两次提交的结果都是 RC=0 / SER=1（没有被对方重置污染）",
          values == [0, 0, 1, 1], str(values))
    check("重叠提交结束后不残留数据库", not leftover_databases("obs_dup_"),
          str(leftover_databases("obs_dup_")))


def test_lock_wait():
    print("== 行锁等待（blocked 模式） ==")
    rc, ser = run_preset("lock-wait")
    check("RC 轮正常完成", not rc["timed_out"] and not rc["error"], rc.get("error"))
    blocked_ev = [e for e in rc["events"] if e["kind"] == "blocked" and e["session"] == "B"]
    check("B 被数据库确认阻塞", len(blocked_ev) == 1)
    check("阻塞者正是会话 A", bool(blocked_ev and blocked_ev[0]["by_other"]))
    resumed = [e for e in rc["events"]
               if e["kind"] == "step_ok" and e["session"] == "B" and e.get("after_blocked")]
    check("A 提交后 B 的 UPDATE 解除阻塞并完成", bool(resumed))
    check("RC 两事务都提交",
          rc["outcomes"]["A"]["outcome"] == "committed"
          and rc["outcomes"]["B"]["outcome"] == "committed",
          str(rc["outcomes"]))
    rc_bal = check_value(rc["check"]["rows"], "balance", rc["check"]["columns"])
    check("RC 余额 = 700（B 在 A 的结果上继续）", rc_bal == 700, f"got {rc_bal}")

    check("SER 轮正常完成", not ser["timed_out"] and not ser["error"], ser.get("error"))
    check("SER 下 A 提交、B 中止",
          ser["outcomes"]["A"]["outcome"] == "committed"
          and ser["outcomes"]["B"]["outcome"] == "aborted",
          str(ser["outcomes"]))
    ser_bal = check_value(ser["check"]["rows"], "balance", ser["check"]["columns"])
    check("SER 余额 = 900（只有 A 生效）", ser_bal == 900, f"got {ser_bal}")


def test_timeout_parallel():
    print("== 单轮超时（并行时只影响该轮） ==")
    # 同一个脚本在两种隔离级别下耗时不同：A 提交前先 sleep 1.5 秒。
    # RC 轮超时 1 秒 → 在 A 的事务里被取消并回滚；SER 轮超时 5 秒 → 正常跑完。
    init = preset("lock-wait")["init_sql"]
    script = """@A complete
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

@B blocked
UPDATE accounts SET balance = balance - 200 WHERE id = 1;

@A complete
SELECT pg_sleep(1.5);

@A complete
COMMIT;

@B complete
COMMIT;"""
    t0 = time.monotonic()
    rounds, _ = run_isolated(
        init, script, "SELECT id, balance FROM accounts ORDER BY id;",
        timeout=5.0, run_id="selftesttout",
        timeout_by_isolation={"READ COMMITTED": 1.0, "SERIALIZABLE": 5.0})
    wall = time.monotonic() - t0
    rc, ser = rounds

    check("RC 轮判定为超时", rc["timed_out"], str(rc.get("error")))
    check("RC 两事务都被回滚",
          rc["outcomes"]["A"]["outcome"] == "rolled_back"
          and rc["outcomes"]["B"]["outcome"] == "rolled_back", str(rc["outcomes"]))
    check("RC 时间线出现语句被取消（57014）",
          any(e["kind"] == "step_error" and e.get("pgcode") == "57014" for e in rc["events"]))
    rc_bal = check_value(rc["check"]["rows"], "balance", rc["check"]["columns"])
    check("RC 余额保持 1000（未提交的全部回滚）", rc_bal == 1000, f"got {rc_bal}")

    check("SER 轮未超时、正常完成", not ser["timed_out"] and not ser.get("error"),
          str(ser.get("error")))
    check("SER 下 A 提交、B 中止（按原脚本运行）",
          ser["outcomes"]["A"]["outcome"] == "committed"
          and ser["outcomes"]["B"]["outcome"] == "aborted",
          str(ser["outcomes"]))
    ser_bal = check_value(ser["check"]["rows"], "balance", ser["check"]["columns"])
    check("SER 余额 = 900（RC 的超时回滚没有污染 SER）", ser_bal == 900, f"got {ser_bal}")
    check("SER 轮按自己的 5 秒时限跑完（sleep 1.5 秒）",
          ser["duration"] >= 1.4, f"duration={ser['duration']}")
    check("RC 在约 1 秒时结束（用自己 1 秒的时限）",
          rc["duration"] < 2.5, f"duration={rc['duration']}")
    check("两轮并行：总墙钟接近 SER 的 1.5 秒而非 1 + 1.5 秒",
          wall < 2.8, f"wall={wall:.3f}")

    # 两轮都超时（每轮 3 秒，并行完成，总墙钟 ~3 秒）
    t0 = time.monotonic()
    rounds2 = run_preset("timeout-demo", timeout=3.0)
    wall2 = time.monotonic() - t0
    check("两轮都超时时也都正确回滚",
          all(r["timed_out"] for r in rounds2)
          and all(o["outcome"] == "rolled_back"
                  for r in rounds2 for o in r["outcomes"].values()),
          str([r["outcomes"] for r in rounds2]))
    check("两轮都超时时并行完成（墙钟 < 5 秒）", wall2 < 5.0, f"wall={wall2:.3f}")

    # 超时之后立刻重跑另一组实验：仍应从初始数据开始并正常完成
    rc2, _ = run_preset("lock-wait")
    bal2 = check_value(rc2["check"]["rows"], "balance", rc2["check"]["columns"])
    check("超时后重跑仍从初始数据开始（锁等待样例 RC 余额 = 700）",
          not rc2["timed_out"] and bal2 == 700, f"got {bal2}")
    check("单轮超时用例结束后无残留数据库",
          not leftover_databases("obs_selftesttout_"),
          str(leftover_databases("obs_selftesttout_")))


def test_init_failure_isolation():
    print("== 初始化失败只标记该轮 ==")
    p = preset("lock-wait")
    bad_init = p["init_sql"] + "\nSELECT 1/0;"
    rounds, _ = run_isolated(bad_init, p["script"], p["check_sql"],
                             run_id="selftestinit")
    check("初始数据失败时两轮都标记为不可用",
          all(not r["available"] and r["setup_failed"] for r in rounds),
          str([(r["available"], r["setup_failed"]) for r in rounds]))
    check("初始化失败的两轮都没有执行核对查询",
          all(r["check"] is None for r in rounds))
    check("初始化失败的两轮数据库都被清理",
          not leftover_databases("obs_selftestinit_"),
          str(leftover_databases("obs_selftestinit_")))

    # 只让 RC 轮初始化失败：SER 仍应正常跑完
    orig = engine.create_round_database

    def flaky_create(ci, name):
        if "_rc_" in name:
            raise RuntimeError("simulated RC init failure")
        return orig(ci, name)

    engine.create_round_database = flaky_create
    try:
        rounds, _ = run_isolated(p["init_sql"], p["script"], p["check_sql"],
                                 run_id="selftestinit1")
    finally:
        engine.create_round_database = orig
    rc, ser = rounds
    check("RC 轮初始化失败 → 标记不可用", not rc["available"] and rc["setup_failed"],
          str(rc))
    check("SER 轮不受影响、正常完成", ser["available"] and not ser["error"],
          str(ser.get("error")))
    ser_bal = check_value(ser["check"]["rows"], "balance", ser["check"]["columns"])
    check("SER 轮核对结果正确（余额 900）", ser_bal == 900, f"got {ser_bal}")
    check("失败轮的数据库没有残留", not leftover_databases("obs_selftestinit1_"),
          str(leftover_databases("obs_selftestinit1_")))


def test_cleanup_failure_isolation():
    print("== 清理失败只标记该轮 ==")
    p = preset("shift-withdraw")
    orig = engine.drop_round_database
    failed_db = {}

    def flaky_drop(ci, name):
        if "_ser_" in name:
            failed_db["name"] = name
            raise RuntimeError("simulated cleanup failure")
        return orig(ci, name)

    engine.drop_round_database = flaky_drop
    try:
        rounds, _ = run_isolated(p["init_sql"], p["script"], p["check_sql"],
                                 run_id="selftestdrop")
    finally:
        engine.drop_round_database = orig
    rc, ser = rounds
    check("RC 轮清理成功、结果可用", rc["available"] and not rc["cleanup_failed"],
          str(rc.get("cleanup_error")))
    check("SER 轮清理失败 → 标记不可用", not ser["available"] and ser["cleanup_failed"],
          str(ser))
    check("SER 清理失败不影响 RC 的核对结果",
          check_value(rc["check"]["rows"], "on_duty_count", rc["check"]["columns"]) == 0)
    check("清理失败后只残留 SER 自己的数据库",
          failed_db.get("name") in leftover_databases("obs_selftestdrop_")
          and len(leftover_databases("obs_selftestdrop_")) == 1,
          str(leftover_databases("obs_selftestdrop_")))
    check("残留的数据库名只对应 SER 轮",
          "_ser_" in (failed_db.get("name") or ""), str(failed_db))
    if failed_db.get("name"):
        drop_if_left(failed_db["name"])
    check("手工清理后无残留", not leftover_databases("obs_selftestdrop_"))


def test_reset_conflict():
    print("== 重置冲突：重复提交不交叉覆盖 ==")
    p = preset("lock-wait")
    steps = parse_script(p["script"])
    # 两次提交使用相同的 run_id，并且故意让第一次的初始数据不同（余额 5000），
    # 如果重置交叉覆盖，就会出现 5500/5700 之类的串值。
    init_a = p["init_sql"].replace("VALUES (1, 1000)", "VALUES (1, 1000)")
    init_b = p["init_sql"].replace("VALUES (1, 1000)", "VALUES (1, 5000)")

    import threading
    out = {}
    barrier = threading.Barrier(2)

    def submit(tag, init):
        barrier.wait()
        out[tag] = run_parallel_experiment(
            conninfo(), init, steps, p["check_sql"], 6.0, run_id="conflict")

    threads = [
        threading.Thread(target=submit, args=("a", init_a)),
        threading.Thread(target=submit, args=("b", init_b)),
    ]
    for th in threads:
        th.start()
    for th in threads:
        th.join()

    dbs = {tag: [r["database"] for r in rs] for tag, (rs, _) in out.items()}
    all_dbs = dbs["a"] + dbs["b"]
    check("同 run_id 的两次并行提交得到 4 个不同数据库", len(set(all_dbs)) == 4, str(dbs))
    check("两次提交的所有轮次都可用",
          all(r["available"] for _, (rs, _) in out.items() for r in rs))
    ba = by_iso(out["a"][0])["READ COMMITTED"]
    bb = by_iso(out["b"][0])["READ COMMITTED"]
    bal_a = check_value(ba["check"]["rows"], "balance", ba["check"]["columns"])
    bal_b = check_value(bb["check"]["rows"], "balance", bb["check"]["columns"])
    check("提交 A 的余额 = 700（1000 - 100 - 200）", bal_a == 700, f"got {bal_a}")
    check("提交 B 的余额 = 4700（5000 - 100 - 200）", bal_b == 4700, f"got {bal_b}")
    check("两次提交之间没有重置交叉覆盖",
          bal_a in (700,) and bal_b in (4700,), f"a={bal_a} b={bal_b}")
    check("两次提交的 SER 轮各自正确（A=900，B=4900）",
          check_value(by_iso(out["a"][0])["SERIALIZABLE"]["check"]["rows"], "balance",
                      by_iso(out["a"][0])["SERIALIZABLE"]["check"]["columns"]) == 900
          and check_value(by_iso(out["b"][0])["SERIALIZABLE"]["check"]["rows"], "balance",
                          by_iso(out["b"][0])["SERIALIZABLE"]["check"]["columns"]) == 4900)
    check("重置冲突用例结束后无残留", not leftover_databases("obs_conflict_"),
          str(leftover_databases("obs_conflict_")))


def test_no_block_branch():
    print("== blocked 模式但语句未阻塞 ==")
    init = preset("lock-wait")["init_sql"]
    script = """@B blocked
UPDATE accounts SET balance = 0 WHERE id = 999;

@B complete
COMMIT;"""
    steps = parse_script(script)
    rc, _ = run_experiment(conninfo(), init, steps,
                           "SELECT balance FROM accounts WHERE id = 1;", 5.0)
    check("语句直接完成、本轮未超时", not rc["timed_out"] and not rc["error"])
    check("时间线记录“未阻塞”事件",
          any(e["kind"] == "no_block" for e in rc["events"]))
    bal = check_value(rc["check"]["rows"], "balance", rc["check"]["columns"])
    check("未命中任何行，余额不变（1000）", bal == 1000, f"got {bal}")


def main():
    tests = [
        ("脚本解析器", test_parser),
        ("写偏斜", test_write_skew),
        ("并行与独立数据空间", test_parallel_independent_spaces),
        ("行锁等待", test_lock_wait),
        ("单轮超时", test_timeout_parallel),
        ("初始化失败隔离", test_init_failure_isolation),
        ("清理失败隔离", test_cleanup_failure_isolation),
        ("重置冲突", test_reset_conflict),
        ("blocked 未阻塞分支", test_no_block_branch),
    ]
    started = time.monotonic()
    for name, fn in tests:
        try:
            fn()
        except Exception:
            FAILURES.append(f"{name}（未捕获异常）")
            print(f"  [FAIL] {name} 抛出异常：")
            traceback.print_exc()
    print()
    print(f"通过 {len(PASSES)} 项，失败 {len(FAILURES)} 项，"
          f"总耗时 {time.monotonic() - started:.2f}s")
    if FAILURES:
        print("失败项：")
        for name in FAILURES:
            print("  -", name)
        return 1
    print("全部自测通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
