"""Flask 入口：页面 + 运行接口。

环境变量：PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD / PORT

每次提交的两种隔离级别会**并行**各跑一轮，每轮在维护库里创建自己的数据库
（维护库即 PGDATABASE，只用来 CREATE/DROP DATABASE，不承载实验表）。
两轮的初始化和清理互相独立；重复提交也各自使用新的随机数据库名，不会交叉覆盖。
"""
import os
import uuid

from flask import Flask, jsonify, render_template, request

from engine import run_parallel_experiment
from presets import PRESETS
from script_parser import ScriptError, parse_script

app = Flask(__name__)

DEFAULT_TIMEOUT = 5.0
MAX_TIMEOUT = 60.0


def conninfo():
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "dbname": os.environ.get("PGDATABASE", "sandbox"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", ""),
        "connect_timeout": 10,
    }


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/presets")
def get_presets():
    return jsonify(PRESETS)


@app.post("/api/run")
def run():
    body = request.get_json(force=True, silent=True) or {}
    init_sql = (body.get("init_sql") or "").strip()
    script = (body.get("script") or "").strip()
    check_sql = (body.get("check_sql") or "").strip()
    try:
        timeout_s = float(body.get("timeout_s") or DEFAULT_TIMEOUT)
    except (TypeError, ValueError):
        timeout_s = DEFAULT_TIMEOUT
    timeout_s = min(max(timeout_s, 1.0), MAX_TIMEOUT)

    if not init_sql:
        return jsonify({"error": "初始数据 SQL 不能为空"}), 400
    if not check_sql:
        return jsonify({"error": "核对查询不能为空"}), 400
    try:
        steps = parse_script(script)
    except ScriptError as exc:
        return jsonify({"error": f"脚本解析失败：{exc}"}), 400

    # 客户端可选传入 run_id（用于页面标识）；缺省由服务端生成。
    # 数据库名还会附加随机后缀，所以即使两次提交使用同一 run_id 也互不影响。
    run_id = str(body.get("run_id") or "").strip() or uuid.uuid4().hex[:12]

    rounds, run_id = run_parallel_experiment(
        conninfo(), init_sql, steps, check_sql, timeout_s, run_id=run_id)
    return jsonify({"rounds": rounds, "timeout_s": timeout_s, "run_id": run_id,
                    "parallel": True})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), threaded=True)
