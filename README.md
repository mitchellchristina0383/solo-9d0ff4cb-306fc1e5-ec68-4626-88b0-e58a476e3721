# 并发写入观测台（隔离级别沙箱）

给数据库工程师的一个小工具：在改隔离级别之前，先看看**同一组并发写入**在不同隔离级别下会留下什么数据。

两个会话（A、B）按可编辑的脚本交错执行 SQL。同一组脚本、同一份初始数据，分别在
**READ COMMITTED** 和 **SERIALIZABLE** 下**并行**各跑一轮，页面上并排展示：

- 每一步的执行时间线（完成 / 出错 / 被阻塞 / 解除阻塞）；
- 每个事务的结局（已提交 / 已回滚 / 40001 中止）；
- 两轮事务都结束后，各自在本轮数据库上核对查询查到的最终数据。

**两轮并行、数据空间隔离**：每次提交时，两轮同时开始，各自在维护库里创建一个
专属数据库（从 `template0` 复制），初始数据 SQL、会话 A/B、核对查询都只连自己的库；
结束后各自删除自己的库。某一轮建库 / 初始化 / 清理失败只把该轮标记为“不可用”，
另一轮照常跑完并给出结果。数据库名包含 run_id 和随机后缀，因此重复提交（哪怕
两次 run_id 相同）也各有各的库，重置和清理不会交叉覆盖。

每步有两种推进方式：

| 模式 | 含义 |
| --- | --- |
| `complete`（默认） | 等这条语句执行完成再推进下一步 |
| `blocked` | 语句在后台执行，等数据库确认它**被另一会话阻塞**（`pg_blocking_pids()`）就推进下一步；语句仍在飞，持锁会话可以继续走完自己的 `COMMIT` |

每轮有超时（默认 5 秒）：超时立即结束本轮，取消卡住的语句并**回滚未提交的事务**，
另一轮不受影响。重跑总是从“初始数据 SQL”重新开始，两轮之间、两次运行之间互不影响。

> 维护库（`PGDATABASE`，Compose 里是 `sandbox`）只用于 CREATE/DROP DATABASE，
> 不承载实验表；实验表只存在于每轮临时创建的数据库里。

## 快速开始

```bash
docker compose up --build
# 打开 http://localhost:8080
```

`web` 依赖 `db` 的健康检查，数据库就绪后页面即可用。端口默认 `127.0.0.1:8080`
（本工具不做任何鉴权，会原样执行你贴的 SQL，请勿暴露到公共网络）。

## 页面与脚本语法

页面上三段可编辑内容：

1. **初始数据 SQL**：每轮开始前在本轮自己的数据库里执行（可多条语句），负责把该库
   重置到相同起点；
2. **交错脚本**：见下；
3. **核对查询**：本轮事务都结束后，在本轮数据库的新连接上执行并展示结果。

交错脚本由步骤组成，每个步骤一行头部 + 一段 SQL：

```sql
@A complete
UPDATE accounts SET balance = balance - 100 WHERE id = 1;

@B blocked
UPDATE accounts SET balance = balance - 200 WHERE id = 1;

@A complete
COMMIT;
```

- 头部：`@A` 或 `@B`，模式可写 `complete` / `blocked`（也接受 `完成` / `阻塞`），省略时是 `complete`；
- 事务由引擎按本轮隔离级别自动 `BEGIN ISOLATION LEVEL ...`，**脚本里不要写 `BEGIN`**，
  用 `COMMIT` / `ROLLBACK` 结束事务；脚本结束仍未提交的事务会被引擎回滚并记在时间线里；
- 一个步骤里可以写多条语句（结果只展示最后一条的），但建议一步一条，时间线更清晰。

## 内置样例

页面顶部可一键载入：

1. **两人同时撤班（写偏斜）**：`alice`、`bob` 两人在岗，规则是“至少留一人在岗”。
   两人同时撤班：各自确认在岗人数 ≥ 2 才撤。READ COMMITTED 下两笔都成功，
   核对查询显示 `on_duty_count = 0`——**无人值守**；SERIALIZABLE 下后提交者收到
   `40001 could not serialize access`，事务中止，仍有一人在岗。
2. **行锁等待（blocked 模式）**：两个会话更新同一行。B 用 `blocked` 模式确认被 A
   阻塞即推进，让 A 先提交。READ COMMITTED 下 B 在 A 的结果上继续（余额 700）；
   SERIALIZABLE 下 B 解除阻塞时收到 40001（余额 900）。
3. **超时回滚（演示）**：B 用 `complete` 模式等 A 的行锁，而 A 的 `COMMIT` 排在 B 之后
   永远轮不到 → 本轮超时，引擎取消卡住的语句并回滚两个事务，余额保持 1000。
   超时后立刻重跑，仍从初始数据开始。

## 自测

栈起来之后，在容器里跑端到端自测（覆盖：脚本解析、写偏斜两种结局、行锁等待的阻塞
确认与解除、**双轮并行与独立数据库**、**单轮超时不影响另一轮**、**初始化/清理失败只
标记该轮**、**同 run_id 重复提交的重置冲突**、blocked 模式未阻塞分支）：

```bash
docker compose up -d --build
docker compose exec web python tests/selftest.py
```

也可以对着任意可达的 PostgreSQL（需要 CREATEDB 权限）跑：

```bash
pip install -r app/requirements.txt
PGHOST=... PGDATABASE=... PGUSER=... PGPASSWORD=... python tests/selftest.py
```

自测会在维护库里临时建 `obs_selftest*` / `obs_conflict*` / `obs_dup*` 数据库并在
用例结束时删除；若中途异常退出，可手工清理这些前缀的库。

## 实现要点

- `app/engine.py`：执行引擎。`run_parallel_experiment()` 用线程池同时启动两轮；
  每轮先 `CREATE DATABASE ... TEMPLATE template0`，再在库内 `run_round()`：
  每个会话一条连接（`autocommit=True`，事务边界完全由 SQL 语句控制），语句在工作
  线程里执行；`blocked` 模式通过控制连接轮询 `pg_blocking_pids()` 确认阻塞；超时用
  `pg_cancel` 取消在飞语句；收尾先回滚“没有在飞语句”的事务（可能借此解除另一会话的
  阻塞），再等在飞语句结束，最后删掉本轮数据库。每轮的可用性单独记录
  （`available` / `setup_failed` / `cleanup_failed`）。
- 事务结局以服务器命令标签为准：`COMMIT` 一个已失败的事务时 PostgreSQL 实际返回
  `ROLLBACK`，页面会如实显示为回滚；`40001` / `40P01` 记为“已中止”。
- `app/script_parser.py`：`@A complete` / `@B blocked` 头部解析，带行号报错。
- `app/main.py`：Flask 页面与 `/api/run`。不再有全局锁：并发提交各自建库、并行执行。
- `tests/selftest.py`：上面的端到端自测。

## 限制与说明

- 只支持两个会话；`blocked` 确认是 50ms 轮询，时间线精度有限；
- 引擎**不会**自动重试 40001（重试策略属于应用层，可在脚本里自行表达）；
- 单步可含多条语句，但只展示最后一条的结果；
- 初始数据 SQL 在维护库的超级用户身份下执行 `CREATE DATABASE`，脚本则在每轮临时库里
  以同一身份执行，能力不设限——这是本地沙箱，不是多租户服务；
- 建库/删库需要连接维护库的账号拥有 `CREATEDB` 权限（Compose 默认的 `postgres` 满足）；
- 每轮建库需要几十到一百多毫秒，双轮并行时总耗时约等于单轮较慢者；
- 数据不持久化：每轮临时库用完即删，容器删除即清空。若进程在删库前崩溃，可能残留
  `obs_*` 数据库，可用 `DROP DATABASE` 手工清理。
