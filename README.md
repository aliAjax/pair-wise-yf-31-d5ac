# 航班中断恢复系统

独立的 Python 标准库项目，用 SQLite 保存机场、飞机、机组、航线许可、航班、中断事件和恢复方案。系统会校验维护间隔、执勤时限、机场宵禁、航线许可、资源重叠，并计算取消、延误、受影响旅客和错失衔接成本。

## 运行

```bash
python3 app.py --db airline_recovery.db
```

默认监听 `127.0.0.1:8202`，首页为 `/`，健康检查为 `/health`。

身份头：`X-User-Id`、`X-Role`。角色包括 `viewer`、`scheduler`、`ops_manager`、`auditor`。

## 主要接口

- `POST /api/airports`、`/api/aircraft`、`/api/crew`、`/api/permits`：基础资源与约束。
- `POST /api/flights`、`POST /api/disruptions`：创建航班和中断。
- `POST /api/disruptions/{id}/update`：更新中断窗口/信息（带 `expected_revision`），修订号前进。
- `POST /api/recovery-plans`：一次提交方案及航班调整（记录所基于的中断修订）。
- `POST /api/plans/{id}/assignments`：用 `expected_revision` 临时改派。
- `POST /api/plans/{id}/rebase`：草案重新对齐最新中断修订（旧修订上不能锁定）。
- `POST /api/plans/{id}/validate`、`/lock`：校验并原子锁定方案（只占资源，不改航班）。
- `POST /api/plans/{id}/process`：处理方案，把调整按快照写入航班；失败自动恢复原方案，重试幂等。
- `POST /api/plans/{id}/restore`：撤稿，只回退本方案写入、未被执行或接管的航班值。
- `GET /api/disruptions/{id}/compare`：比较恢复方案成本。
- `POST /api/flights/{id}/cancel`、`/recover`、`/execute`、`/review`：取消、人工恢复、标记执行、待复核确认。
- `GET /api/state`、`GET /api/plans/{id}`：查询状态、过期标记（`stale`）和处理结果（`result`）。

## 修订链路

中断事件、恢复方案、航班执行共用一条修订链：

1. `disruptions.revision` 是链路源头，方案记录 `disruption_revision`。
2. 更新中断信息后版本号 +1，依赖旧版本的锁定方案变为 `stale`（过期），其未执行/未取消且未被新方案接管的航班转为 `pending_review`，调度台必须 `review`（`keep` 接受现值或 `restore` 回退）后才能执行。
3. `lock` 只做约束校验和飞机/机组资源占位，`process` 才写航班值。写入前把航班原值快照存到 assignment（时刻、资源、状态、版本号、写入者）。
4. 处理失败按快照自动恢复原方案；重试只处理 `applied=0` 的调整，不重复改航班；成功后重复提交为幂等返回，不重复占用资源。
5. `restore` 只撤本方案写入的航班值（写入者守卫）；已执行、已人工取消、已被其他方案接管的航班跳过并在结果中列出。
6. 所有写操作要求 `expected_revision`，后提交方在版本不一致时收到 409 `revision_conflict`，响应体 `details.current` 带最新状态。写事务在单进程内串行化（`BEGIN IMMEDIATE` + 写锁）。

页面顶部会汇总待复核航班、过期方案和处理失败方案，并在方案卡片上展示所基于的中断修订与最新修订的差异。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

时区和机场本地时刻没有引入完整时区数据库；模型使用简化航线许可与宵禁规则。身份头、SQLite 和单进程 HTTP 服务适合原型演示，正式运行需要外部身份系统、共享数据库和更强的跨实例锁。
