# 药品生产偏差与批次放行系统

Python 标准库 + SQLite。批次可关联关键/一般偏差、检验复测、返工、供应商变更和稳定性数据。质量人员可以拒绝、再取样、有条件放行或正式放行；关键偏差始终阻止正式放行，修改必须携带当前批次修订号。

## 放行凭据与依据冻结

- 质量人员签发 `release` / `conditional` 决定时生成放行凭据（编号 `CR-{批次}-{修订号}`），同时**冻结**当前批次修订号与五类依据清单：偏差、检验、返工、供应商变更、稳定性，逐项保存内容指纹（SHA-256）。
- 放行/有条件放行后仍允许补录或更正依据（不再像终态一样禁止）；任何依据变化（含修订号变化、新增、字段更正、删除）都会让有效凭据**失效**，批次自动退回新状态 `awaiting_review`（待复核），必须由 QA 刷新到当前版本重新签发后才能继续发运。
- 失效原因逐项落库（`invalidation_events` / `invalidation_reasons`）并写审计；新签发取代旧凭据（`superseded`），拒收则撤销凭据。

## 并发、事务与幂等

- 所有写操作经 `BEGIN IMMEDIATE` + 进程内锁串行化，决定（decide）的读取、校验、写入全部在同一事务内；两个终端同时提交同一批次时，仅当前修订号的提交成功，后到者收到 409。
- 审计写入与业务写入在同一事务，审计失败整笔回滚（决定、凭据、快照、修订号都不会残留）。
- 写接口可携带请求编号实现幂等重试：请求头 `X-Request-Id` 或请求体 `request_id`。同编号 + 同操作人 + 同路由 + 同请求体重放时直接返回首次结果，响应头带 `X-Idempotency-Replayed: 1`；编号被用于不同请求则返回 409。失败请求（4xx/5xx/回滚）不占用编号，可继续用同一编号重试。

## 运行

```bash
python3 app.py --init --seed
python3 app.py
```

默认端口 `8214`。身份通过 `X-Actor` 与 `X-Role` 模拟，角色为 `operator`、`inspector`、`lab`、`qa`。工厂人员只能修改本工厂批次。可用 `--port`、`--db` 覆盖。

## 主要接口

- `POST /api/factories`、`POST /api/batches`：登记工厂和批次。
- `POST /api/batches/{id}/deviations`、`POST /api/deviations/{id}/close`：记录和关闭偏差。
- `POST /api/deviations/{id}/exception`：为一般偏差批准有期限例外。
- `POST /api/batches/{id}/tests`：记录检验和复测轮次。
- `POST /api/batches/{id}/rework`、`POST /api/rework/{id}/complete`：计划和完成返工。
- `POST /api/batches/{id}/supplier-changes`、`POST /api/batches/{id}/stability`：关联供应链和稳定性记录。
- `POST /api/batches/{id}/decide`：质量决定，携带 `expected_revision` 做当前版本检查；成功返回 `credential`（含冻结修订号与各类清单数量），阻塞时 409 响应带结构化 `blockers` 数组。
- `GET /api/batches/{id}`：详情，含 `credential`（当前有效凭据）、`invalidations`（每次失效的触发动作与逐项原因）、`release_blockers`（当前放行阻塞项）。
- `GET /api/state`：列表，每批次附凭据摘要、最近失效原因与放行阻塞项；页面 `/` 以卡片展示状态徽标、凭据、失效原因与阻塞项。
- `GET /api/health`：健康检查。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

覆盖完整调查/复测/返工放行、关键偏差阻塞、放行后补录/更正导致凭据失效退回待复核、有条件放行凭据取代、双终端并发仅一胜、审计失败整笔回滚、请求编号幂等重放与冲突、结构化阻塞项。

当前为原型：规则以最新检验项目、未关闭偏差和例外有效期为核心，不等同于真实 GMP 质量体系、电子签名、验证或监管提交规范。
