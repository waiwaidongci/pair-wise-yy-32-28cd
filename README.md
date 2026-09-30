# 药品生产偏差与批次放行系统

Python 标准库 + SQLite。批次可关联关键/一般偏差、检验复测、返工、供应商变更和稳定性数据。质量人员可以拒绝、再取样、有条件放行或正式放行；关键偏差始终阻止正式放行，修改必须携带当前批次修订号。

## 放行凭据冻结与失效

质量人员签发 `release` / `conditional` 放行凭据时，会**冻结**当时的批次修订号及偏差、检验、返工、供应商变更和稳定性清单（`basis_json`）。此后任一依据发生补录或更正（新增/关闭偏差、例外批准、补录检验、计划/完成返工、供应商变更、稳定性记录），系统都会：

1. 作废已签发的放行凭据（`status` 由 `active` 置为 `invalid`）；
2. 把批次状态退回 `pending_review`（待复核）；
3. 在凭据的 `invalid_reasons` 中逐条记录失效原因。

只有重新签发新凭据后批次才能再次放行发运。`reject` / `resample` 不属于放行凭据，不冻结、不失效。

## 并发与重试

- 所有修改携带 `expected_revision`（批次修订号），两个终端同时提交同一批次时只接受当前版本，旧版本返回 `409`。
- 签发请求携带 `request_no`（请求编号）。审计写入失败会整笔回滚（凭据与批次状态都不持久化）；重试时沿用同一 `request_no`，服务端幂等返回已签发凭据，不会重复签发。凭据已失效后沿用旧编号重试会被拒绝，要求换新编号。

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
- `POST /api/batches/{id}/decide`：质量决定，支持并发修订号检查与 `request_no` 幂等；不满足放行条件时返回 `409` 并在 `blockers` 中列出全部阻塞项。
- `GET /api/batches/{id}/release-check?decision=release`：预检出放行阻塞项，不写入。
- `GET /api/batches/{id}`、`GET /api/state`、`GET /api/health`：详情（含 `active_credential` 凭据状态、冻结清单与失效原因）、状态和健康检查。

页面（`/`）列出各批次的凭据状态、失效原因与放行阻塞项，并提供签发/复核表单。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

当前为原型：规则以最新检验项目、未关闭偏差和例外有效期为核心，不等同于真实 GMP 质量体系、电子签名、验证或监管提交规范。
