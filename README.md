# 编排极地飞行窗口与任务承诺基础服务

本项目提供极地科考站服务端应用共用的基础能力，用于登记科考机构、站点、操作者和结构化业务资料，并提供角色权限、请求幂等、SQLite 事务与哈希串联审计。具体的物流、样品、能源、医疗和许可业务可在这些边界上扩展自己的状态、规则和接口。

在此基础之上，`polar_flight_ops` 包实现了调度席的**飞行窗口与任务承诺服务**：把机场开放期、机组资质、航程油量、载荷、备降点、气象预报版本、旅客限制和地面保障组合成可比较的候选方案；关键资源先以限时租约保留，运行与站点双方批准后才正式封存；气象修订、飞机故障、部分卸载、紧急医疗插队等扰动只重排尚未执行且确实受影响的航段。

## 目录

- src/polar_station_foundation/：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由和离线验收；
- src/polar_flight_ops/：飞行约束评估与候选生成、限时租约、双方批准封存、扰动重排、候补队列、任务解释、HTTP 路由和离线验收；
- tests/：基础规则、事务边界、接口路由、飞行服务规则和端到端验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

    PYTHONPATH=src python3 -m unittest discover -s tests -v

## 构建检查

    python3 -m compileall -q src tests

## 离线验收

    PYTHONPATH=src python3 -m polar_station_foundation.acceptance
    PYTHONPATH=src python3 -m polar_flight_ops.acceptance

验收命令会在临时 SQLite 数据库中完成一次端到端流程并核对幂等回执与审计链，成功时输出一行 status 为 ok 的 JSON 并以退出码 0 结束。飞行服务验收覆盖：候选方案比较、租约保留、竞争方案冲突、双方批准封存、封存版本冲突、起飞、气象修订重排、重复告警幂等、任务解释和进程恢复延续。

## 飞行窗口与任务承诺服务

### 业务规则

- **候选方案**：为任务枚举飞机 × 窗口 × 机组 × 预报 × 备降 × 地面保障组合，逐项评估九项约束（机场开放期、机组资质、航程油量、载荷、旅客限制、气象最低标准、备降覆盖、地面保障、资源可用性），按可执行性、起飞时刻和耗油排序比较；载荷超出单机运力且任务允许时可拆分为多段。
- **限时租约**：方案创建时对飞机、机组（排他资源）和窗口起降容量、地面保障时段（池化容量）建立限时租约；租约过期未封存则自动释放，方案作废。
- **双方批准封存**：运行（ops_approver）与站点（station_approver）两侧都批准后，调度员才能封存方案；封存把租约转为正式承诺，并以调度版本号做比较并交换——多个调度员同时封存时只有一个版本生效，其余必须基于最新版本重新起草。
- **扰动重排**：气象预报修订、飞机故障、部分卸载、紧急医疗插队只取消尚未执行且确实受影响的航段；已起飞航段与完成的交接永远保留；同一 alert_id 重复送达返回原处理结果，不会再次取消同一承诺。紧急医疗插队会让更低优先级的未执行航段让出资源并进入候补。
- **候补与恢复**：资源不足的任务按优先级和入队次序候补，资源释放时自动转正；全部状态持久化在 SQLite，进程恢复后有效租约、候补次序和待审批版本继续延续。
- **解释**：任一任务都能说明它为何获准起飞、延后、拆分或取消，并逐项列出每次变化释放和重新占用的资源。

### HTTP 接口（/flight-ops 前缀）

- POST /flight-ops/aircraft、/flight-ops/crew、/flight-ops/windows、/flight-ops/weather-forecasts、/flight-ops/ground-support、/flight-ops/missions：登记资源与任务（气象预报按站点自动递增版本并废止旧版）；
- GET /flight-ops/candidates?mission_id=…：生成可比较的候选方案；
- POST /flight-ops/plans：创建方案并建立限时租约（`auto=true` 自动选优，或显式传入候选航段；`waitlist_if_blocked` 不可行时进入候补）；
- POST /flight-ops/plans/{id}/approvals：运行或站点侧审批；
- POST /flight-ops/plans/{id}/seal：双方批准后按期望版本封存；
- POST /flight-ops/legs/{id}/events：记录 depart / arrive / complete_handoff；
- POST /flight-ops/disruptions：提交扰动告警（weather_revision / aircraft_failure / partial_offload / medical_insertion，以 alert_id 幂等）；
- GET /flight-ops/missions/{id}/explanation：任务解释与逐项资源台账；
- GET /flight-ops/waitlist、POST /flight-ops/waitlist/process：候补查询与处理；
- GET /flight-ops/schedule、GET /flight-ops/plans、GET /flight-ops/plans/{id}、GET /flight-ops/missions/{id}：状态查询。

## HTTP 服务

    PYTHONPATH=src python3 -m polar_station_foundation.api --database polar_station.sqlite3 --host 127.0.0.1 --port 8080
    PYTHONPATH=src python3 -m polar_flight_ops.api --database flight_ops.sqlite3 --host 127.0.0.1 --port 8081

健康检查使用 GET /health。写入接口通过 X-Actor-Id 标识操作者，服务重启后 SQLite 中的业务状态和审计历史继续保留。飞行服务端口同时承载基础服务接口（/organizations、/sites 等）。
