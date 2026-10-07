# 编排极地飞行窗口与任务承诺基础服务

本项目提供极地科考站服务端应用共用的基础能力，用于登记科考机构、站点、操作者和结构化业务资料，并提供角色权限、请求幂等、SQLite 事务与哈希串联审计。在此之上，`dispatch` 子模块为内陆营地轮换提供**飞行窗口与任务承诺服务**：

- **可比较候选**：把机场开放期、机组资质、航程油量、载荷、备降点、气象预报版本、旅客限制和地面保障组合为候选，逐项给出满足/警告/违反因子与评分；
- **限时租约 + 双方封存**：关键资源（飞机、机组、窗口、地面、油量）先以带 TTL 的软租约保留，运行与站点双方批准后唯一版本才封存为承诺；
- **受影响才重排**：气象修订、飞机故障、部分卸载、医疗插队只释放尚未起飞且确实受影响的航段；已起飞与已完成交接继续保留，重复告警不重复生效，部分卸载会就地交接并拆出后继任务；
- **并发与恢复**：全部写入走 SQLite IMMEDIATE 事务，封存用条件更新保证唯一生效版本；租约、候补次序、待审批版本均持久化，进程重启后延续；
- **可解释**：任务解释接口说明获准起飞、延后或拆分的原因，并逐条列出每次资源释放与重新占用的台账。

## 目录

- src/polar_station_foundation/：领域模型、SQLite 存储、权限服务、审计链、HTTP 路由、飞行调度服务和离线验收；
- tests/：基础规则、事务边界、接口路由、调度场景和端到端验收测试。

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
    PYTHONPATH=src python3 -m polar_station_foundation.dispatch_acceptance

验收命令会在临时 SQLite 数据库中登记科考机构、操作者、站点和业务资料，核对幂等回执与审计链，成功时输出一行 status 为 ok 的 JSON 并以退出码 0 结束。调度验收额外覆盖候选比较、限时租约、双方封存、告警重排、医疗插队、进程恢复与资源台账。

## HTTP 服务

    PYTHONPATH=src python3 -m polar_station_foundation.api --database polar_station.sqlite3 --host 127.0.0.1 --port 8080

健康检查使用 GET /health。写入接口通过 X-Actor-Id 标识操作者，服务重启后 SQLite 中的业务状态和审计历史继续保留。

### 调度接口（/dispatch）

- `POST /dispatch/aircraft|airfields|airfield-windows|crew|distances|fuel-stocks|forecasts`：登记飞机、机场、开放窗口、机组、航程、油料库存与预报版本；
- `POST /dispatch/missions`：登记 medevac / calibration / supply 任务；
- `GET  /dispatch/missions/<id>/candidates`：返回按可行性与评分排序的候选，每个候选带八类因子明细；
- `POST /dispatch/missions/<id>/reserve`：以限时租约保留最佳（或指定）候选，资源繁忙时返回阻塞者并入候补；
- `POST /dispatch/plans/<id>/approvals`：`party=operations|station` 双方批准，齐备即唯一封存；
- `POST /dispatch/plans/<id>/rejections`：驳回并释放租约；
- `POST /dispatch/commitments/<id>/departures|handovers`：标记起飞、完成交接；
- `POST /dispatch/alerts`：weather_revision / aircraft_fault / partial_unload / medevac_preempt，按 alert_key 幂等去重；
- `GET  /dispatch/missions/<id>/explanation`：获准/延后/拆分的决策解释与逐次资源释放、重占台账；
- `GET  /dispatch/waitlist`、`GET  /dispatch/leases`：候补次序与租约视图。
