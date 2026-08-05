# Kafka 即時訂單事件與應用日誌平台

這是一個可在本機重複執行的 Kafka MVP，用來展示事件產生、manual offset commit、PostgreSQL
transaction、idempotent consumer、bounded retry、DLQ、每分鐘日誌聚合、Consumer Lag、故障恢復與實測
benchmark。它是一個作品集與面試展示專案，不是 production-ready 系統。

## 解決的問題

平台同時處理兩類資料：需要逐筆可靠落地的訂單事件，以及需要依 event time 聚合的 API access logs。
Kafka offset 與 PostgreSQL transaction 分屬不同系統；本專案以 at-least-once processing、manual
commit 與資料庫 idempotency 安全處理 replay，不宣稱跨 Kafka/PostgreSQL exactly-once。

## 架構

```mermaid
flowchart LR
    G["Event Generator<br/>EPS / duration / mix"]
    OT["ecommerce.orders.raw.v1<br/>6 partitions / key=order_id"]
    LT["ecommerce.application-logs.raw.v1<br/>6 partitions / key=service"]
    OC["Order Consumer<br/>order-processing-group-v1"]
    LC["Log Consumer<br/>application-log-processing-group-v1"]
    DLQ["ecommerce.dlq.v1<br/>3 partitions"]
    DB[("PostgreSQL 16")]
    R["Benchmark / Demo JSON Reports"]

    G --> OT --> OC
    G --> LT --> LC
    OC --> DB
    LC --> DB
    OC --> DLQ
    LC --> DLQ
    G --> R
    OC --> R
    LC --> R
```

## 技術棧

- Python 3.14.6、pyenv virtualenv `kafka_streaming`
- Apache Kafka 4.1.0，單節點 combined KRaft mode
- PostgreSQL 16、Kafka UI
- confluent-kafka、Pydantic 2、pydantic-settings
- SQLAlchemy 2、Alembic、psycopg
- pytest、Ruff、mypy、Docker Compose

## Repository structure

```text
apps/                  executable composition roots
src/streaming_platform reusable models, Kafka/DB services, consumers and benchmark logic
migrations/            Alembic schema migration
scripts/               topic bootstrap, readiness, lag and Kafka smoke tools
tests/unit/             isolated deterministic logic
tests/integration/      real Kafka/PostgreSQL integration behavior
tests/e2e/              executable process and recovery flows
docs/                   architecture, reliability, benchmark and demo details
reports/runs/           timestamped machine-readable reports
```

## Topic、Partition 與 Message Key

| Topic | Partitions | RF | Key | 用途 |
|---|---:|---:|---|---|
| `ecommerce.orders.raw.v1` | 6 | 1 | `order_id` | 訂單與付款事件 |
| `ecommerce.application-logs.raw.v1` | 6 | 1 | `service` | API 與應用程式日誌 |
| `ecommerce.dlq.v1` | 3 | 1 | `event_id`，無法解析時使用source coordinate | 永久錯誤 |

6個source partitions讓consumer group最多有6個有效並行instances。Order key保留同一order在同一partition
內的順序；Log key讓同一service穩定路由。Kafka只保證單一partition內的順序。RF=1符合本機單broker，
不提供正式環境高可用。

Consumer groups固定為：

- `order-processing-group-v1`
- `application-log-processing-group-v1`

## Event schema

每個事件都有相同envelope：

```json
{
  "event_id": "f2a89c11-5ef2-41df-a337-3c44902b2340",
  "event_type": "order_created",
  "event_version": 1,
  "event_time": "2026-08-05T10:00:00Z",
  "source": "order-api",
  "payload": {
    "order_id": "ORD-100001",
    "user_id": "USR-1001",
    "product_id": "PRD-501",
    "quantity": 2,
    "amount": "1800.00",
    "currency": "TWD",
    "channel": "web"
  }
}
```

支援4種Order events與2種Log events，各自使用獨立Pydantic payload model。Datetime必須timezone-aware並
正規化為UTC；money使用`Decimal`。Duplicate injection故意重用原event ID，新benchmark run預設由
run ID導出不同seed，避免跨run ID collision。

## PostgreSQL model

- `valid_orders`：合法Order business rows，包含Kafka source coordinates。
- `processed_events`：`(consumer_group, event_id)` composite primary key，也是idempotency marker。
- `log_metrics_minute`：`(metric_minute, service, endpoint)` minute aggregate。

`processed_events`與business write在同一transaction。Log average不另存欄位，而由
`response_time_sum_ms / request_count`查詢時計算。

## 環境需求與Python設定

- pyenv、pyenv-virtualenv
- Python 3.14.6 virtualenv `kafka_streaming`
- Docker Desktop或相容Docker Compose環境
- 可用ports：29092、5432、8080

```bash
pyenv install 3.14.6
pyenv virtualenv 3.14.6 kafka_streaming
pyenv local kafka_streaming
python --version
pyenv version
python -c "import sys; print(sys.executable)"
python -m pip install -e . --group dev
cp .env.example .env
```

Repository的`.python-version`已指定`kafka_streaming`；共享程式碼沒有寫死使用者的Python路徑。

## 快速啟動

```bash
make up
make topics
make migrate
make order-consumer   # terminal A
make log-consumer     # terminal B
make generate-smoke  # terminal C
make consumer-lag
```

Kafka UI位於 <http://localhost:8080>。`make topics`與`make migrate`都可安全重跑。

## Makefile commands

| Command | 用途 |
|---|---|
| `make up/down/restart/logs` | 管理本機infra；`down`保留volumes |
| `make topics/list-topics/describe-topics` | 建立與檢查topics |
| `make smoke-kafka` | 真實Kafka key/value round trip |
| `make migrate` | Alembic upgrade |
| `make order-consumer/log-consumer` | 啟動正式consumers |
| `make generate-smoke/standard/stress` | Producer-only presets |
| `make inject-bad-events` | 產生schema-invalid records |
| `make consumer-lag` | Human-readable真實Kafka lag |
| `make consumer-lag FORMAT=json` | Machine-readable lag |
| `make benchmark` | Smoke benchmark |
| `make benchmark-standard/benchmark-stress` | Standard／Stress profile |
| `make demo` | Mixed、DLQ、lag recovery與uncommitted replay |
| `make lint/typecheck/test` | 品質與完整tests |

## Demo

`make demo`不會清表、reset offsets或刪除volumes，因此可從既有環境執行，也能在使用者明確執行
`docker compose down -v`後從乾淨狀態開始。流程包含：

```text
readiness → topics → migration → consumers → mixed events
→ DB/DLQ/lag驗證 → stop consumers → produce while stopped
→ lag rises → restart → lag returns to zero
→ deterministic DB-commit-before-offset-commit replay → cleanup → JSON report
```

Failure demo中的SIGTERM是「真實consumer process停止」，uncommitted window則是刻意省略Kafka commit的
可重現simulation。它們不是Kafka/PostgreSQL container failure。

## Consumer Lag

`make consumer-lag`使用confluent-kafka API讀取真實committed offset與partition log-end offset，而不是
使用DB row count。每個partition顯示：

```text
Consumer Group / Topic / Partition / Current Offset / Log End Offset / Lag / Status
```

Missing group或尚未commit的partition顯示`not_available`；topic不存在或Kafka無法連線時回傳非0。
Lag不會被clamp成假的0。

## Benchmark方法

| Profile | 原始目標 |
|---|---:|
| Smoke | 100 EPS / 60 seconds |
| Standard | 1,000 EPS / 300 seconds |
| Stress | 5,000 EPS / 300 seconds |

CLI與`BENCHMARK_*`環境變數可覆寫EPS、duration、order/log ratio、application error ratio、invalid、
duplicate、seed、poll interval與timeout。Benchmark預設使用20% Order、80% `api_access_log`、0% error
log／invalid／duplicate，量測主要persistence與aggregation路徑；`make demo`另外量測mixed DLQ與duplicate。

每次run使用唯一run ID和filename。Producer callback保存本次精確`topic/partition/offset` intervals；DB與
DLQ統計以這些coordinates隔離，不使用全表差值。既有backlog必須先在bounded timeout內排空，runner
不會reset consumer offsets。

Latency正式命名為`producer_delivery_latency_ms`：從第一次local produce attempt至成功Kafka delivery
callback，包含local queue backoff。這是producer-side acknowledgement latency，不是end-to-end latency。
P95/P99採nearest-rank。`end_to_end_latency_ms`目前為`not_implemented`且數值為`null`。

詳細定義與report schema見[Benchmark文件](docs/benchmark.md)。

## 實際Benchmark結果

量測環境：macOS 24.6.0、Intel Core i9-9880H、16 logical cores、64 GiB host memory、Docker Desktop
29.6.1（VM可見16 CPUs、約15.6 GiB memory）、Python 3.14.6、Kafka 4.1.0、PostgreSQL 16。
Docker Desktop UI設定的資源上限無法由CLI可靠辨認，report標示`not_available`。

| Run | Produced | Actual EPS | Delivery avg / P95 / P99 | Observed max / final lag | Durable runtime | Status |
|---|---:|---:|---:|---:|---:|---|
| Smoke | 6,000 | 99.99 | 10.07 / 11.86 / 12.31 ms | 90 / 0 | 61.58 s | passed |
| Standard | 300,000 | 999.55 | 5.78 / 7.54 / 8.22 ms | 34,381 / 0 | 697.52 s | passed |
| Stress original | 1,500,000 | 4,983.86 | 4.64 / 6.67 / 7.85 ms | unavailable in failed run | not completed | failed |
| Stress adjusted (1,000 EPS / 60s) | 60,000 | 999.67 | 5.37 / 7.15 / 8.07 ms | 7,886 / 0 | 142.73 s | passed |

原始Stress的Producer全部delivery成功，但120秒drain後只commit 1,209,387/1,500,000 records，因此報告
正確標示failed。這顯示本機單一Order/Log consumer的durable capacity遠低於Producer delivery rate；不把
4,983.86 EPS解讀成end-to-end容量。根據該失敗與Standard backlog，降載版保留Stress profile原始目標，將
effective workload明確改為1,000 EPS／60秒並成功完成；理由與兩組參數都保存在report。

Evidence reports：

- [Smoke report](reports/runs/benchmark-smoke-20260805T094857116796Z-39268c85-e8f3-485d-ae3c-087b407499ac.json)
- [Standard report](reports/runs/benchmark-standard-20260805T095019746379Z-456dade3-07c5-4994-b1a1-7f9ad3c37b96.json)
- [Original Stress failed report](reports/runs/benchmark-stress-20260805T100330400923Z-e27d4bce-7675-4208-bf10-da205e87e054.json)
- [Adjusted Stress passed report](reports/runs/benchmark-stress-20260805T102059023444Z-8d8642fe-0e3e-4ee4-ad14-16c70ab91db4.json)

這些本機數據只描述本次環境與workload，不能外推為production capacity。

## Reliability語意

### Manual offset commit與transaction boundary

Consumers關閉automatic commit與automatic offset storage。Order流程為DB transaction成功後同步commit
Kafka；Log流程只對已durable且per-partition contiguous的next offset commit。DB失敗不commit。

### At-least-once與idempotent consumer

DB成功但Kafka commit前停止時，record會replay。`processed_events` marker讓相同event ID不會再次寫入
business data或累加minute metrics。這是at-least-once加idempotency，不是distributed exactly-once。

### Retry與DLQ

暫時性DB/Kafka錯誤最多retry 3次，預設backoff 1、2、4秒。JSON/Pydantic/unsupported type等永久錯誤
送入DLQ；只有DLQ delivery callback確認後才允許source offset前進。Retry exhausted會非0停止，不吞錯。

### Log minute aggregation與late event

`api_access_log`依UTC `event_time`截斷到分鐘，以`(minute, service, endpoint)`聚合。Buffer定期snapshot/
flush並additive upsert。Late event更新它自己的舊分鐘。`application_error_log`目前不符合HTTP minute metric
欄位要求，會以`UnsupportedEventType`進DLQ。

### Graceful shutdown

Order Consumer完成目前critical section再退出；Log Consumer在退出前flush buffer與安全offset；Producer
flush pending deliveries。Phase 5 manager先SIGTERM，bounded wait後才SIGKILL，forced cleanup會使run失敗。

## 測試策略

- Unit：schema、Decimal、UTC、factory、profile、percentile、report、lag、timeout、cleanup、retry、DLQ、
  aggregation與offset tracker。
- Integration：使用真實Kafka/PostgreSQL驗證delivery、lag rise/recovery、DB transaction、DLQ、duplicate、
  commit failure與aggregation。
- E2E：啟動executable consumers，驗證mixed flow、shutdown flush、uncommitted replay、完整Demo report與
  無orphan process。

```bash
make lint
make typecheck
make test-unit
make test-integration
make test-e2e
make test
```

## 已知限制與production recommendations

- 單節點combined KRaft、RF=1，沒有broker/controller failover。
- `acks=all`只代表唯一副本確認，不是多副本durability。
- 本機plaintext Kafka與開發憑證只適合local development。
- 沒有TLS、ACL、secrets manager、backup/HA、capacity alerting或跨機房設計。
- Producer delivery latency不是end-to-end latency。
- Consumer Lag的maximum是polling interval內觀察到的maximum。
- 本Demo沒有模擬broker crash、network partition或disk failure。
- 正式環境應另外評估multi-broker replication、security、PostgreSQL HA/backup、監控告警與容量規劃；
  這些不在本MVP實作範圍。

## Roadmap

Post-MVP可研究CDC/outbox、stream processing、OLAP、observability與Kafka HA，但本repository目前沒有加入
Flink、Spark、Debezium、Kafka Connect、Schema Registry、ClickHouse、Prometheus、Grafana、Kubernetes、
Terraform或cloud infrastructure。

## 面試展示重點

1. 說明為何key只能保證單partition順序。
2. 畫出DB commit與Kafka commit間的crash window。
3. 展示`processed_events`如何阻止replay重複寫入。
4. 停止consumer後用`make consumer-lag`觀察lag上升與恢復。
5. 比較Producer actual EPS與durable runtime，說明不能把delivery throughput當成end-to-end capacity。
6. 展示failed Stress JSON如何誠實保存timeout與未完成狀態。

更完整設計請見[Architecture](docs/architecture.md)、[Kafka Design](docs/kafka-design.md)、
[Reliability](docs/reliability.md)、[Benchmark](docs/benchmark.md)與[Demo](docs/demo.md)。
