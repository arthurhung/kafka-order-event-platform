# Kafka 即時資料處理平台

這是一個可在本機執行的 Kafka 即時事件與應用程式日誌處理平台。目前完成 Phase 1–3：本機 Kafka／
PostgreSQL 基礎環境、Pydantic 事件模型、可調整流量的 Event Generator，以及具備 manual offset
commit、PostgreSQL transaction、idempotency、bounded retry 與 DLQ 的 Order Consumer。

Application Log Consumer、分鐘聚合與 benchmark 尚未實作，會依照 `SPEC.md` 在後續階段加入。

## 前置需求

- 已安裝 pyenv 與 pyenv-virtualenv
- 已建立名為 `kafka_streaming` 的 virtualenv，Python 版本為 3.14.6
- 已安裝 Docker Desktop 或其他支援 Docker Compose 的 Docker 環境
- 本機的 `29092`、`5432`、`8080` ports 尚未被其他程式占用

可先確認 virtualenv 是否存在：

```bash
pyenv versions
```

若尚未建立，可執行：

```bash
pyenv install 3.14.6
pyenv virtualenv 3.14.6 kafka_streaming
```

## 從頭拉起服務

以下指令都在 repository 根目錄執行。

### 1. 確認 Python 環境

Repository 的 `.python-version` 已指定 `kafka_streaming`。進入目錄後，pyenv 應自動切換環境：

```bash
python --version
pyenv version
python -c "import sys; print(sys.executable)"
```

預期結果應包含：

```text
Python 3.14.6
kafka_streaming
.../.pyenv/versions/kafka_streaming/bin/python
```

若沒有自動切換，可確認 shell 已載入 pyenv，然後執行：

```bash
pyenv local kafka_streaming
```

### 2. 建立本機環境設定

```bash
cp .env.example .env
```

`.env` 只供本機使用，不會提交至 Git。預設設定會使用：

- Kafka：`localhost:29092`
- PostgreSQL：`localhost:5432`
- Kafka UI：`localhost:8080`
- PostgreSQL database：`streaming`
- PostgreSQL user：`streaming`

如需調整 ports 或開發用密碼，請在啟動服務前修改 `.env`。

### 3. 安裝 Python 相依套件

```bash
python -m pip install -e . --group dev
```

依賴會安裝到 `kafka_streaming` virtualenv，不需要另外建立 `.venv`。

### 4. 啟動 Kafka、PostgreSQL 與 Kafka UI

建議使用：

```bash
make up
```

此指令等同啟動三個 Compose services，並等待 Kafka 與 PostgreSQL 的 TCP endpoint 可連線：

```bash
docker compose up -d kafka postgres kafka-ui
python scripts/wait_for_services.py
```

查看服務狀態：

```bash
docker compose ps
```

Kafka 與 PostgreSQL 應顯示 `healthy`，Kafka UI 應顯示 `Up`。

### 5. 建立 Kafka topics

```bash
make topics
```

此指令可以安全重複執行，只會建立尚不存在的 topics：

| Topic | Partitions | Replication factor | 後續階段使用的 key |
|---|---:|---:|---|
| `ecommerce.orders.raw.v1` | 6 | 1 | `order_id` |
| `ecommerce.application-logs.raw.v1` | 6 | 1 | `service` |
| `ecommerce.dlq.v1` | 3 | 1 | `event_id` |

列出並檢查 topics（Kafka image 內部 CLI 路徑已封裝於 Makefile）：

```bash
make list-topics
make describe-topics
```

執行實際 produce / consume 並驗證 Kafka key round-trip：

```bash
make smoke-kafka
```

### 6. 執行 PostgreSQL migration

```bash
make migrate
```

Migration 會建立：

- `valid_orders`
- `processed_events`
- `log_metrics_minute`
- `alembic_version`

確認資料表：

```bash
docker compose exec postgres \
  psql -U streaming -d streaming \
  -c "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;"
```

Migration 可以重複執行；已套用最新 revision 時不會重建資料表。

### 7. 開啟 Kafka UI

瀏覽器開啟：

<http://localhost:8080>

介面中應可看到 `local-kraft` cluster 與前一步建立的三個 topics。

### 8. 執行品質檢查

```bash
make lint
make typecheck
make test
```

Unit tests 涵蓋 Phase 1–3；integration 與 E2E tests 使用真實 Kafka/PostgreSQL 驗證 Producer、DB
transaction、manual offset、duplicate、DLQ 與 restart replay。

## Phase 2 Event Generator

CLI 可直接指定 EPS、持續時間、Order／Log 比例，以及互斥的 invalid、stale、duplicate 注入比例：

```bash
python -m apps.event_generator \
  --events-per-second 1000 \
  --duration-seconds 60 \
  --order-ratio 0.2 \
  --log-ratio 0.8 \
  --invalid-rate 0.02 \
  --stale-rate 0.01 \
  --stale-hours 168 \
  --duplicate-rate 0.05 \
  --seed 42 \
  --report-path reports/latest.json
```

Order event 使用 `order_id` 作為 Kafka key；application log 使用 `service`。stale event 保持
schema-valid，只將 aware `event_time` 往前移動；invalid、stale、duplicate 會在 JSON report 中分開
統計。Producer 會定期 poll delivery callback、在本機 queue full 時 bounded backoff，並於結束前
flush。

Log Generator 會從 `order-api`、`payment-api`、`user-api`、`inventory-api`、
`notification-api`、`gateway-api`、`catalog-api`、`auth-api`、`shipping-api` 隨機選擇 service。
Producer 不寫死 partition；相同 service key 由 Kafka client 穩定分配到相同 partition，不同 key 仍可能
因 hash collision 落在同一 partition。

## Phase 3 Order Consumer

```bash
make up
make topics
make migrate
make order-consumer
```

Order Consumer 使用 `.env` 中的 `KAFKA_ORDER_CONSUMER_GROUP`，範例值固定為
`order-processing-group-v1`。Kafka automatic commit 與 automatic offset store 均關閉。

合法事件會在同一個 PostgreSQL transaction 內寫入 `processed_events` 與 `valid_orders`，DB commit
完成後才同步提交 Kafka offset。重複 `event_id` 由 `(consumer_group, event_id)` primary key 原子判斷，
不會再次寫入 `valid_orders`。

JSON decode 或 Pydantic validation error 會寫入 `ecommerce.dlq.v1`；只有 delivery callback 確認
DLQ delivery 後才提交原訊息 offset。PostgreSQL 與暫時性 Kafka 錯誤預設最多 retry 3 次，等待
1、2、4 秒。若仍失敗，Consumer 不提交 offset 並以非零狀態停止。

此設計是 at-least-once processing 加 idempotent consumer，不是 Kafka 與 PostgreSQL 之間的
exactly-once transaction。

常用 preset：

```bash
make generate-smoke
make inject-bad-events
```

## 日常開發指令

```bash
make up          # 啟動基礎服務並等待 endpoint
make down        # 停止服務，保留 Kafka 與 PostgreSQL volumes
make restart     # 重新啟動服務
make logs        # 持續顯示所有服務 logs
make topics      # 建立缺少的 Kafka topics
make list-topics # 列出 Kafka topics
make describe-topics # 檢查 topic partitions 與 replication factor
make smoke-kafka # 實際 produce / consume 並驗證 key
make migrate     # 套用 Alembic migrations
make lint        # 執行 Ruff
make format      # 執行 Ruff formatter
make typecheck   # 執行 mypy
make test        # 執行 pytest
make test-integration # 使用真實 Kafka/PostgreSQL 執行 integration tests
make test-e2e      # 執行 Phase 3 executable flow
make order-consumer # 啟動 Phase 3 Order Consumer
make generate-smoke # 100 EPS、60 秒的混合事件
make inject-bad-events # 產生 schema-invalid 測試事件
```

只查看特定服務的 logs：

```bash
docker compose logs -f kafka
docker compose logs -f postgres
docker compose logs -f kafka-ui
```

## 停止服務

保留 Kafka 與 PostgreSQL 資料：

```bash
make down
```

若確定要連同所有本機 Kafka message、topic metadata 與 PostgreSQL 資料一起清除，可執行：

```bash
docker compose down -v
```

`-v` 會刪除本專案的 Docker volumes，資料無法由 Compose 自動復原，請勿在需要保留資料時使用。

## 設計與可靠性限制

- Kafka 使用單節點 combined KRaft mode，不使用 ZooKeeper。
- Broker 已關閉自動建立 topic，topics 必須透過 `make topics` 建立。
- 本機 Kafka 使用 plaintext connection。
- PostgreSQL 使用本機開發憑證。
- Replication factor 為 1，沒有 broker failure redundancy。
- 單節點環境的 `acks=all` 不代表具備多副本耐久性。
- 本專案不宣稱 Kafka 與 PostgreSQL 之間具有 exactly-once delivery。
- 此環境只供本機開發與展示，不是 production-ready 設定。

架構與設計細節請參考：

- [Phase 1 架構](docs/architecture.md)
- [Kafka 設計](docs/kafka-design.md)
- [可靠性說明](docs/reliability.md)
- [完整實作規格](SPEC.md)

## 後續階段

後續會依序加入：

1. Phase 4：Log Consumer、每分鐘 aggregation 與 PostgreSQL upsert
2. Phase 5：benchmark、consumer lag、完整 demo 與實測報告

README 不會在完成實測前加入虛構的效能數字。
