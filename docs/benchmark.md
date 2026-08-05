# Benchmark方法與指標

## Workloads

| Profile | Target EPS | Duration | Default mix |
|---|---:|---:|---|
| Smoke | 100 | 60s | 20% Order / 80% API access log |
| Standard | 1,000 | 300s | 20% Order / 80% API access log |
| Stress | 5,000 | 300s | 20% Order / 80% API access log |

Benchmark預設`application_error_ratio=0`、`invalid_rate=0`、`duplicate_rate=0`，量測主要DB persistence與
minute aggregation路徑。Demo使用獨立mixed workload驗證error log、invalid、duplicate與DLQ。所有參數可由
CLI或`BENCHMARK_*`環境變數覆寫；CLI優先於environment，environment優先於profile。

Stress的Producer timeout為duration加120秒，Producer完成後drain timeout為120秒。下游若未在期限內commit
全部run records，report為failed。降低EPS或duration時，report仍保留5,000/300的profile target與effective
parameters。

## 執行程序

```text
infra readiness → topic bootstrap → migration
→ start consumers → drain pre-existing backlog
→ capture DLQ tail → run existing Event Generator
→ poll real Kafka lag → wait for source offsets to commit
→ match DLQ source coordinates → query run-scoped DB rows
→ graceful process cleanup → UTF-8 JSON report
```

每個等待都有deadline。Runner不reset offsets、不清DB，也不以固定sleep宣稱處理完成。

## Run isolation

每次run的Producer callback保存成功delivery的精確`topic/partition/offset` intervals。Run-scoped queries使用：

- `processed_events(topic, partition_id, offset_id)`
- `valid_orders(kafka_topic, kafka_partition, kafka_offset)`
- DLQ的`original_topic/original_partition/original_offset`

這避免歷史資料混入。`log_metrics_minute`是跨run additive table，所以report只把它的row count標為
`whole_table`；本次log處理量來自run-scoped `processed_events`。

未指定seed時，Benchmark以run ID導出不同seed，避免deterministic generator跨run重用event ID。明確指定seed
可重現事件選擇，但固定consumer group會把已處理event ID視為duplicate。

## Metric dictionary

| Metric | Definition | Source | Unit |
|---|---|---|---|
| target EPS | effective requested rate | config | events/s |
| actual EPS | delivered / Producer monotonic elapsed | callback tracker | events/s |
| attempted | records entering send path | Generator | records |
| delivered | successful Kafka callback | Producer | records |
| failed | callback error, queue exhaustion or unconfirmed flush | Producer | records |
| consumed count | run offsets below real committed next offsets | Kafka | records |
| duplicate count | delivered duplicate-injected records | Generator/callback | records |
| DLQ count | DLQ originals matching run source coordinates | Kafka | records |
| observed max lag | maximum complete combined lag sample | Kafka | records |
| final lag | last complete combined lag | Kafka | records |
| durable runtime | generation start through all run offsets committed | monotonic clock | seconds |
| restart recovery | replacement start through run offsets committed | Demo | seconds |

Missing offset、group或metadata不會變成0；使用`null`與status/reason。

## Latency

`producer_delivery_latency_ms`起點為第一次local produce attempt前，終點為成功delivery callback。Queue full
backoff包含在內，failed deliveries不進sample。它是Producer-side acknowledgement latency，不是DB durable
latency。

Average為arithmetic mean。P95/P99使用nearest-rank：排序後取`ceil(p*n)-1`。空sample全部為null，單一
sample直接回傳該值，不插值。

目前`end_to_end_latency_ms.status=not_implemented`，因為現有schema沒有能一致涵蓋Order、buffered Log、
duplicate與DLQ的單一durable endpoint timestamp。

## Report schema

```text
schema_version / run_id / profile / started_at / finished_at
status / failure_stage / exit_code
environment / configuration / producer / consumer / latency
runtime / database / run_scope / errors / limitations / artifacts
```

Datetime為UTC ISO 8601，JSON為UTF-8。`passed`表示workload與required metrics完成；`failed`表示stage、delivery、
timeout或cleanup失敗；`partial`表示workload完成但required metric無法可靠取得。失敗也盡可能寫report。

Report只收集OS、CPU、memory、Python、Docker版本/可見資源與image tags白名單，不dump environment、password、
token或connection string。Docker Desktop UI resource limits若CLI無法辨識則為`not_available`。

## 實際結果與限制

詳細數字和evidence links在README。2026-08-05量測中Smoke與Standard完整通過；原始Stress Producer成功
delivery 150萬筆，但bounded drain只commit 1,209,387筆，因此failed。結果只描述本機單節點、單一consumer
instance，不代表production capacity。

依原始失敗與Standard量測，降載Stress保留profile target 5,000 EPS／300秒，但effective parameters改為
1,000 EPS／60秒。它delivery/consume 60,000筆、actual 999.67 EPS、observed max lag 7,886、final lag 0，
durable runtime 142.73秒。降低原因、original target與effective configuration都保存在JSON。

Observed max lag是polling samples的最大值，可能低於兩次poll之間的瞬間峰值。Producer EPS不可解讀成
end-to-end DB throughput。

## Repeatability

```bash
make benchmark
make benchmark-standard
make benchmark-stress

python -m apps.benchmark --profile stress \
  --events-per-second 1000 --duration-seconds 60 --timeout 300
```

重跑前不需清資料；runner先等待既有lag歸零。不要同時對固定consumer groups啟動多個benchmark。
