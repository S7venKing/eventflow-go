# TESTREADME — Hướng dẫn test Outbox Worker (Step 12: Failure Injection & Recovery)

Tài liệu này hướng dẫn tự chạy toàn bộ test cho outbox worker: retry khi publish
fail, permanent failure → CLOSE, worker crash → reclaim, và concurrent reclaim.

## 0. Chuẩn bị

Chỉ cần Postgres đang chạy (không cần API):

```bash
docker compose up -d postgres
```

- Test tự resolve connection theo thứ tự: `TEST_DATABASE_URL` → `DATABASE_URL`
  (kể cả trong `.env`) → default `postgres://eventflow:eventflow@localhost:5452/eventflow`.
- Mỗi integration test tạo database tạm `eventflow_conc_<timestamp>` rồi tự xóa,
  **không đụng vào database dev**.
- Không có Postgres thì integration test tự **skip** (không fail).

## 1. Chạy tất cả

```bash
go test ./... -v
```

Chạy hết mất khoảng 1–2 phút khi có Postgres (test transient failure 1000 events
chạy 2 lần cho rate 10% và 30%).

## 2. Từng test — cái gì được chứng minh

Tất cả nằm trong `internal/event/application/`. Chạy riêng từng cái bằng:

```bash
go test ./internal/event/application/ -run '<TÊN_TEST>' -v
```

| Test | Part | Chứng minh |
|------|------|-----------|
| `TestLoadOutbox.*` + `TestOutboxConfigValidate` (gói `internal/config`) | — | Parse/validate `OUTBOX_STALE_TIMEOUT`, `PUBLISH_FAILURE_RATE` (không cần DB) |
| `TestOutboxWorkersConcurrentNoDuplicateNoLoss` | baseline | 4 workers, 1000 events: exactly-once, không mất event |
| `TestOutboxWorkersGracefulShutdownLosesNothing` | baseline | Shutdown giữa chừng không mất event, không kẹt PROCESSING |
| `TestOutboxWorkersTransientFailuresRetryUntilPublished` | A | Fail 10% / 30% mỗi attempt → retry theo backoff → cuối cùng **published = 1000**, `SUM(attempts)` = đúng số lần fail, `last_error` được ghi |
| `TestOutboxWorkersPermanentFailuresEndInClose` | B | 20 event poison fail mãi → đúng 4 attempts/event → **CLOSE**; 200 event thường vẫn PUBLISHED, queue không bị block |
| `TestOutboxRepositoryReclaimStale` | C | `ClaimPending` stamp `processing_at`; `ReclaimStale` chỉ nhặt row quá stale timeout, trả về PENDING, không đổi `attempts`, claim lại được ngay |
| `TestOutboxWorkerCrashIsRecoveredByReclaim` | C + D | **Crash thật**: worker bị force-cancel giữa lúc publish (không MarkFailed, không cleanup) → 10 row kẹt PROCESSING → quá stale timeout → worker khác reclaim → tất cả PUBLISHED đúng 1 lần |
| `TestReclaimStaleConcurrentCallersReclaimEachRowOnce` | E | 4 goroutine cùng gọi `ReclaimStale`: tổng reclaim = đúng số row stale, mỗi row về tay đúng 1 caller |
| `TestOutboxWorkersReclaimStaleWhileProcessing` | E | 4 workers vừa claim/publish vừa reclaim mỗi tick, đua với nhau: vẫn exactly-once, không corrupt state |

Lưu ý Part D: crash được giả lập bằng cách để publisher treo rồi force-cancel
context in-flight sau shutdown timeout (tương đương `kill -9` giữa batch) —
**không** gọi `MarkFailed`, không có cleanup nào chạy. Test assert worker trả về
`ErrShutdownTimeout` để chắc chắn đi đúng đường hard-kill.

## 3. Failure matrix bằng outboxbench (chạy trên DB compose thật)

`cmd/outboxbench` dùng chính `OutboxWorker` + `OutboxRepository` production.
Mỗi run tự truncate + seed lại, nên các scenario không dùng chung events.

```bash
# 1. Normal — 0% failure
go run ./cmd/outboxbench -workers 4 -batch 10 -events 1000

# 2. Transient 10%
go run ./cmd/outboxbench -workers 4 -batch 10 -events 1000 -failure-rate 0.10

# 3. Transient 30%
go run ./cmd/outboxbench -workers 4 -batch 10 -events 1000 -failure-rate 0.30

# 4. Permanent failure — 50/1000 events poison, phải kết thúc ở CLOSE
go run ./cmd/outboxbench -workers 4 -batch 10 -events 1000 -poison 50

# 5+6. Crash/reclaim và concurrent reclaim: chạy bằng go test (mục 2),
# vì bench không tự kill được worker giữa chừng:
go test ./internal/event/application/ -run 'TestOutboxWorkerCrashIsRecoveredByReclaim|TestReclaimStale|TestOutboxWorkersReclaimStaleWhileProcessing' -v
```

Thêm `-out benchmark/results-failure-matrix.md` để gom bảng kết quả.
Transient failure dùng retry delay production (base 1s) nên run 10%/30% sẽ chậm
hơn run normal — đó là retry hoạt động, không phải bug.

### Kết quả kỳ vọng

| Scenario | published | failed (CLOSE) | pending | processing | duplicate | lost |
|----------|-----------|----------------|---------|------------|-----------|------|
| Normal | 1000 | 0 | 0 | 0 | 0 | 0 |
| 10% transient | 1000 | 0 | 0 | 0 | 0 | 0 |
| 30% transient | 1000 | 0 | 0 | 0 | 0 | 0 |
| Poison 50 | 950 | 50 | 0 | 0 | 0 | 0 |

(Bench cap số lần fail mỗi event = 3 = retry budget, nên transient run luôn hội
tụ về published đủ; `injected_failures` trong report cho biết đã fail bao nhiêu
lần thật.)

## 4. Chaos test trên app thật (tùy chọn)

```bash
# Bật injection 10% cho API trong compose rồi bắn k6 như Step 10:
PUBLISH_FAILURE_RATE=0.10 docker compose up -d --force-recreate api
```

Quan sát:

- Logs: `publish_failed` (có `event_id`, `worker_id`, `retry`, `retry_at`, `error`),
  `stale_events_reclaimed`, `publish_failure_injection_enabled`.
- Metrics `http://localhost:4053/metrics`: `outbox_events_published_total`,
  `outbox_events_failed_total`, `outbox_events_closed_total`.

Tắt lại: `docker compose up -d --force-recreate api` (rate mặc định 0).

## 5. Invariants — SQL kiểm tra tay sau mỗi lần test

```sql
-- Không mất event: tổng phải bằng số đã seed
SELECT status, COUNT(*) FROM outbox_events GROUP BY status;

-- Không có row kẹt PROCESSING sau khi hệ đã yên
SELECT COUNT(*) FROM outbox_events
WHERE status = 'PROCESSING'
  AND processing_at < NOW() - INTERVAL '5 minutes';

-- Mỗi lần fail đều được ghi sổ
SELECT COUNT(*) FROM outbox_events
WHERE attempts > 0 AND last_error IS NULL;   -- phải = 0

-- CLOSE chỉ được phép khi đã hết retry budget (maxRetries = 3)
SELECT COUNT(*) FROM outbox_events
WHERE status = 'CLOSE' AND attempts <> 3;    -- phải = 0
```

## 6. Kafka (Outbox → Kafka)

### 6.1 Bật stack

```bash
docker compose config            # kiểm tra cú pháp compose
docker compose up -d             # postgres, kafka, kafka-init, api, pgadmin, prometheus
docker compose ps                # kafka phải "healthy", kafka-init phải "exited (0)"
docker compose logs kafka-init   # thấy topic eventflow.events được create/describe
```

Địa chỉ broker:

- trong Docker (API container): `kafka:9092` — compose tự set
- từ host (tests, `outboxbench`, binary chạy tay): `localhost:9092` (`.env`)

Xem message thật trên topic:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic eventflow.events \
  --from-beginning --property print.key=true --property print.headers=true
```

### 6.2 Unit test (không cần broker)

```bash
go test ./internal/platform/kafka/ -run 'TestPublish|TestClose' -v
go test ./internal/config/ -run 'Kafka|Publisher' -v
```

`TestPublish*` dùng writer giả: key = event_id, value = payload nguyên
bản, headers, metrics, error được wrap, context cancel không tính là
failure, payload rỗng/không phải JSON bị từ chối trước khi ghi.

### 6.3 Integration test (cần Postgres + Kafka đang chạy)

| Test | Chứng minh |
|------|-----------|
| `TestPublisherDeliversEventToKafka` (`internal/platform/kafka`) | Publish 1 event → đọc lại từ topic → verify key, header, và toàn bộ field JSON (`event_id, type, version, source, user_id, anonymous_id, session_id, timestamp, properties`) |
| `TestOutboxWorkersPublishEveryEventToKafkaOnce` (`internal/event/application`) | 100 PENDING → 4 workers → 100 PUBLISHED, đọc đúng 100 message, mỗi `event_id` đúng 1 lần, không có message thừa |
| `TestOutboxWorkerKafkaOutageKeepsEventsPendingUntilRecovery` | **Kafka outage**: writer giả "down" → mọi event fail → `attempts ≥ 1`, `last_error` chứa "kafka", **PUBLISHED = 0**; bật lại → cùng workers drain hết → PUBLISHED = 20, Kafka nhận đúng 20 lần (chỉ cần Postgres) |

```bash
go test ./internal/platform/kafka/ -run TestPublisherDeliversEventToKafka -v
go test ./internal/event/application/ -run 'Kafka' -v
```

Test tự tạo topic tạm `eventflow.test.*` / `eventflow.outbox-test.*` (1
partition) và xóa sau khi xong. Không có broker thì skip.

### 6.4 Failure drill với broker thật (thủ công)

Chứng minh outbox bảo vệ pipeline khi Kafka chết thật:

```bash
# 1. Postgres + Kafka lên, API tắt (để không tranh event với bench)
docker compose up -d postgres kafka kafka-init
docker compose stop api

# 2. Tắt Kafka
docker compose stop kafka

# 3. Chạy bench qua Kafka, bench sẽ chờ tới 3 phút cho outbox drain
go run ./cmd/outboxbench -publisher kafka -workers 2 -batch 10 -events 50 -timeout 3m
#    -> log "kafka_publish_failed", published = 0, pending/processing = 50,
#       attempts tăng theo backoff. Không có event nào PUBLISHED.

# 4. Trong lúc bench vẫn đang chạy (terminal khác): bật lại Kafka
docker compose start kafka

# 5. Bench tự drain: published = 50, pending = 0, processing = 0, lost = 0
```

Nếu lỡ để bench timeout trước khi bật Kafka: chạy lại bước 3 sau khi Kafka
lên, các event PENDING còn lại sẽ được publish (bench truncate + seed lại,
nên số liệu là của lần chạy mới).

### 6.5 Benchmark BEFORE / AFTER

Cùng workload: 1000 events, batch 10, interval 50ms, workers 1/2/4/8.

```powershell
docker compose up -d postgres kafka kafka-init
./benchmark/run_worker_bench.ps1 -Publisher inmemory   # BEFORE
./benchmark/run_worker_bench.ps1 -Publisher kafka      # AFTER
```

Kết quả vào `benchmark/results-worker-concurrency-<publisher>.md`. Bench
in thêm `publish_duration_avg/max` (thời gian tới khi Kafka ack) và
`kafka_publish_*` metrics được expose ở API `/metrics` khi chạy qua API.

### 6.6 At-least-once — duplicate window (chủ đích, không sửa ở step này)

```
Kafka ack  →  crash trước MarkPublished  →  PROCESSING stale  →  reclaim
           →  publish lại  →  Kafka có 2 message cùng key/payload
```

Đây là hành vi đúng của at-least-once. Consumer ở Step 13 phải idempotent
theo `event_id`. Test crash/reclaim ở mục 2 (`TestOutboxWorkerCrashIsRecoveredByReclaim`)
chính là đường đi này, chỉ khác là publisher giả.

## 7. Env liên quan

| Env | Default | Ý nghĩa |
|-----|---------|---------|
| `OUTBOX_WORKERS` | 1 | Số worker cùng cạnh tranh `ClaimPending` |
| `OUTBOX_BATCH_SIZE` | 100 | Số event mỗi lần claim |
| `OUTBOX_INTERVAL` | 5s | Chu kỳ poll của mỗi worker |
| `OUTBOX_STALE_TIMEOUT` | 5m | Row PROCESSING già hơn mức này bị reclaim về PENDING. Phải lớn hơn hẳn `SHUTDOWN_TIMEOUT` + publish chậm nhất, nếu không sẽ double-publish |
| `PUBLISH_FAILURE_RATE` | 0 | Xác suất inject fail mỗi publish attempt, `[0, 1)`. Chỉ dùng khi drill |
| `OUTBOX_PUBLISHER` | kafka | `kafka` = publish qua broker; `log` = chỉ log ra stdout (chạy không cần Kafka) |
| `KAFKA_BROKERS` | localhost:9092 | Danh sách broker, phân cách bằng dấu phẩy. Trong compose API dùng `kafka:9092` |
| `KAFKA_TOPIC` | eventflow.events | Topic outbox worker publish vào; `kafka-init` tạo sẵn |
| `KAFKA_CLIENT_ID` | eventflow-api | Client id của producer |
| `KAFKA_WRITE_TIMEOUT` | 10s | Deadline mỗi lần produce (dial + write + ack); quá hạn = publish fail → retry |
| `KAFKA_MAX_ATTEMPTS` | 3 | Producer tự retry bấy nhiêu lần trước khi báo fail cho outbox worker |
| `KAFKA_BATCH_TIMEOUT` | 10ms | Thời gian producer gom batch; giữ nhỏ vì mỗi worker publish đồng bộ |
| `SHUTDOWN_TIMEOUT` | 30s | Thời gian drain khi shutdown |
