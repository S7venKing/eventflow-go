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

## 6. Env liên quan

| Env | Default | Ý nghĩa |
|-----|---------|---------|
| `OUTBOX_WORKERS` | 1 | Số worker cùng cạnh tranh `ClaimPending` |
| `OUTBOX_BATCH_SIZE` | 100 | Số event mỗi lần claim |
| `OUTBOX_INTERVAL` | 5s | Chu kỳ poll của mỗi worker |
| `OUTBOX_STALE_TIMEOUT` | 5m | Row PROCESSING già hơn mức này bị reclaim về PENDING. Phải lớn hơn hẳn `SHUTDOWN_TIMEOUT` + publish chậm nhất, nếu không sẽ double-publish |
| `PUBLISH_FAILURE_RATE` | 0 | Xác suất inject fail mỗi publish attempt, `[0, 1)`. Chỉ dùng khi drill |
| `SHUTDOWN_TIMEOUT` | 30s | Thời gian drain khi shutdown |
