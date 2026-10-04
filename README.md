# tb-order-events-dataflow

Apache Beam **2.66.0** / Google Cloud **Dataflow** pipelines for the Tailored Brands
Order-to-Delivery (OTD) integration platform (see `tb-platform-infra/docs/ARCHITECTURE.md`):

| Pipeline | Kind | What it does |
|---|---|---|
| `OrderEventsStreamingPipeline` | streaming, Flex Template | `orders-v1` / `inventory-v1` / `shipments-v1` → validate → enrich → BigQuery `otd.*` + 1-minute per-store metrics + dead-letter path |
| `DailyReconciliationPipeline` | batch, Flex Template (Cloud Scheduler / Composer) | legacy OMS XML extract ⨝ `otd.order_events` → `otd.order_reconciliation` + CSV scorecard |

Java 21, Maven (single module), JUnit 4 (Beam's `TestPipeline` / `PAssert` / `TestStream` are
JUnit 4 rules), no Lombok, no code generation.

## How this maps to the job posting

* **"Design and build streaming integrations on GCP (Pub/Sub, Dataflow, BigQuery)"** — this repo is
  the analytics leg of the event backbone: Pub/Sub → Dataflow → BigQuery with schema validation,
  enrichment, windowed aggregation and a dead-letter strategy.
* **"Migrate TIBCO EMS / IBM MQ integrations to Pub/Sub"** — the batch job is the migration
  safety net: it reconciles the legacy OMS extract against what reached Pub/Sub, every day, with
  classifications the migration runbook uses as exit criteria (`diff = 0`).
* **"CI/CD, infrastructure as code"** — GitHub Actions build/test on every PR, build Flex Templates
  through Workload Identity Federation (no keys), Terraform in `tb-platform-infra` owns the
  dataset, topics, bucket and service account.

## DAG

```
                                   ┌─────────────────────────────────────────────────────────────┐
 orders-dataflow (Pub/Sub) ──▶ ParseOrders ─┬──▶ EnrichOrders (side input: store CSV) ─┬▶ order_events  (BQ, Storage Write API)
   attributes + messageId     Jackson +     │                                          └▶ order_lines   (BQ)
                              validation    ├──▶ ORDER_CREATED only ▶ KV<storeId,event>
                                            │     ▶ Window 1 min (early 30s / on-time / late ≤5 min, accumulating)
                                            │     ▶ Combine.perKey(StoreMetricsCombineFn) ▶ store_order_metrics (BQ)
                                            └──▶ DeadLetter ───────────────────────┐
 inventory-dataflow ───────▶ ParseInventory ─┬─▶ inventory_events (BQ)              │
                                             └─▶ DeadLetter ───────────────────────┤
 shipments-dataflow ───────▶ ParseShipments ─┬─▶ shipment_events (BQ)               │
                                             └─▶ DeadLetter ───────────────────────┤
 BigQuery rejected rows (getFailedStorageApiInserts) ──────────────────────────────┤
                                                                                   ▼
                                                     Flatten ▶ events-dlq (Pub/Sub) + dead_letter (BQ)
```

```
 gs://…/oms-extract/<date>/*.xml ▶ FileIO.match ▶ readMatches ▶ DOM parse ▶ LegacyOrder ──┬▶ legacy_oms_orders (BQ)
                                                                                           │
 otd.order_events WHERE DATE(event_time)=runDate ▶ PubsubOrderSummary ──┐                   ▼
                                                                       └▶ CoGroupByKey(orderId) ▶ ReconcileFn ▶ order_reconciliation (BQ)
                                                                                                      └▶ Count.perElement ▶ CSV report (GCS)
```

Source layout:

```
src/main/java/com/tailoredbrands/otd/dataflow/
  OrderEventsStreamingPipeline.java   DAG + options wiring (build() is runner-agnostic, reused by tests)
  OrderEventsOptions.java             extends DataflowPipelineOptions
  DailyReconciliationPipeline.java    batch DAG
  DailyReconciliationOptions.java
  model/        Java records (OrderEvent, Order, OrderLine, InventoryEvent, ShipmentEvent, DeadLetter,
                Received<T>, LegacyOrder, StoreMetricsAccumulator, ...) — all Serializable + SerializableCoder
  transforms/   DoFns: ParseAndValidateFn<T>, *Validator, EnrichOrderFn, StoreMetricsCombineFn,
                *ToTableRowFn, DeadLetterToPubsubMessageFn, LegacyXmlParseFn, ReconcileFn, local sinks
  bigquery/     BigQuerySchemas (explicit TableSchemas), BigQueryWrites (Storage Write API factory)
  util/         Json (one Jackson mapper), Times
metadata/       Flex Template parameter metadata (regex-validated)
scripts/        build-template.sh, run-streaming.sh, run-batch.sh, local-direct-runner.sh
docs/           BIGQUERY-SCHEMAS.md
```

## Key design decisions

### Event time, watermark, windows, triggers, late data

* **Element timestamp = Pub/Sub publish time.** `PubsubIO` stamps each element with the publish
  time and Dataflow derives the **watermark** from Pub/Sub's oldest-unacknowledged message; the
  payload's `eventTime` is kept as a column (`event_time`) and drives BigQuery partitioning. We
  deliberately do not re-timestamp on `eventTime` (`withTimestampAttribute` would need the
  attribute on every message, and bridged legacy events can carry an `eventTime` hours old, which
  would stall the watermark).
* **Fixed 1-minute windows** (`--metricsWindowMinutes`) per store on `ORDER_CREATED` only.
* **Trigger** =
  `AfterWatermark.pastEndOfWindow()`
  `.withEarlyFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(30s))`
  `.withLateFirings(AfterPane.elementCountAtLeast(1))`,
  `withAllowedLateness(5 min)`, **`accumulatingFiredPanes()`**.
  * *Early* panes give a dashboard something to show within ~30 s even if the watermark is held
    back by a slow store.
  * The *on-time* pane fires once the watermark passes the end of the window (the "complete"
    answer).
  * *Late* panes fire for every late element for 5 minutes, then the window is garbage-collected
    and later data is dropped (counted by Dataflow's `droppedDueToLateness`).
  * Accumulating mode means each row in `store_order_metrics` is the full running total for the
    window — consumers simply take the highest `pane_index` per (store, window). The cost is
    re-emitting the total, which is tiny here (4 numbers per store per minute). Discarding mode
    would emit deltas and push the summing to the reader.
* `Combine.perKey` with a `CombineFn` (not `GroupByKey` + loop) lets the runner **lift the
  combiner** to the mapper side and keeps only the 4-number accumulator per key/window in state.

### BigQuery: Storage Write API, exactly-once vs at-least-once

All tables use the **Storage Write API** (`BigQueryIO.Write.Method.STORAGE_WRITE_API`), not the
legacy streaming inserts: cheaper, higher throughput, no "streaming buffer" and real transactional
semantics.

| Mode | Option | How | Trade-off |
|---|---|---|---|
| Exactly-once (default) | `--useStorageApiAtLeastOnce=false` | rows are grouped per destination/stream, flushed every `--bigQueryTriggeringSeconds` (10 s) into committed streams with offsets; retries cannot duplicate | an extra shuffle and up to ~10 s latency; needs `withNumStorageWriteApiStreams` or auto-sharding |
| At-least-once | `--useStorageApiAtLeastOnce=true` | `STORAGE_API_AT_LEAST_ONCE` writes through the default stream as rows arrive, no shuffle | lowest latency/cost, but a worker retry can append a row twice; dedupe on `event_id` in queries |

Rows BigQuery rejects (schema mismatch, bad timestamp) come back from
`WriteResult.getFailedStorageApiInserts()` and are routed into the same dead-letter path as
invalid messages (`stage = BIGQUERY_WRITE`), so nothing is silently lost.

Tables are created with explicit schemas (`CREATE_IF_NEEDED`, `WRITE_APPEND`), **day-partitioned
on `event_time`** and clustered on `store_id` (or `sku` / `order_id`) so dashboard queries prune
partitions and skip blocks. `dead_letter` uses ingestion-time partitioning.

### Dead-letter strategy

Every message ends up in exactly one place:

1. `ParseAndValidateFn<T>` (one per stream) parses with Jackson and runs a JSON-schema-like
   `Validator<T>` (required fields, enum values, `totalAmount >= 0`, non-empty `lines` for
   `ORDER_CREATED`, RFC-3339 timestamps, …). Failures become `DeadLetter(originalTopic, stage,
   reason, payload, attributes, messageId)` on a side output (`TupleTag<DeadLetter>`).
2. Dead letters from the three parsers and from BigQuery failed inserts are flattened and written
   **twice**: to Pub/Sub `events-dlq` (payload verbatim + original attributes + `dlqReason`,
   `dlqStage`, `originalTopic` — ARCHITECTURE §3.4, consumed by the `events-dlq-monitor`
   alert and replay tooling) and to BigQuery `otd.dead_letter` (analysis, "top reasons" queries).
3. Beam metrics `otd.<stream>/parsed`, `otd.<stream>/invalid`, `otd.bigquery/<table>_failed_inserts`
   show up in the Dataflow UI and Cloud Monitoring; the platform alerts on DLQ backlog.
4. The pipeline never throws on bad data — a poison message must not crash a worker and block
   the ordering key for every other store.

### Enrichment with a side input

`--storeReferenceGcsPath` points to a small CSV (`store_id,store_name,region,timezone`) read once
with `TextIO` and turned into a `PCollectionView<Map<String, StoreRef>>` (`View.asMap()`).
`EnrichOrderFn` looks up `order.storeId` and adds `store_name` / `region`. Unknown stores are not
an error (counted in `otd.orders/store_not_found`). For reference data that changes while the job
runs, the pattern is a *slowly changing side input* (`GenerateSequence` every N minutes → re-read
→ `View.asSingleton`), noted here but not implemented.

### Autoscaling, Streaming Engine, cost

* **Streaming Engine** (`--enable-streaming-engine`) moves shuffle/state off the workers to the
  Dataflow service, so workers are small (`n1-standard-2`) and scale 1→2 (`--max-workers=2`) on
  backlog + CPU. The state kept per key is tiny (one accumulator per store per minute), so
  memory is not a scaling concern; throughput is bounded by Pub/Sub pull and BigQuery flushes.
* **Cost drivers** for this demo: 1 small worker ≈ $50–70/month + Streaming Engine data processed
  (cents per GB at this volume) + BigQuery Storage Write API ($0.025/GB after the free 2 TB/month)
  + Pub/Sub. The batch job runs a few minutes a day. Keep it cheap: `--max-workers=2`, stop the
  streaming job when not demoing (`gcloud dataflow jobs cancel`/`drain`), use day partitioning so
  dashboards scan one day, and consider `STORAGE_API_AT_LEAST_ONCE` to drop the shuffle.
* **Draining** (`gcloud dataflow jobs drain`) closes windows and flushes BigQuery before
  stopping; cancel does not. Use drain for deploys.

### Batch reconciliation

* Legacy extract files are matched with `FileIO.match` + `readMatches` and parsed with the JDK
  DOM parser (`LegacyXmlParseFn`, namespace-agnostic, DTD disabled). We avoid `XmlIO` because it
  requires JAXB classes generated from the XSD.
* `otd.order_events` for the day is read with `BigQueryIO.readTableRows().fromQuery(...)`
  (Standard SQL, filtered to `ORDER_CREATED` and `DATE(event_time) = runDate`).
* `CoGroupByKey` on order id → `ReconcileFn` classifies `MATCH`, `MISSING_IN_PUBSUB`,
  `MISSING_IN_LEGACY`, `AMOUNT_MISMATCH` (|Δ| > `--amountTolerance`, default 0.01),
  `LINE_COUNT_MISMATCH`; duplicates (replays) collapse to the latest event.
* Outputs: `otd.order_reconciliation` (per order), `otd.legacy_oms_orders` (the parsed extract, so
  analysts can query the legacy side too) and a CSV scorecard
  `run_date,classification,count` written with `TextIO.write().withoutSharding()`.
* Re-runs append with a new `run_id`; the Composer DAG deletes the partition first when it needs
  a clean re-run.

## Running locally

### Unit + pipeline tests (no GCP)

```bash
mvn -B -ntp verify
```

* `ParseAndValidateFnTest` — valid / invalid enum / missing fields / malformed JSON → main vs DLQ tag.
* `StoreMetricsCombineFnTest` — accumulator maths, merge associativity, coder round-trip.
* `OrderEventsStreamingPipelineTest` — `TestStream` of Pub/Sub messages across two 1-minute
  windows plus one late element; `PAssert.inOnTimePane` / `inLatePane` on the metric rows.
* `DailyReconciliationPipelineTest` — whole batch DAG in local mode on
  `src/test/resources/reconciliation` (4 legacy orders, 4 Pub/Sub orders: 1 match, 1 amount
  mismatch, 1 line-count mismatch, 1 missing on each side) + the written CSV/JSONL files.
* `LegacyXmlParseFnTest` — XML fixtures.

### Streaming pipeline against the Pub/Sub emulator (what the platform e2e does)

```bash
# 1. emulator + topics/subscriptions (tb-platform-infra/local)
gcloud beta emulators pubsub start --project=local-project --host-port=localhost:8085 &
PUBSUB_EMULATOR_HOST=localhost:8085 ../tb-platform-infra/local/pubsub-init.sh

# 2. run the pipeline with the DirectRunner, output as JSON lines
PUBSUB_EMULATOR_HOST=localhost:8085 ./scripts/local-direct-runner.sh ./out

# 3. publish a sample event (any Pub/Sub client that honours PUBSUB_EMULATOR_HOST), e.g.
PUBSUB_EMULATOR_HOST=localhost:8085 python3 - <<'EOF'
from google.cloud import pubsub_v1
p = pubsub_v1.PublisherClient()
data = open("src/test/resources/samples/order-created.json","rb").read()
p.publish("projects/local-project/topics/orders-v1", data, eventType="ORDER_CREATED", storeId="0412").result()
EOF

# 4. verify
cat out/order_events.jsonl | jq '{order_id, store_id, total_amount, line_count}'
cat out/order_lines.jsonl  | jq -c '{order_id, line_number, sku}'
cat out/store_order_metrics.jsonl | jq -c .      # appears when the 1-min window closes (+ early pane after 30s)
cat out/dead_letter.jsonl | jq -c '{stage, reason}'
```

Expected `order_events.jsonl` line (abridged):

```json
{"event_id":"6f1c0c8e-…","event_type":"ORDER_CREATED","event_time":"2026-10-03T22:14:05.120Z","order_id":"ORD-2026-000123","store_id":"0412","total_amount":649.99,"line_count":2,"alteration_line_count":1,"lines":[{"line_number":1,"sku":"MW-SUIT-NAVY-42R",…},{"line_number":2,"sku":"ALT-HEM-TROUSER","fulfillment_type":"ALTERATION","alteration_type":"HEM",…}]}
```

The local mode (`--localOutputDir`) swaps the BigQuery sinks for an append-only JSON-lines DoFn and
skips Pub/Sub DLQ publishing; everything upstream (parsing, validation, enrichment, windows,
triggers, combine) is the production code path. `--pubsubRootUrl=http://localhost:8085` points
Beam's Pub/Sub client at the emulator (no credentials needed).

### Batch pipeline locally

```bash
mvn -q compile exec:java \
  -Dexec.mainClass=com.tailoredbrands.otd.dataflow.DailyReconciliationPipeline \
  -Dexec.args="--runner=DirectRunner --runDate=2026-10-03 \
    --localInputDir=src/test/resources/reconciliation --localOutputDir=out/recon \
    --reportGcsPath=out/recon/reconciliation-2026-10-03.csv"
cat out/recon/reconciliation-2026-10-03.csv
# run_date,classification,count
# 2026-10-03,MATCH,1
# 2026-10-03,AMOUNT_MISMATCH,1
# 2026-10-03,LINE_COUNT_MISMATCH,1
# 2026-10-03,MISSING_IN_PUBSUB,1
# 2026-10-03,MISSING_IN_LEGACY,1
```

## Running on Dataflow

Prerequisites from `tb-platform-infra` Terraform: bucket `gs://$PROJECT-tb-otd-dataflow`,
Artifact Registry repo `tb-otd` (us-central1), service account
`dataflow-runner@$PROJECT.iam.gserviceaccount.com` (roles: Dataflow Worker, Pub/Sub
Subscriber/Publisher, BigQuery Data Editor + Job User, Storage Object Admin on the bucket),
dataset `otd`, topics/subscriptions from ARCHITECTURE §4, Cloud Build API enabled.

```bash
export PROJECT=my-project
./scripts/build-template.sh            # mvn -Pdataflow package + gcloud dataflow flex-template build (x2)
./scripts/run-streaming.sh             # Streaming Engine, n1-standard-2, max 2 workers
./scripts/run-batch.sh 2026-10-03      # one business date
```

`build-template.sh` uses `gcloud dataflow flex-template build --jar … --env FLEX_TEMPLATE_JAVA_MAIN_CLASS=…`
(Cloud Build makes the launcher image). `Dockerfile.flex` is the equivalent for people who build the
launcher image themselves. Templates land in
`gs://$PROJECT-tb-otd-dataflow/templates/{order-events-streaming,daily-reconciliation}.json`.

### Verify on GCP

```bash
gcloud dataflow jobs list --region us-central1 --status active
bq query --use_legacy_sql=false 'SELECT store_id, window_start, pane_timing, orders, revenue
  FROM `'"$PROJECT"'.otd.store_order_metrics` ORDER BY window_start DESC, pane_index DESC LIMIT 20'
bq query --use_legacy_sql=false 'SELECT stage, reason, COUNT(*) FROM `'"$PROJECT"'.otd.dead_letter` GROUP BY 1,2'
gcloud pubsub subscriptions pull projects/$PROJECT/subscriptions/events-dlq-monitor --limit 5 --auto-ack
```

## CI/CD

* `.github/workflows/ci.yml` — `mvn verify` on every push/PR, uploads surefire reports; on `main`
  also builds the shaded jar (`-Pdataflow`) and uploads it as a workflow artifact.
* `.github/workflows/deploy-gcp.yml` — guarded by `vars.GCP_PROJECT_ID != ''`; authenticates with
  `google-github-actions/auth@v2` (WIF provider + deployer SA secrets, no keys), builds both Flex
  Templates into the template bucket with launcher images in Artifact Registry
  (`tb-otd/dataflow-<name>:<sha>`), and — on `workflow_dispatch` with `start_streaming=true` —
  launches the streaming job with `gcloud dataflow flex-template run`.

## Things I would change for production

* `NUMERIC` instead of `FLOAT` for money; `Rental`/`ShipTo` as RECORDs once the contract is
  frozen.
* `PubsubIO.withTimestampAttribute("eventTime")` once every producer stamps `eventTime` as a
  Pub/Sub attribute, so windows are true event-time windows and the watermark follows it.
* Slowly-changing side input for store reference data; Dataflow Prime right-fitting.
* Deduplicate on `event_id` with `Deduplicate.values()` (10-minute state TTL) before BigQuery if
  the producers' at-least-once delivery turns out to matter for dashboards.
* Partition-level truncation in the reconciliation re-run, and a Composer sensor on the extract
  file instead of a fixed schedule.

## What to say in the interview

1. **Beam model in one breath**: a pipeline is a DAG of `PTransform`s over `PCollection`s; every
   element has a value, a timestamp and a window; the *runner* (Dataflow, Direct, Flink…)
   decides how to execute it. Same code ran here under the DirectRunner in CI and under
   Dataflow in GCP.
2. **Watermark vs processing time**: the watermark is the runner's estimate that all data up to
   time *t* has arrived (for Pub/Sub, derived from oldest-unacked). Windows close on the
   watermark; early firings are processing-time triggers for freshness; late data is anything
   behind the watermark and is handled explicitly with allowed lateness, not dropped silently.
3. **Trigger design is a product decision**: early/on-time/late + accumulating panes mean the
   dashboard sees something in 30 s, a complete value after the minute, and corrections for 5
   minutes — and the table carries `pane_timing`/`pane_index` so consumers can reason about it.
4. **CombineFn over GroupByKey**: associative + commutative accumulators let the runner combine
   on the mapper side and keep tiny state; we test associativity directly.
5. **Dead letters as a first-class output**: multi-output `ParDo` with `TupleTag`s, no exceptions
   for bad data, DLQ to Pub/Sub for alerting/replay *and* BigQuery for analysis, plus BigQuery's
   own rejected rows fed back into the same path.
6. **Storage Write API semantics**: exactly-once (grouped, offset-tracked streams, triggering
   frequency) versus at-least-once (default stream, no shuffle) — I made it a flag and can
   explain when each is right; streaming inserts are legacy.
7. **Operational knobs**: Streaming Engine, autoscaling bounds, machine type, drain vs cancel,
   `--update` for in-place upgrades when the graph is compatible, Flex Templates so Scheduler /
   Composer / a colleague can launch with parameters and no SDK.
8. **Migration safety net**: the daily reconciliation is how we prove the TIBCO→Pub/Sub cutover
   lost nothing — `CoGroupByKey` across the legacy extract and the new event store, five
   classifications, a scorecard the runbook reads, and tests that pin the classification rules.
