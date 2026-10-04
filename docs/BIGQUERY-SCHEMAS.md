# BigQuery dataset `otd` — tables written by this repo

Source of truth: `src/main/java/com/tailoredbrands/otd/dataflow/bigquery/BigQuerySchemas.java`
(the pipelines pass these schemas with `CREATE_IF_NEEDED`). Terraform in `tb-platform-infra`
may pre-create the same tables; any change must be **additive only** (new NULLABLE columns),
in line with the schema-evolution rule in ARCHITECTURE.md §8.

Type names are the legacy BigQuery names (`INTEGER`, `FLOAT`, `RECORD`, `BOOLEAN`) because they
are accepted by every API surface Beam uses. Money is stored as `FLOAT` (FLOAT64) for simplicity;
a production ledger would use `NUMERIC` (see README "Things I would change for production").

Conventions:

* `event_time` is the **business** time from the payload (`eventTime`), `publish_time` is the
  Pub/Sub publish time (= Beam element timestamp / watermark basis), `ingested_at` is the time
  the row was built on the worker. The three together give end-to-end latency.
* `message_id` is the Pub/Sub message id, `event_id` the producer's id. Dedup on `event_id`.

---

## Streaming tables (`OrderEventsStreamingPipeline`)

### `order_events` — partition `DAY(event_time)`, cluster `store_id`

One row per `OrderEvent` (all `eventType`s): flattened envelope + order header, lines as a
REPEATED RECORD.

| Column | Type | Mode | Source |
|---|---|---|---|
| event_id | STRING | REQUIRED | envelope `eventId` |
| event_type | STRING | REQUIRED | `ORDER_CREATED` \| `ORDER_UPDATED` \| `ORDER_CANCELLED` |
| event_time | TIMESTAMP | REQUIRED | envelope `eventTime` |
| schema_version | STRING | NULLABLE | |
| source | STRING | NULLABLE | `ORDER_INTAKE_API` \| `LEGACY_SOAP_ADAPTER` \| `TIBCO_EMS_BRIDGE` \| `REPLAY` |
| correlation_id | STRING | NULLABLE | |
| legacy_message_id | STRING | NULLABLE | JMS message id when bridged |
| message_id | STRING | NULLABLE | Pub/Sub message id |
| publish_time | TIMESTAMP | NULLABLE | Pub/Sub publish time |
| order_id | STRING | REQUIRED | `order.orderId` |
| order_type | STRING | NULLABLE | `RETAIL` \| `TAILORED` \| `CUSTOM` \| `RENTAL` \| `ECOM` |
| channel | STRING | NULLABLE | |
| store_id | STRING | REQUIRED | `order.storeId` (clustering key) |
| store_name | STRING | NULLABLE | side-input enrichment |
| region | STRING | NULLABLE | side-input enrichment |
| customer_id | STRING | NULLABLE | |
| ordered_at | TIMESTAMP | NULLABLE | |
| promised_date | DATE | NULLABLE | |
| currency | STRING | NULLABLE | |
| total_amount | FLOAT | NULLABLE | |
| line_count | INTEGER | NULLABLE | `ARRAY_LENGTH(lines)` precomputed |
| alteration_line_count | INTEGER | NULLABLE | lines with `fulfillmentType = ALTERATION` |
| is_rental | BOOLEAN | NULLABLE | `orderType = RENTAL` or `rental` block present |
| lines | RECORD | REPEATED | see below |
| lines.line_number | INTEGER | NULLABLE | |
| lines.sku | STRING | NULLABLE | |
| lines.quantity | INTEGER | NULLABLE | |
| lines.unit_price | FLOAT | NULLABLE | |
| lines.fulfillment_type | STRING | NULLABLE | |
| lines.alteration_type | STRING | NULLABLE | `alteration.type` |
| lines.alteration_measurement_inches | FLOAT | NULLABLE | |
| lines.tailor_shop_id | STRING | NULLABLE | |
| rental_json | STRING | NULLABLE | raw `rental` block as JSON |
| ship_to_json | STRING | NULLABLE | raw `shipTo` block as JSON |
| ingested_at | TIMESTAMP | NULLABLE | |

### `order_lines` — partition `DAY(event_time)`, cluster `sku`

One row per order line (denormalised for SKU / alteration analytics).

| Column | Type | Mode |
|---|---|---|
| event_id, event_type | STRING | REQUIRED |
| event_time | TIMESTAMP | REQUIRED |
| order_id | STRING | REQUIRED |
| order_type, channel | STRING | NULLABLE |
| store_id | STRING | REQUIRED |
| region | STRING | NULLABLE |
| line_number | INTEGER | REQUIRED |
| sku | STRING | REQUIRED |
| quantity | INTEGER | NULLABLE |
| unit_price, line_amount | FLOAT | NULLABLE |
| fulfillment_type, alteration_type | STRING | NULLABLE |
| alteration_measurement_inches | FLOAT | NULLABLE |
| tailor_shop_id | STRING | NULLABLE |
| ingested_at | TIMESTAMP | NULLABLE |

### `inventory_events` — partition `DAY(event_time)`, cluster `store_id`

| Column | Type | Mode |
|---|---|---|
| event_id, event_type | STRING | REQUIRED |
| event_time | TIMESTAMP | REQUIRED |
| schema_version, source, correlation_id, message_id | STRING | NULLABLE |
| publish_time | TIMESTAMP | NULLABLE |
| order_id, store_id | STRING | REQUIRED |
| line_count | INTEGER | NULLABLE |
| lines | RECORD | REPEATED |
| lines.line_number, lines.quantity | INTEGER | NULLABLE |
| lines.sku, lines.status, lines.location_id | STRING | NULLABLE |
| ingested_at | TIMESTAMP | NULLABLE |

### `shipment_events` — partition `DAY(event_time)`, cluster `order_id`

| Column | Type | Mode |
|---|---|---|
| event_id, event_type | STRING | REQUIRED |
| event_time | TIMESTAMP | REQUIRED |
| schema_version, source, correlation_id, message_id | STRING | NULLABLE |
| publish_time | TIMESTAMP | NULLABLE |
| order_id | STRING | REQUIRED |
| tracking_number, carrier | STRING | NULLABLE |
| status | STRING | REQUIRED (`LABEL_CREATED` \| `IN_TRANSIT` \| `OUT_FOR_DELIVERY` \| `DELIVERED` \| `EXCEPTION`) |
| status_time | TIMESTAMP | NULLABLE |
| location | STRING | NULLABLE |
| ingested_at | TIMESTAMP | NULLABLE |

### `store_order_metrics` — partition `DAY(window_start)`, cluster `store_id`

One row per (store, window, **pane**). Early / on-time / late panes are all written with
accumulating semantics, so take the row with the highest `pane_index` per (store, window_start):

```sql
SELECT * EXCEPT(rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id, window_start ORDER BY pane_index DESC) rn
  FROM `otd.store_order_metrics`
  WHERE window_start >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
) WHERE rn = 1
```

| Column | Type | Mode | Meaning |
|---|---|---|---|
| store_id | STRING | REQUIRED | |
| window_start, window_end | TIMESTAMP | REQUIRED | fixed window bounds (event time) |
| pane_timing | STRING | REQUIRED | `EARLY` \| `ON_TIME` \| `LATE` |
| pane_index | INTEGER | NULLABLE | 0-based firing index within the window |
| is_last | BOOLEAN | NULLABLE | true only for the final (GC) pane |
| orders | INTEGER | REQUIRED | ORDER_CREATED events |
| revenue | FLOAT | REQUIRED | sum of `totalAmount` |
| alteration_lines | INTEGER | REQUIRED | lines with `fulfillmentType = ALTERATION` |
| rental_orders | INTEGER | REQUIRED | orders with `orderType = RENTAL` or a `rental` block |
| emitted_at | TIMESTAMP | NULLABLE | processing time of the firing |

### `dead_letter` — ingestion-time partitioning

| Column | Type | Mode | Meaning |
|---|---|---|---|
| original_topic | STRING | REQUIRED | `orders-v1` \| `inventory-v1` \| `shipments-v1` \| `bigquery:<table>` |
| stage | STRING | REQUIRED | `PARSE` \| `VALIDATE` \| `BIGQUERY_WRITE` |
| reason | STRING | REQUIRED | parse error or `;`-joined validation problems (capped at 1000 chars) |
| message_id | STRING | NULLABLE | original Pub/Sub message id |
| payload | STRING | NULLABLE | original payload, verbatim (replayable) |
| attributes_json | STRING | NULLABLE | original attributes |
| failed_at | TIMESTAMP | REQUIRED | |

---

## Batch tables (`DailyReconciliationPipeline`)

### `legacy_oms_orders` — partition `DAY(extract_date)`, cluster `store_nbr`

| Column | Type | Mode | Source (XML) |
|---|---|---|---|
| extract_date | DATE | REQUIRED | `--runDate` |
| extract_file | STRING | NULLABLE | GCS path of the XML file |
| order_nbr | STRING | REQUIRED | `OrderNbr` |
| order_type | STRING | NULLABLE | `OrderType` |
| store_nbr | STRING | NULLABLE | `StoreNbr` |
| cust_nbr | STRING | NULLABLE | `CustNbr` |
| order_date | STRING | NULLABLE | `OrderDate` kept verbatim (legacy format varies) |
| total_amount | FLOAT | NULLABLE | `TotalAmount` if present, else Σ `Qty * Price` |
| line_count | INTEGER | NULLABLE | |
| lines | RECORD | REPEATED | `Lines/Line` |
| lines.line_nbr, lines.qty | INTEGER | NULLABLE | `LineNbr`, `Qty` |
| lines.sku, lines.fulfill_type | STRING | NULLABLE | `SKU`, `FulfillType` |
| lines.price | FLOAT | NULLABLE | `Price` |
| loaded_at | TIMESTAMP | NULLABLE | |

### `order_reconciliation` — partition `DAY(run_date)`, cluster `classification`

| Column | Type | Mode | Meaning |
|---|---|---|---|
| run_date | DATE | REQUIRED | |
| run_id | STRING | NULLABLE | Dataflow job name (re-runs append; filter on the latest) |
| order_id | STRING | REQUIRED | |
| classification | STRING | REQUIRED | `MATCH` \| `MISSING_IN_PUBSUB` \| `MISSING_IN_LEGACY` \| `AMOUNT_MISMATCH` \| `LINE_COUNT_MISMATCH` |
| legacy_total, pubsub_total, amount_diff | FLOAT | NULLABLE | `amount_diff = legacy - pubsub` |
| legacy_line_count, pubsub_line_count | INTEGER | NULLABLE | |
| legacy_store, pubsub_store | STRING | NULLABLE | |
| detail | STRING | NULLABLE | human-readable explanation |
| checked_at | TIMESTAMP | NULLABLE | |

Useful queries:

```sql
-- daily reconciliation scorecard
SELECT run_date, classification, COUNT(*) AS orders
FROM `otd.order_reconciliation`
WHERE run_id = (SELECT MAX(run_id) FROM `otd.order_reconciliation` WHERE run_date = '2026-10-03')
GROUP BY 1, 2 ORDER BY 1, 2;

-- dead letters by reason in the last day
SELECT original_topic, stage, SUBSTR(reason, 1, 80) AS reason, COUNT(*) c
FROM `otd.dead_letter`
WHERE failed_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
GROUP BY 1, 2, 3 ORDER BY c DESC;
```
