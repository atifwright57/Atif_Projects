# Streaming Ingestion Platform

This project simulates business events in Kafka and consumes them with Spark
Structured Streaming. It parses and validates the event contract, deduplicates
within an event-time watermark, merges valid events into a Delta table, and
routes malformed records to a separate Delta quarantine table.

## Flow

```text
Kafka topic
    |
    v
Spark Kafka source (offsets + persistent checkpoint)
    |
    +-- invalid contract rows --> Delta quarantine (idempotent MERGE by Kafka offset)
    |
    +-- valid events --> 10-minute event-time watermark / dedup
                            |
                            v
                       Delta events (MERGE by event_id)
```

## Prerequisites

- Python 3.10-3.12, Java 8/11/17, and Spark 3.5.1-compatible runtime.
- A Kafka cluster accessible to both the producer and Spark process, with the
  `business-events` topic created.
- Persistent writable storage for the Spark checkpoint. For distributed
  deployments, use a durable cloud path such as a Unity Catalog Volume or
  supported object-storage location, not a local driver filesystem.

Install the project's Python dependencies:

```powershell
Set-Location projects\streaming-ingestion-platform
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

`kafka-python` provides the producer client. `delta-spark` configures Delta
Lake, while the Spark job resolves the matching Kafka connector package
(`spark-sql-kafka-0-10_2.12:3.5.1`) from Maven when its Spark session starts.
Allow access to Maven Central or pre-provision that connector in your Spark
runtime.

## Run

Set the broker and topic for your environment:

```powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_TOPIC = "business-events"
```

In one terminal, publish 100 sample events, including periodic duplicate and
late-arriving records:

```powershell
python producer\kafka_producer.py --events 100 --duplicate-every 10 --late-every 15
```

In another terminal, start the stream:

```powershell
python src\structured_streaming.py
```

The local output paths are `data/delta/events`,
`data/delta/quarantine`, and `data/checkpoints/kafka-events`. Configure durable
storage and unique checkpoint paths before running multiple environments or
deployments. Existing Delta data paths and checkpoints should be retained
together across restarts.

Use the command-line options for other paths and Kafka settings:

```powershell
python src\structured_streaming.py --help
python producer\kafka_producer.py --help
```

## Checkpointing, deduplication, and late data

- **Offsets and recovery:** Spark stores Kafka progress and state under the
  checkpoint location. Restarting with the same checkpoint resumes the query
  and its deduplication state. Do not delete or share a checkpoint between
  independent queries.
- **Idempotent sink writes:** Spark's `foreachBatch` is at-least-once. Each
  valid batch is merged into Delta by `event_id`; an existing event is only
  updated when its event timestamp is newer. If Spark retries a micro-batch
  after a failure, the merge does not create a second row. Malformed-record
  quarantine uses topic/partition/offset as its idempotency key.
- **Duplicates:** `dropDuplicatesWithinWatermark` retains event IDs within the
  configured watermark horizon. Delta merge provides a second safeguard
  against duplicate table rows, even if a checkpoint is recreated.
- **Late arrivals:** The default event-time watermark is 10 minutes. A valid
  event arriving after the watermark has advanced beyond its event timestamp
  is too late for the stateful deduplicator and may be dropped by Spark. Increase
  `--watermark-delay` when the source's lateness distribution requires a wider
  horizon. The watermark is a state-retention/late-data policy, not a claim that
  all late records are retained.
- **Contract errors:** Invalid JSON, missing fields, invalid timestamps, and
  negative amounts are written to the quarantine Delta table with their raw
  Kafka payload and source offset for inspection.

The stream uses Kafka's event timestamp from the payload for event-time
processing; the broker timestamp is used only as a safe fallback for malformed
events so they can be quarantined. Keep the event ID stable across producer
retries to get the intended deduplication behavior.
