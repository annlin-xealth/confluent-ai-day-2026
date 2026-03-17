# Heart Rate Producer (Gradle, Java)

Java producer for hackathon Phase 1:
- Ensures required topics exist in Confluent Kafka.
- Produces Avro records (using Schema Registry) to `heart-rate-raw`.
- Simulates `1 patient` at `1 event/sec` by default.

## Prerequisites
- Java 17+
- Gradle installed locally
- Confluent Kafka + Schema Registry credentials

## Project environment setup (recommended)

From the `producer/` folder:

```bash
cp .env.example .env
```

Then edit `.env` with your Confluent values:
- `BOOTSTRAP_SERVERS`
- `SCHEMA_REGISTRY_URL`
- `KAFKA_API_KEY`, `KAFKA_API_SECRET`
- `SR_API_KEY`, `SR_API_SECRET`

## Environment variables
Required:
- `BOOTSTRAP_SERVERS` (e.g. `pkc-xxxx.us-west-2.aws.confluent.cloud:9092`)
- `SCHEMA_REGISTRY_URL`
- `SASL_JAAS_CONFIG` (Kafka API key/secret JAAS string)

Optional (defaults shown):
- `RAW_TOPIC=heart-rate-raw`
- `TOPICS=heart-rate-raw,heart-rate-cleaned,heart-rate-features,heart-rate-risk-events,patient.heart_state.snapshot`
- `TOPIC_PARTITIONS=6`
- `TOPIC_REPLICATION_FACTOR=3`
- `EVENTS_PER_SECOND=1`
- `TOTAL_EVENTS=0` (`0` means run forever)
- `HEART_RATE_MODE=normal` (`normal | stable | elevated | spiky`)
- `PATIENT_ID=user-00987`
- `DEVICE_ID=dev-watch-4421`
- `SOURCE_APP_ID=wearable-ingest-service`
- `SCHEMA_PATH=../schemas/HeartRate.avsc`
- `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO=<sr_api_key>:<sr_api_secret>`

## Run producer + topic checkpoint
From the `producer/` folder:

```bash
./run-producer.sh
```

### Demo batch run (recommended)

To send a short sample batch into `heart-rate-raw`:

```bash
TOTAL_EVENTS=30 EVENTS_PER_SECOND=1 HEART_RATE_MODE=spiky ./run-producer.sh
```

## Checkpoint only (verify topics exist)

```bash
./check-topics.sh
```

Both scripts read credentials from `.env`.

Success signal:
- `✅ Topic checkpoint passed. Required topics exist: [...]`

## Connector config template
A mirror connector template is included at:
- `../connectors/upstream-to-heart-rate-raw.template.json`

Register it against Kafka Connect REST:

```bash
../connectors/register-connector.sh http://<connect-host>:8083 ../connectors/upstream-to-heart-rate-raw.template.json
```
