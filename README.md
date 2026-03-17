# confluent-ai-day-2026
Confluent AI Day Hackathon Project

## Automation Scripts

- Deploy infra + Flink jobs:
	- [scripts/deploy_infra.sh](scripts/deploy_infra.sh)
- Run finite pipeline test (produce + consume checks):
	- [scripts/test_pipeline.sh](scripts/test_pipeline.sh)

### Usage

From repo root, run the workflow in this order:

1. Make scripts executable:

```bash
chmod +x scripts/deploy_infra.sh scripts/test_pipeline.sh
chmod +x ai-agent/run_summary_agent.sh
```

2. Deploy infrastructure and Flink pipeline:

```bash
./scripts/deploy_infra.sh
```

This step provisions or validates required Confluent resources (environment/cluster/pool), ensures Kafka topics exist, creates Flink tables, and starts Flink SQL jobs.

3. Send test data and validate pipeline flow:

```bash
./scripts/test_pipeline.sh
```

This step sends finite test events and samples output topics so you can confirm data is flowing through cleaning, feature, and risk stages.

4. Start the 30-second summary agent (keep it running in its own terminal):

```bash
./ai-agent/run_summary_agent.sh
```

5. In a second terminal, send fresh events so the running agent has live data to summarize:

```bash
./scripts/test_pipeline.sh
```

The summary agent reads from `heart-rate-cleaned` with latest offsets, so it should run in parallel with data production to emit new summaries.

6. Validate summary output in `patient.heart_state.snapshot` (for example, with Confluent CLI consume).

Single agent that consumes [heart-rate-cleaned](flink/sql/00_tables.sql), builds a rolling 30-second per-user context, composes a natural-language summary, and publishes it to `patient.heart_state.snapshot`.

- Agent code: [ai-agent/src/main/java/com/hackathon/heartrate/agent/HeartSummaryAgentApp.java](ai-agent/src/main/java/com/hackathon/heartrate/agent/HeartSummaryAgentApp.java)
- Optional env template: [ai-agent/.env.example](ai-agent/.env.example)

The scripts read credentials and defaults from [producer/.env](producer/.env). If `OPENAI_API_KEY` is set, it uses an LLM for the summary text; otherwise it falls back to deterministic rule-based wording.
