package com.hackathon.heartrate;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Locale;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class HeartRateProducerApp {

    public static void main(String[] args) throws Exception {
        String bootstrapServers = requiredEnv("BOOTSTRAP_SERVERS");
        String schemaRegistryUrl = requiredEnv("SCHEMA_REGISTRY_URL");

        String rawTopic = env("RAW_TOPIC", "heart-rate-raw");
        String topicsCsv = env("TOPICS", "heart-rate-raw,heart-rate-cleaned,heart-rate-features,heart-rate-risk-events,patient.heart_state.snapshot");
        int partitions = Integer.parseInt(env("TOPIC_PARTITIONS", "6"));
        short replicationFactor = Short.parseShort(env("TOPIC_REPLICATION_FACTOR", "3"));

        int eventsPerSecond = Integer.parseInt(env("EVENTS_PER_SECOND", "1"));
        int totalEvents = Integer.parseInt(env("TOTAL_EVENTS", "0")); // 0 = run forever
        String heartRateMode = env("HEART_RATE_MODE", "normal");
        String patientId = env("PATIENT_ID", "user-00987");
        String deviceId = env("DEVICE_ID", "dev-watch-4421");
        String sourceAppId = env("SOURCE_APP_ID", "wearable-ingest-service");
        String schemaPath = env("SCHEMA_PATH", "../schemas/HeartRate.avsc");

        boolean checkOnly = args.length > 0 && "--check-only".equals(args[0]);

        Properties adminProps = buildCommonClientProps(bootstrapServers, schemaRegistryUrl);
        List<String> requiredTopics = parseTopics(topicsCsv);

        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            ensureTopics(adminClient, requiredTopics, partitions, replicationFactor);
            boolean exists = verifyTopicsExist(adminClient, requiredTopics);
            if (!exists) {
                throw new IllegalStateException("Topic checkpoint failed: one or more required topics do not exist.");
            }
            System.out.println("✅ Topic checkpoint passed. Required topics exist: " + requiredTopics);
        }

        if (checkOnly) {
            return;
        }

        Schema schema = loadSchema(schemaPath);
        Properties producerProps = buildProducerProps(bootstrapServers, schemaRegistryUrl);

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(producerProps)) {
            runProducerLoop(producer, schema, rawTopic, patientId, deviceId, sourceAppId, eventsPerSecond, totalEvents, heartRateMode);
        }
    }

    private static void runProducerLoop(
            KafkaProducer<String, GenericRecord> producer,
            Schema schema,
            String topic,
            String patientId,
            String deviceId,
            String sourceAppId,
            int eventsPerSecond,
            int totalEvents,
            String heartRateMode
    ) throws InterruptedException {
        Random random = new Random();
        long delayMs = Math.max(1L, 1000L / Math.max(1, eventsPerSecond));
        int sent = 0;

        System.out.printf("🚀 Starting producer: topic=%s patient=%s mode=%s rate=%d/sec totalEvents=%d%n",
                topic,
                patientId,
                heartRateMode,
                eventsPerSecond,
                totalEvents);

        while (totalEvents == 0 || sent < totalEvents) {
            GenericRecord record = buildHeartRateRecord(schema, patientId, deviceId, sourceAppId, random, heartRateMode);
            ProducerRecord<String, GenericRecord> message = new ProducerRecord<>(topic, patientId, record);

            producer.send(message, (metadata, exception) -> {
                if (exception != null) {
                    System.err.println("❌ Produce failed: " + exception.getMessage());
                } else {
                    System.out.printf("📨 topic=%s partition=%d offset=%d event_id=%s heart_rate=%.1f%n",
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset(),
                            record.get("event_id"),
                            ((Float) record.get("heart_rate"))
                    );
                }
            });

            sent += 1;
            Thread.sleep(delayMs);
        }

        producer.flush();
    }

    private static GenericRecord buildHeartRateRecord(
            Schema schema,
            String patientId,
            String deviceId,
            String sourceAppId,
            Random random,
            String heartRateMode
    ) {
        GenericRecord record = new GenericData.Record(schema);

        long now = Instant.now().toEpochMilli();
        long start = now - 30_000;
        long end = now;
        int utcOffsetMs = OffsetDateTime.now().getOffset().getTotalSeconds() * 1000;

        float baseline = baselineForMode(heartRateMode);
        float noiseAmplitude = noiseAmplitudeForMode(heartRateMode);
        float noise = (random.nextFloat() * (noiseAmplitude * 2.0f)) - noiseAmplitude;
        float occasionalSpike = random.nextDouble() < spikeChanceForMode(heartRateMode)
            ? spikeMagnitudeForMode(heartRateMode)
            : 0.0f;
        float hr = Math.max(40.0f, Math.min(190.0f, baseline + noise + occasionalSpike));

        float min = Math.max(35.0f, hr - (2.0f + random.nextFloat() * 4.0f));
        float max = Math.min(200.0f, hr + (2.0f + random.nextFloat() * 6.0f));
        int heartBeatCount = Math.max(1, Math.round(hr / 2.0f)); // approx beats in 30 seconds

        record.put("user_id", patientId);
        record.put("event_id", UUID.randomUUID().toString());
        record.put("device_id", deviceId);
        record.put("source_app_id", sourceAppId);
        record.put("create_time", now);
        record.put("update_time", now);
        record.put("start_time", start);
        record.put("end_time", end);
        record.put("time_offset", (long) utcOffsetMs);
        record.put("comment", null);
        record.put("heart_rate", hr);
        record.put("heart_beat_count", heartBeatCount);
        record.put("min", min);
        record.put("max", max);
        record.put("client_data_id", null);
        record.put("client_data_ver", null);

        return record;
    }

    private static float baselineForMode(String mode) {
        return switch (normalizeMode(mode)) {
            case "stable" -> 68.0f;
            case "elevated" -> 102.0f;
            case "spiky" -> 86.0f;
            default -> 74.0f;
        };
    }

    private static float noiseAmplitudeForMode(String mode) {
        return switch (normalizeMode(mode)) {
            case "stable" -> 2.0f;
            case "elevated" -> 7.0f;
            case "spiky" -> 10.0f;
            default -> 6.0f;
        };
    }

    private static float spikeChanceForMode(String mode) {
        return switch (normalizeMode(mode)) {
            case "stable" -> 0.02f;
            case "elevated" -> 0.10f;
            case "spiky" -> 0.20f;
            default -> 0.08f;
        };
    }

    private static float spikeMagnitudeForMode(String mode) {
        return switch (normalizeMode(mode)) {
            case "stable" -> 12.0f;
            case "elevated" -> 22.0f;
            case "spiky" -> 34.0f;
            default -> 30.0f;
        };
    }

    private static String normalizeMode(String mode) {
        if (mode == null) {
            return "normal";
        }
        String value = mode.toLowerCase(Locale.ROOT).trim();
        return switch (value) {
            case "stable", "elevated", "spiky", "normal" -> value;
            default -> "normal";
        };
    }

    private static Schema loadSchema(String schemaPath) throws IOException {
        Path path = Path.of(schemaPath);
        return new Schema.Parser().parse(path.toFile());
    }

    private static void ensureTopics(AdminClient adminClient, List<String> topics, int partitions, short replicationFactor)
            throws ExecutionException, InterruptedException {
        List<NewTopic> toCreate = new ArrayList<>();
        for (String topic : topics) {
            toCreate.add(new NewTopic(topic, partitions, replicationFactor));
        }

        CreateTopicsResult result = adminClient.createTopics(toCreate);
        for (String topic : topics) {
            try {
                result.values().get(topic).get();
                System.out.println("✅ Created topic: " + topic);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof TopicExistsException) {
                    System.out.println("ℹ️ Topic already exists: " + topic);
                } else {
                    System.out.println("⚠️ Could not create topic " + topic + ": " + e.getCause());
                }
            }
        }
    }

    private static boolean verifyTopicsExist(AdminClient adminClient, List<String> requiredTopics)
            throws ExecutionException, InterruptedException {
        ListTopicsResult topicsResult = adminClient.listTopics();
        Set<String> existingTopics = topicsResult.names().get();

        List<String> missing = requiredTopics.stream()
                .filter(topic -> !existingTopics.contains(topic))
                .toList();

        if (!missing.isEmpty()) {
            System.err.println("❌ Missing topics: " + missing);
            return false;
        }
        return true;
    }

    private static Properties buildProducerProps(String bootstrapServers, String schemaRegistryUrl) {
        Properties props = buildCommonClientProps(bootstrapServers, schemaRegistryUrl);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "25");
        return props;
    }

    private static Properties buildCommonClientProps(String bootstrapServers, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("client.id", env("CLIENT_ID", "heart-rate-producer"));

        props.put("security.protocol", env("SECURITY_PROTOCOL", "SASL_SSL"));
        props.put(SaslConfigs.SASL_MECHANISM, env("SASL_MECHANISM", "PLAIN"));

        String jaas = env("SASL_JAAS_CONFIG", null);
        if (jaas != null && !jaas.isBlank()) {
            props.put(SaslConfigs.SASL_JAAS_CONFIG, jaas);
        }

        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, env("SR_BASIC_AUTH_CREDENTIALS_SOURCE", "USER_INFO"));

        String srAuth = env("SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO", null);
        if (srAuth != null && !srAuth.isBlank()) {
            props.put("schema.registry.basic.auth.user.info", srAuth);
        }

        props.put("auto.register.schemas", env("AUTO_REGISTER_SCHEMAS", "true"));
        return props;
    }

    private static String requiredEnv(String key) {
        String value = env(key, null);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required env var: " + key);
        }
        return value;
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }

    private static List<String> parseTopics(String csv) {
        String[] split = csv.split(",");
        List<String> topics = new ArrayList<>();
        for (String item : split) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                topics.add(trimmed);
            }
        }
        return topics;
    }
}
