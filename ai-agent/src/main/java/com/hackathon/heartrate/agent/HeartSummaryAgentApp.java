package com.hackathon.heartrate.agent;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeartSummaryAgentApp {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final class HeartEvent {
        private final String userId;
        private final String eventId;
        private final Instant start;
        private final Instant end;
        private final double heartRate;

        private HeartEvent(String userId, String eventId, Instant start, Instant end, double heartRate) {
            this.userId = userId;
            this.eventId = eventId;
            this.start = start;
            this.end = end;
            this.heartRate = heartRate;
        }
    }

    public static void main(String[] args) throws Exception {
        Path repoRoot = Path.of("..").toAbsolutePath().normalize();
        loadEnvFile(repoRoot.resolve("producer/.env"));
        loadEnvFile(repoRoot.resolve("ai-agent/.env"));

        String bootstrapServers = requiredEnv("BOOTSTRAP_SERVERS");
        String kafkaApiKey = requiredEnv("KAFKA_API_KEY");
        String kafkaApiSecret = requiredEnv("KAFKA_API_SECRET");
        String schemaRegistryUrl = requiredEnv("SCHEMA_REGISTRY_URL");
        String srApiKey = requiredEnv("SR_API_KEY");
        String srApiSecret = requiredEnv("SR_API_SECRET");

        String cleanedTopic = env("CLEANED_TOPIC", "heart-rate-cleaned");
        String summaryTopic = env("SUMMARY_TOPIC", "patient.heart_state.snapshot");
        int windowSeconds = Integer.parseInt(env("WINDOW_SECONDS", "30"));
        long pollMillis = Long.parseLong(env("POLL_MILLIS", "1000"));

        String openAiApiKey = env("OPENAI_API_KEY", "");
        String openAiModel = env("OPENAI_MODEL", "gpt-4.1-mini");
        HttpClient httpClient = HttpClient.newHttpClient();
        String kafkaJaasConfig = String.format(
            "org.apache.kafka.common.security.plain.PlainLoginModule required username='%s' password='%s';",
            kafkaApiKey,
            kafkaApiSecret
        );

        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put("security.protocol", "SASL_SSL");
        consumerProps.put("sasl.mechanism", "PLAIN");
        consumerProps.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaJaasConfig);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "heart-summary-agent-java-v1");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        consumerProps.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, srApiKey + ":" + srApiSecret);

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProps.put("security.protocol", "SASL_SSL");
        producerProps.put("sasl.mechanism", "PLAIN");
        producerProps.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaJaasConfig);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        Map<String, Deque<HeartEvent>> userBuffers = new HashMap<>();
        Map<String, Instant> userLastPublished = new HashMap<>();

        try (KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(consumerProps);
             KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps)) {

            consumer.subscribe(List.of(cleanedTopic));
            System.out.println("Listening on topic: " + cleanedTopic);

            while (running.get()) {
                ConsumerRecords<String, Object> records = consumer.poll(java.time.Duration.ofMillis(pollMillis));
                Instant now = Instant.now();

                for (ConsumerRecord<String, Object> record : records) {
                    HeartEvent event = parseEvent(record.value());
                    if (event == null) {
                        continue;
                    }

                    Deque<HeartEvent> queue = userBuffers.computeIfAbsent(event.userId, ignored -> new ArrayDeque<>());
                    queue.addLast(event);
                    evictOld(queue, now, windowSeconds);
                }

                for (Map.Entry<String, Deque<HeartEvent>> entry : userBuffers.entrySet()) {
                    String userId = entry.getKey();
                    Deque<HeartEvent> queue = entry.getValue();
                    if (queue.isEmpty()) {
                        continue;
                    }

                    Instant last = userLastPublished.get(userId);
                    if (last != null && now.minusSeconds(windowSeconds).isBefore(last)) {
                        continue;
                    }

                    Map<String, Object> summary = buildSummary(userId, queue, now, windowSeconds, openAiApiKey, openAiModel, httpClient);
                    String payload = MAPPER.writeValueAsString(summary);
                    producer.send(new ProducerRecord<>(summaryTopic, userId, payload));
                    producer.flush();
                    userLastPublished.put(userId, now);

                    System.out.printf(Locale.ROOT,
                            "Published 30s summary: user=%s risk=%s samples=%s%n",
                            userId,
                            summary.get("risk_level"),
                            summary.get("sample_count"));
                }
            }
        }
    }

    private static HeartEvent parseEvent(Object value) {
        if (value == null) {
            return null;
        }

        Map<String, Object> map = toMap(value);
        if (map == null) {
            return null;
        }

        String userId = asString(map.get("user_id"));
        if (userId == null || userId.isBlank()) {
            return null;
        }

        Double heartRate = asDouble(map.get("heart_rate"));
        if (heartRate == null) {
            return null;
        }

        String eventId = Objects.toString(map.get("event_id"), "");
        Instant start = parseInstant(map.get("start_time"));
        Instant end = parseInstant(map.get("end_time"));

        Instant now = Instant.now();
        if (start == null && end == null) {
            start = now;
            end = now;
        } else if (start == null) {
            start = end;
        } else if (end == null) {
            end = start;
        }

        return new HeartEvent(userId, eventId, start, end, heartRate);
    }

    private static void evictOld(Deque<HeartEvent> queue, Instant now, int windowSeconds) {
        Instant cutoff = now.minusSeconds(windowSeconds);
        while (!queue.isEmpty() && queue.peekFirst().start.isBefore(cutoff)) {
            queue.removeFirst();
        }
    }

    private static Map<String, Object> buildSummary(
            String userId,
            Deque<HeartEvent> queue,
            Instant now,
            int windowSeconds,
            String openAiApiKey,
            String openAiModel,
            HttpClient httpClient
    ) {
        List<HeartEvent> events = new ArrayList<>(queue);
        double min = events.stream().mapToDouble(event -> event.heartRate).min().orElse(0.0);
        double max = events.stream().mapToDouble(event -> event.heartRate).max().orElse(0.0);
        double avg = events.stream().mapToDouble(event -> event.heartRate).average().orElse(0.0);

        String riskLevel;
        if (max >= 145.0 || min <= 40.0) {
            riskLevel = "critical";
        } else if (max >= 130.0 || min <= 45.0) {
            riskLevel = "watch";
        } else {
            riskLevel = "stable";
        }

        Instant windowStart = events.stream().map(event -> event.start).min(Instant::compareTo).orElse(now);
        Instant windowEnd = events.stream().map(event -> event.end).max(Instant::compareTo).orElse(now);

        Map<String, Object> summary = new HashMap<>();
        summary.put("user_id", userId);
        summary.put("window_seconds", windowSeconds);
        summary.put("sample_count", events.size());
        summary.put("avg_hr", round1(avg));
        summary.put("min_hr", round1(min));
        summary.put("max_hr", round1(max));
        summary.put("risk_level", riskLevel);
        summary.put("window_start", windowStart.atOffset(ZoneOffset.UTC).toString());
        summary.put("window_end", windowEnd.atOffset(ZoneOffset.UTC).toString());
        summary.put("summary_generated_at", now.atOffset(ZoneOffset.UTC).toString());
        summary.put("summary", composeSummary(summary, openAiApiKey, openAiModel, httpClient));

        return summary;
    }

    private static String composeSummary(Map<String, Object> summary, String openAiApiKey, String openAiModel, HttpClient httpClient) {
        if (openAiApiKey == null || openAiApiKey.isBlank()) {
            return ruleSummary(summary);
        }

        try {
            String prompt = "Write a concise 1-2 sentence heart-rate summary for the last 30s. "
                    + "Do not diagnose disease. Include risk level. Input JSON: "
                    + MAPPER.writeValueAsString(summary);

            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("model", openAiModel);
            requestBody.put("input", prompt);
            requestBody.put("max_output_tokens", 120);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("https://api.openai.com/v1/responses"))
                    .header("Authorization", "Bearer " + openAiApiKey)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(MAPPER.writeValueAsString(requestBody)))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() >= 200 && response.statusCode() < 300) {
                Map<String, Object> payload = MAPPER.readValue(response.body(), new TypeReference<>() {});
                String outputText = asString(payload.get("output_text"));
                if (outputText != null && !outputText.isBlank()) {
                    return outputText.trim();
                }
            }
        } catch (Exception ignored) {
        }

        return ruleSummary(summary);
    }

    private static String ruleSummary(Map<String, Object> summary) {
        return String.format(Locale.ROOT,
                "30s summary for %s: avg %s bpm (min %s, max %s) across %s readings; current risk is %s.",
                summary.get("user_id"),
                summary.get("avg_hr"),
                summary.get("min_hr"),
                summary.get("max_hr"),
                summary.get("sample_count"),
                summary.get("risk_level"));
    }

    private static String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private static Double asDouble(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        try {
            return Double.parseDouble(String.valueOf(value));
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Instant parseInstant(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            long raw = number.longValue();
            return raw > 1_000_000_000_000L ? Instant.ofEpochMilli(raw) : Instant.ofEpochSecond(raw);
        }

        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return null;
        }

        try {
            return Instant.parse(text);
        } catch (DateTimeParseException ignored) {
        }

        try {
            long raw = Long.parseLong(text);
            return raw > 1_000_000_000_000L ? Instant.ofEpochMilli(raw) : Instant.ofEpochSecond(raw);
        } catch (NumberFormatException ignored) {
            return null;
        }
    }

    private static Map<String, Object> toMap(Object value) {
        if (value instanceof Map<?, ?> map) {
            return MAPPER.convertValue(map, new TypeReference<>() {});
        }
        return MAPPER.convertValue(value, new TypeReference<>() {});
    }

    private static double round1(double value) {
        return Math.round(value * 10.0) / 10.0;
    }

    private static void loadEnvFile(Path path) throws IOException {
        if (!Files.exists(path)) {
            return;
        }
        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#") || !trimmed.contains("=")) {
                    continue;
                }
                String[] pieces = trimmed.split("=", 2);
                String key = pieces[0].trim();
                String value = pieces[1].trim();
                if (!key.isEmpty() && System.getenv(key) == null) {
                    System.setProperty(key, value);
                }
            }
        }
    }

    private static String requiredEnv(String key) {
        String value = env(key, "");
        if (value.isBlank()) {
            throw new IllegalArgumentException("Missing required env var: " + key);
        }
        return value;
    }

    private static String env(String key, String defaultValue) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            value = System.getProperty(key);
        }
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value;
    }
}
