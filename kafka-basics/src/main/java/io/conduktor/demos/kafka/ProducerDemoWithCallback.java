package io.conduktor.demos.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.Stream;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Starting ProducerDemo");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);


        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(
                        "https://stream.wikimedia.org/v2/stream/recentchange"
                ))
                .header("Accept", "text/event-stream")
                .header(
                        "User-Agent",
                        "KafkaLearningDemo/1.0 (contact: phuochungus@gmail.com)"
                ).GET()
                .build();

        HttpResponse<Stream<String>> response = null;
        try {
            response = client.send(
                    request,
                    HttpResponse.BodyHandlers.ofLines()
            );
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        try (Stream<String> lines = response.body()) {
            if (response.statusCode() != 200) {
                throw new IllegalStateException(
                        "HTTP status: " + response.statusCode()
                );
            }

            StringBuilder data = new StringBuilder();
            Iterator<String> iterator = lines.iterator();

            while (iterator.hasNext()) {
                String line = iterator.next();

                // Dòng trống đánh dấu kết thúc một event.
                if (line.isEmpty()) {
                    if (data.length() > 0) {
                        // Bỏ newline cuối cùng.
                        data.setLength(data.length() - 1);

                        ProducerRecord<String, String> record = new ProducerRecord<>("wikimedia", data.toString());
                        producer.send(record, (metadata, exception) -> {
                            if (exception != null) {
                                log.error("Error while sending record", exception);
                            } else {
                                var partition = metadata.partition();
                                log.info("Partition: {}", partition);
                            }
                        });
                        data.setLength(0);
                    }
                    continue;
                }

                // Comment / heartbeat.
                if (line.startsWith(":")) {
                    continue;
                }

                int colon = line.indexOf(':');
                String field = colon < 0 ? line : line.substring(0, colon);
                String value = colon < 0 ? "" : line.substring(colon + 1);

                if (value.startsWith(" ")) {
                    value = value.substring(1);
                }

                if ("data".equals(field)) {
                    data.append(value).append('\n');
                }
            }
        }
        producer.flush();
        producer.close();
    }
}
