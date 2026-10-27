package org.example;

import jakarta.json.Json;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.client.json.JsonData;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class WikiChangeConsumer {
    private static final Logger log = LoggerFactory.getLogger(WikiChangeConsumer.class.getSimpleName());
    private static final String topic = "wikimedia.changes";

    public static void main(String[] args) {
        log.info("I am Consumer");

        var transport = ApacheHttpClient5TransportBuilder
                .builder(new HttpHost("http", "localhost", 9200))
                .build();

        OpenSearchClient client = new OpenSearchClient(transport);

        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia-consumer");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty("auto.offset.reset", "earliest");

        // create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shut down detected by consumer");
            consumer.wakeup();

            try {
                mainThread.join();
                log.info("Main thread exiting");
            } catch (InterruptedException e) {
                log.warn("Interrupted while waiting for main thread");
                throw new RuntimeException(e);
            }
        }));

        try {
            consumer.subscribe(List.of(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                        String documentId = record.topic() + "_"
                            + record.partition() + "_" + record.offset();

                    try (var reader = Json.createReader(new StringReader(record.value()))) {
                        var document = JsonData.of(reader.readObject());
                        var response = client.index(request -> request
                                .index("wikimedia-v2")
                                .id(documentId)
                                .document(document)
                        );

                        log.info("Indexed document: {}", response.id());
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch (Exception e) {
            log.error("Error while consuming", e);
        } finally {
            consumer.close();
            log.info("Consumer is finally shut down");
        }
    }
}
