package org.example;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class WikimediaChangeProducer {
    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        logger.info("I am wiki change producer");
        String topic = "wikimedia.changes";
        URI streamUri = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        // 2. Configure connection parameters using the modern ConnectStrategy
        EventSource.Builder baseBuilder = new EventSource.Builder(ConnectStrategy.http(streamUri)
                .connectTimeout(10, TimeUnit.SECONDS)
                .readTimeout(0, TimeUnit.SECONDS).headers(Headers.of("User-Agent", "Kafka learning (mailto:phuochungus@gmail.com)"))
        );

        // 3. Bind the handler to the background execution thread wrapper
        BackgroundEventHandler handler = new WikimediaChangeEventHandler(kafkaProducer, topic);
        BackgroundEventSource backgroundSource = new BackgroundEventSource.Builder(handler, baseBuilder)
                .threadPriority(Thread.MAX_PRIORITY)
                .build();

        // 4. Fire up the engine asynchronously
        backgroundSource.start();

        // Keep the application running to receive data points
        TimeUnit.MINUTES.sleep(TimeUnit.MINUTES.toMillis(1));

        // 5. Shutdown the background workers and release sockets cleanly
        backgroundSource.close();
    }
}
