package org.example;


import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeEventHandler implements BackgroundEventHandler {

    private final KafkaProducer<String, String> kafkaProducer;
    private final Logger logger = LoggerFactory.getLogger(WikimediaChangeEventHandler.class.getSimpleName());
    private final String topic;

    public WikimediaChangeEventHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        logger.info("Wikimedia change event handler opened");

    }

    @Override
    public void onClosed() throws Exception {
        logger.info("Wikimedia change event handler closed");
        kafkaProducer.close();

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        logger.info("Wikimedia change event handler received {}", event);
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String comment) {

    }

    @Override
    public void onError(Throwable t) {
        logger.error("Wikimedia change event handler error", t);
        throw new RuntimeException("Wikimedia change event handler error", t);
    }
}
