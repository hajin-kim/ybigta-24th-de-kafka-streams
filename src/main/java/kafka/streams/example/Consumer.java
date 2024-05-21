package kafka.streams.example;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

    private final static Logger log = LoggerFactory.getLogger(Consumer.class);
    private final static String GROUP_ID = "kstream-application";  /* this can be anything you want */

    public Consumer() {
    }

    public static void main(final String[] args) {
        new Consumer().run();
    }

    public void run() {
        log.info("Starting Kafka consumer...");
        final var consumer = createKafkaConsumer();

        // safe close
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("closing consumer...");
            consumer.close();
            log.info("done!");
        }));
        consumer.subscribe(List.of(Config.SINK_TOPIC_NAME));

        log.info("Consumer is ready");
        while (true) {
            final var records = consumer.poll(Duration.ofMillis(100));

            for (final var record : records) {
                log.info("Key: {}, Value: {}", record.key(), record.value());
                log.info("Partition {}, Offset: {}", record.partition(), record.offset());
            }
        }
    }

    public KafkaConsumer<String, String> createKafkaConsumer() {
        final var properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVER);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(properties);
    }

//    ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic stream_test_destination
//        --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

//    ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic stream_test --property print.key=true
//            --property key.separator=":"
}
