package kafka.streams.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.StringTokenizer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

    private final static Logger log = LoggerFactory.getLogger(Consumer.class);
    static BufferedReader br;
    static StringTokenizer str;

    public Producer() {
    }

    public static void main(final String[] args) throws IOException {
        new Producer().run();
    }

    public void run() throws IOException {
        log.info("Starting Kafka producer...");
        final var producer = createKafkaProducer();

        // safe close
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("closing producer...");
            producer.close();
            log.info("done!");
        }));

        log.info("Producer is ready");
        br = new BufferedReader(new InputStreamReader(System.in));
        final var keyExist = false;

        while (true) {
            final String key;
            final String value;
            if (keyExist) {
                str = new StringTokenizer(br.readLine());
                key = str.nextToken();
                value = str.nextToken();
            } else {
                key = null;
                value = br.readLine();
            }
            final var record = new ProducerRecord<>(Config.SOURCE_TOPIC_NAME, key, value);

            if (value != null) {
                producer.send(record, (metadata, e) -> {
                    if (e != null) {
                        log.error("Something went wrong", e);
                    }
                });
            }
        }
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        System.out.println("BOOTSTRAP_SERVERS = " + Config.BOOTSTRAP_SERVER);
        final Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }

//    ./kafka-console-producer.sh --bootstrap-server localhost:9092 --topic stream_test --property parse.key=true
}
