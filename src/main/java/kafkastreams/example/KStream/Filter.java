package kafkastreams.example.KStream;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class Filter {

    private final static String APPLICATION_NAME = "word-filter-application";

    public static void main(final String[] args) {
        final Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVER);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // 핵심부
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> streamLog = builder.stream(Config.SOURCE_TOPIC_NAME);
        final KStream<String, String> filterStream = streamLog.filter(
                (key, value) -> value.length() > 5);
        filterStream.to(Config.SINK_TOPIC_NAME);

        final KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.start();
    }
}
