package com.xinyue.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import java.util.Arrays;
import java.util.Properties;

public class WordCountDemo {

    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic = "streams-wordcount-output";
    static final String bootstrapServers = "192.168.56.108:9092";

    public static void main(String[] args) {

        //create configuration
        final Properties streamsProps = getStreamsConfiguration();

        //Define the processing topology of the Streams application.
        final StreamsBuilder builder = new StreamsBuilder();

        //create wordCount stream topology
        createWordCountStream(builder);

        //start streams
        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsProps);
        streams.start();
        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    /** 生成配置 */
    static Properties getStreamsConfiguration() {
        Properties streamsProps = new Properties();
        streamsProps.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-demo");
        streamsProps.put(StreamsConfig.CLIENT_ID_CONFIG, "wordcount-demo-client");
        streamsProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //serializers
        streamsProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //time interval(10s)
        streamsProps.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10*1000);
        return streamsProps;
    }

    /** 创建 stream topology */
    static void createWordCountStream(final StreamsBuilder builder) {
        //input stream
        final KStream<String, String> textLines = builder.stream(inputTopic);

        final KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(value.toLowerCase().split(" ")))
                .groupBy((key, value) -> value)
                .count();
        //output
        wordCounts.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }
}
