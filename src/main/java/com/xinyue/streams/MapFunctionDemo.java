package com.xinyue.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

/**
 * 演示Map操作
 * 将输入字符串全部转换成大写输出
 * (需要先创建topic)
 */
public class MapFunctionDemo {

    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic = "streams-map-output";
    static final String bootstrapServers = "192.168.56.108:9092";

    public static void main(String[] args) {
        /** configuration */
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "map-function-demo");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "map-function-demo-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        /** processing topology */
        final StreamsBuilder builder = new StreamsBuilder();
        //create stream, use default deserializers in configuration.
        final KStream<byte[], String> textLines = builder.stream(inputTopic);

        //stateless, no need for Ktable
        //convert to uppercase stream
        final KStream<byte[], String> upperStreams = textLines.mapValues(v -> v.toUpperCase());
        //other way: modify key as original value, value as uppercase
        //value is String, so type is <String, String>
        final KStream<String, String> originalAndUpperStreams = textLines
                .map((key, value) -> KeyValue.pair(value, value.toUpperCase()));

        //use default serializers because the data types did not change
        //upperStreams.to(outputTopic);
        originalAndUpperStreams.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
