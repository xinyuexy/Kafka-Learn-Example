package com.xinyue.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Properties;

/**
 * 演示Kafka time windows的用法
 * 检测异常事件: 一分钟内点击超过2次的用户(输入字符串模拟)
 */
public class TimeWindowsDemo {

    static final String inputTopic = "streams-plaintext-input";
    static final String outputTopic = "streams-pipe-output";
    static final String bootstrapServers = "192.168.56.108:9092";

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-time-windows");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "streams-time-windows-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //默认的serializers和deserializers
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        //设置commit interval为500ms, 保证低延迟(事件检测中很重要,任何更改能够及时刷新)
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500);

        final StreamsBuilder builder = new StreamsBuilder();
        //每个record代表用户的一次click, value代表具体的用户名
        final KStream<String, String> clicks = builder.stream(inputTopic);

        //KStream -> KGroupedStream -> KTable
        final KTable<Windowed<String>, Long> anomalousUsers = clicks
                //将用户名映射到key(后续group需要按key)
                .map((key, value) -> new KeyValue<>(value, value))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)))
                .count()
                .filter((windowedUserId, count) -> count >= 3);

        //方便输出,将window的KTable转换成KStream
        final KStream<String, Long> outputStreams = anomalousUsers
                .toStream()
                .filter((windowedUserId, count) -> count != null)
                .map((windowedUserId, count) -> new KeyValue<>(windowedUserId.toString(), count));

        outputStreams.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
