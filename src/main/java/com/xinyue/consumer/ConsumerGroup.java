package com.xinyue.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;

/**
 * 消费者群组
 */
public class ConsumerGroup {
    public static void main(String[] args) {
        String topic = "Hello-Kafka";
        String group = "group1";
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.56.108:9092");
        props.put("group.id", group);
        //自动提交偏移量
        props.put("enable.auto.commit", true);
        //指定反序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        //订阅主题
        consumer.subscribe(Collections.singletonList(topic));
        /* 为什么要包裹在try里 */
        try {
            while (true) {
                //轮询获取数据
                ConsumerRecords<String, String> records = consumer.poll(Duration.of(100, ChronoUnit.MILLIS));
                for (ConsumerRecord<String, String> record: records) {
                    System.out.printf("topic=%s, partition=%d, key=%s, value=%s, offset=%d,\n",
                            record.topic(), record.partition(), record.key(), record.value(), record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
