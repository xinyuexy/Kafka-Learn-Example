package com.xinyue.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

/**
 * 创建2个分区的topic，根据value长度进行分区
 * 大于一定长度在1分区, 否则在0分区
 */
public class ProducerWithPartitioner {

    static final String inputTopic = "kafka-partitioner-test";
    static final String bootstrapServers = "192.168.56.108:9092";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //传递自定义分区器
        props.put("partitioner.class", "com.xinyue.producer.partitioners.MyValuePartitioner");
        //传递分区器参数
        props.put("splitLen", 5);

        Producer<Integer, String> producer = new KafkaProducer<>(props);

        String s1 = "lxy", s2 = "hadoop";
        for (int i=0; i<10; i++) {
            String value = (i % 2 == 0 ? s1+i : s2+i);
            ProducerRecord<Integer, String> record = new ProducerRecord<>(inputTopic, i, value);
            //异步发送消息
            producer.send(record, ((recordMetadata, e) ->
                    System.out.printf("%s, partition=%d, \n", value, recordMetadata.partition())));
        }

        producer.close();
    }
}
