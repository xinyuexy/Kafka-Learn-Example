package com.xinyue.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * Kafka生产者实例
 */
public class SimpleProducer {

    public static void main(String[] args) {
        String topicName = "Hello-Kafka";

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.56.108:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //创建生产者
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        for (int i=0; i<10; i++) {
            //消息封装对象
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "hello" + i, "world" + i);
            //发送消息
            // producer.send(record);
            //同步获取消息发送成功后返回的信息
            /*
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.printf("topic=%s, partition=%d, offset=%s \n",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }*/

            //异步发送消息,在回调函数中对失败进行处理
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        System.out.println("进行异常处理");
                    } else {
                        System.out.printf("topic=%s, partition=%d, offset=%s \n",
                                recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset());
                    }
                }
            });
        }

        //关闭生产者
        producer.close();
    }
}
