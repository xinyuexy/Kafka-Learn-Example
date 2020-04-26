# Kafka-Learn-Example
Learning Kafka By Example

#### Kafka Basis
* [SimpleProducer](/src/main/java/com/xinyue/producer/SimpleProducer.java)：Kafka生产者发送消息的基本用法
* [ConsumerGroup](/src/main/java/com/xinyue/consumer/ConsumerGroup.java)：Kafka消费者群组的基本用法
* [ProducerWithPartitioner](src/main/java/com/xinyue/producer/ProducerWithPartitioner.java)：演示自定义分区器的用法，根据value长度进行分区

#### Kafka Streams
* [WordCountDemo](/src/main/java/com/xinyue/streams/WordCountDemo.java)：word count词频统计，基本用法
* [MapFunctionDemo](src/main/java/com/xinyue/streams/MapFunctionDemo.java)：演示map的基本用法(将输入字符串转换成大写)
* [TimeWindowsDemo](src/main/java/com/xinyue/streams/TimeWindowsDemo.java)：Kafka的TimeWindows操作，在时间窗口内进行聚合操作

更新中...
