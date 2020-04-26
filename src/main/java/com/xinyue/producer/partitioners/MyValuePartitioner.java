package com.xinyue.producer.partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class MyValuePartitioner implements Partitioner {

    private int splitLen;

    @Override
    public void configure(Map<String, ?> configs) {
        //从生产者获取分数
        splitLen = (Integer) configs.get("splitLen");
    }

    @Override
    public int partition(String topic, Object key, byte[] bytes, Object value, byte[] bytes1, Cluster cluster) {
        //当value长度大于splitLen时分到1分区
        return ((String) value).length() >= splitLen ? 1 : 0;
    }

    @Override
    public void close() {
        System.out.println("分区器关闭");
    }
}
