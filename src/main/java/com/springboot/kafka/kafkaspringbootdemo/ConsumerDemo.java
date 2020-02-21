package com.springboot.kafka.kafkaspringbootdemo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo extends Thread {

    KafkaConsumer<Integer, String> consumer;
    String topic;

    public ConsumerDemo(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.32.135:9092,192.168.32.136:9092,192.168.32.137:9092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-demo");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-id1");
        properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000"); //自动提交(批量确认)
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //一个新的group的消费者去消费一个topic
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); //这个属性. 它能够消费昨天发布的数据
        // latest 情况下，新的消费者将会从其他消费者最后消费的offset处开始消费Topic下的消息
        // earliest 情况下，新的消费者会从该topic最早的消息开始消费
        //none 情况下，新的消费者加入以后，由于之前不存在offset，则会直接抛出异常
        consumer = new KafkaConsumer<Integer, String>(properties);
        this.topic = topic;
    }

    @Override
    public void run() {
        consumer.subscribe(Collections.singleton(this.topic));
        while (true) {
            ConsumerRecords<Integer, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));//超时时间1秒钟
            consumerRecords.forEach(record -> {
                System.out.println(record.key() + "->" + record.value());
            });
        }
    }
    public static void main(String[] args) {
        new ConsumerDemo("test").start();
    }
}