package com.springboot.kafka.kafkaspringbootdemo;


import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerDemo extends Thread{
    private final KafkaProducer<Integer,String> producer;
    private final String topic;
    public ProducerDemo(String topic) {
        Properties properties=new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.32.129:9092,192.168.32.131:9092,192.168.32.133:9092");
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"producer-demo");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        /**
         * 异步发送的两个属性
         * properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "100");
         * properties.put(ProducerConfig.LINGER_MS_CONFIG, "100");
         */

        producer=new KafkaProducer<Integer, String>(properties);
        this.topic = topic;
    }
    @Override
    public void run() {
        int num=0;
        while(num<50){
            String msg="producer test message:"+num;
            try {
                /**
                 * 同步发送代码片段
                 * producer.send(new ProducerRecord<Integer, String>(topic,msg)).get();
                 */
                producer.send(new ProducerRecord<Integer, String>(topic, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        System.out.println("异步发送回调");
                    }
                });
                TimeUnit.SECONDS.sleep(2);
                num++;
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static void main(String[] args) {
        new ProducerDemo("test").start();
    }
}