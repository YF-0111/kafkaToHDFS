package com.rogerguo.kafka.test.producer;

import com.rogerguo.kafka.test.beans.UserBehavior;
import com.rogerguo.kafka.test.json.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Properties;

/**
 * @Description
 * @Date 2021/1/17 12:02
 * @Created by X1 Carbon
 */
public class Test {

    public static void main(String[] args) {

        test1();
    }

    public static void test2() {
        String topic = "user-behavior";
        // kafka borker地址
        String broker = "127.0.0.1:9092";

        UserBehavior record = new UserBehavior(1, 1, 1, "test", new Date());

        JsonSerializer<UserBehavior> serializer = new JsonSerializer<>();
        KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(MyKafkaProducer.createKafkaProperties(broker));
        // 将对象序列化成byte数组
        byte[] data = serializer.toJSONBytes(record);
        // 封装
        ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<>(topic, data);
        // 发送
        producer.send(kafkaRecord);
        producer.close();

        System.out.println("test finish");
    }

    public static void test1() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 10000; i++) {
            producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
            System.out.println(i);
        }
        producer.flush();
        producer.close();
    }
}
