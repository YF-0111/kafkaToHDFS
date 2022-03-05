package com.rogerguo.kafka.test.producer;

import com.rogerguo.kafka.test.beans.UserBehavior;
import com.rogerguo.kafka.test.json.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.function.Consumer;

/**
 * @Description: 向kafka发送消息的类
 * @author: willzhao E-mail: zq2599@gmail.com
 * @date: 2020/5/2 15:02
 */
public class MyKafkaProducer implements Consumer<UserBehavior> {

    private final String topic;
    private final KafkaProducer<byte[], byte[]> producer;
    private final JsonSerializer<UserBehavior> serializer;

    public MyKafkaProducer(String kafkaTopic, String kafkaBrokers) {
        this.topic = kafkaTopic;
        this.producer = new KafkaProducer<>(createKafkaProperties(kafkaBrokers));
        this.serializer = new JsonSerializer<>();
    }

    @Override
    public void accept(UserBehavior record) {
        // 将对象序列化成byte数组
        byte[] data = serializer.toJSONBytes(record);
        // 封装
        ProducerRecord<byte[], byte[]> kafkaRecord = new ProducerRecord<>(topic, data);
        // 发送
        producer.send(kafkaRecord);

        // 通过sleep控制消息的速度，请依据自身kafka配置以及flink服务器配置来调整
        try {
            Thread.sleep(500);
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    /**
     * kafka配置
     * @param brokers The brokers to connect to.
     * @return A Kafka producer configuration.
     */
    public static Properties createKafkaProperties(String brokers) {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        return kafkaProps;
    }

}
