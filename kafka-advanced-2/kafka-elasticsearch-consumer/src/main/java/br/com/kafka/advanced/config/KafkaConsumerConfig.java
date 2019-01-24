package br.com.kafka.advanced.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerConfig {


    public static KafkaConsumer<String, String> getConfigKafkaConsumer(final String topic) {
        //Consumer configs
        String bootStrapServers = "127.0.0.1:9092";
        String groupId = "elasticsearch-demo";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //earliest/latest/none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable autocommit
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");


        //create consumer
        KafkaConsumer<String, String> kconsumer = new KafkaConsumer<>(properties);
        kconsumer.subscribe(Arrays.asList(topic));

        return kconsumer;
    }

}
