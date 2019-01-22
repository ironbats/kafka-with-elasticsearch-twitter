package br.com.kafka.advanced;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaGroupsDemo {

    public static void main(String[] args) {

        Logger LOG = LoggerFactory.getLogger(ConsumerKafkaGroupsDemo.class);

        LOG.info("Kafka Initial");

        //Consumer configs
        String bootStrapServers  = "127.0.0.1:9092";
        String groupId  = "my-second-application";
        String topic = "first_topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers );
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //earliest/latest/none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //create consumer
        KafkaConsumer<String,String> kconsumer  = new KafkaConsumer<String, String>(properties);

        //subscrime consumer to our topic
        //Unico topico usando singleton
        //kconsumer.subscribe(Collections.singleton(topic));

        //Diversos topicos
        kconsumer.subscribe(Arrays.asList("first_topic"));

        //poll  for new data
        while(true)
        {

            ConsumerRecords<String,String> crecords =  kconsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : crecords)
            {
                LOG.info("Key : "  + record.key()
                        + " Value " + record.value()
                        + " Partition " + record.partition()
                        + " Offset " + record.offset());
            }


        }

    }
}
