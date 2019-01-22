package br.com.kafka.advanced;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaAssignSeekDemo {


    public static void main(String[] args) {

        Logger LOG = LoggerFactory.getLogger(ConsumerKafkaAssignSeekDemo.class);

        //Consumer configs
        String bootStrapServers  = "127.0.0.1:9092";
        String topic = "first_topic";


        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers );
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //earliest/latest/none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer
        KafkaConsumer<String,String> consumer  = new KafkaConsumer<>(properties);

        //Describe Consumer
        //consumer.subscribe(Arrays.asList(topic));

        //assign and seek are mostly used to replay data or fetch a specific message


        //assign
        long offsetReadFrom = 15L;
        TopicPartition topicPartitionFrom = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(topicPartitionFrom));

        consumer.seek(topicPartitionFrom,offsetReadFrom);

        int numberOfMessageToRead = 10;
        boolean keepOnReadin  = true;
        int numberOfMessagesReadSoFar = 0;



        while(keepOnReadin)
        {

            ConsumerRecords<String,String> crecords =  consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record : crecords)
            {
                numberOfMessagesReadSoFar+= 1;

                LOG.info("Key : "  + record.key()
                        + " Value " + record.value()
                        + " Partition " + record.partition()
                        + " Offset " + record.offset());

                if(numberOfMessagesReadSoFar >= numberOfMessageToRead)
                {
                    keepOnReadin = false;
                    break; // exit to loop
                }
            }
        }


    }



}
