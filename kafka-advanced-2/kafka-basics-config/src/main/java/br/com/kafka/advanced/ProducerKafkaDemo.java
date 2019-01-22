package br.com.kafka.advanced;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKafkaDemo {

    public static void main (String [] args)
    {


        Logger LOG = LoggerFactory.getLogger(ProducerKafkaDemoWIthCallBack.class);

        Properties properties = new Properties();
        //Producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        //Producer

        KafkaProducer <String,String> producer = new KafkaProducer<String, String>(properties);


        //create a producer record
        ProducerRecord<String,String> precord = new ProducerRecord<String, String>("first_topic","Hello World");


        //send data assyncronous

        producer.send(precord, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                if(e != null)
                {
                    // Successfuly
                }
                else
                {
                    //tratar
                }

            }
        });

        //Flush data
        producer.flush();

        //flush and close data
        producer.close();





    }
}
