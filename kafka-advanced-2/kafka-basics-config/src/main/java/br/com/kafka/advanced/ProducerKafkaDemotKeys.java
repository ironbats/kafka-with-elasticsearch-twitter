package br.com.kafka.advanced;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerKafkaDemotKeys {

    public static void main (String [] args) {

        final Logger LOG = LoggerFactory.getLogger(ProducerKafkaDemotKeys.class);

        Properties properties = new Properties();
        //Producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //Loop Producer
        for(int i =0;  i  < 10 ; i++) {

            String topic = "first_topic";
            String value = " Hello world ! " + i;
            String key = "id_  " + i;


            //create a producer record
            ProducerRecord<String, String> precord = new ProducerRecord<String, String>(topic,key,value);
            //send data assyncronous

            producer.send(precord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        LOG.info("Received metadata : \n "
                                + "Topic : " + recordMetadata.topic() + "\n "
                                + "Partition : " + recordMetadata.partition() + "\n"
                                + "Offset : " + recordMetadata.offset() + "\n "
                                + "TimeStamp : " + recordMetadata.timestamp());
                        // Successfuly
                    } else {
                        //tratar
                        LOG.error("Error  produzido :  ", e);
                    }

                }
            });

        }

        //Flush data
        producer.flush();

        //flush and close data
        producer.close();


    }
}
