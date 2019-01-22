package br.com.kafka.advanced;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerKafkaWithThread {

    Logger LOG = LoggerFactory.getLogger(ConsumerKafkaWithThread.class);

    public static void main(String[] args) {
        new ConsumerKafkaWithThread().run();
    }

    private  ConsumerKafkaWithThread()
    {

    }

    private void run ()
    {
        //Consumer configs
        String bootStrapServers  = "127.0.0.1:9092";
        String groupId  = "my-second-application";
        String topic = "first_topic";

        CountDownLatch countDownLatch = new CountDownLatch(1);
        Runnable myconsumerThread  = new ConsumerThread(bootStrapServers,groupId,topic,countDownLatch);

        //Start  the thread
        Thread thread = new Thread(myconsumerThread);
        thread.start();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(
                () ->{
                 LOG.info("shutdown hook");
                 ((ConsumerThread) myconsumerThread).shutDown();
                }
        ));

        try {
            countDownLatch.wait();

        }catch(InterruptedException cause)
        {
            LOG.info("Application error  interrupted " , cause);

        }finally {
            LOG.info("Application is closing");

        }

        LOG.info("Application is out ");

    }

    public class ConsumerThread implements   Runnable
    {
        private CountDownLatch latch;
        private KafkaConsumer<String,String> consumer;
        Logger LOG = LoggerFactory.getLogger(ConsumerThread.class);


        public ConsumerThread(String bootStrapServers,String groupId,String topic,CountDownLatch latch)
        {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers );
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
            //earliest/latest/none
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

            //Create Consumer
            consumer  = new KafkaConsumer<>(properties);

            //Describe Consumer
            consumer.subscribe(Arrays.asList(topic));

        }

        @Override
        public void run() {

            try {


            while(true)
            {

                ConsumerRecords<String,String> crecords =  consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String,String> record : crecords)
                {
                    LOG.info("Key : "  + record.key()
                            + " Value " + record.value()
                            + " Partition " + record.partition()
                            + " Offset " + record.offset());
                }
             }

            }
            catch(WakeupException cause)
            {
                LOG.info("Shutdown received ");
            }finally
            {
               consumer.close();
               latch.countDown();
            }
        }

        public void shutDown ()
        {
            //is a special method to interrupt consumer.poll
            //it will throw the exception WakeupException
            consumer.wakeup();

        }
    }
}
