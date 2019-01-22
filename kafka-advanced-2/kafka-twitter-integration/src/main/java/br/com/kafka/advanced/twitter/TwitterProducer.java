package br.com.kafka.advanced.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger LOG = LoggerFactory.getLogger(TwitterProducer.class);

    public TwitterProducer()
    {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }


    public  void run () {

        //create a twitter client
        BlockingQueue<String> msnQueue  = new LinkedBlockingDeque<>(1000);

        Client client = createTwitterClient(msnQueue);
        client.connect();

        //create a kafka producer
        KafkaProducer<String,String> producer  = createKafkaProducer();

        //add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() ->{
            LOG.info("Stop application ");
            LOG.info("shutdown twitter");
            client.stop();
            producer.close();

            LOG.info("Shutdown producer");
        }));


        //loop to send tweets to kafka

        while(!client.isDone()) {

            String msg  = null;

            try {
                 msg = msnQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOG.error("Error in tweet application " , e);
                client.stop();
            }

            if(msg  != null)
            {
                LOG.info(msg);

                producer.send(new ProducerRecord<>(
                        "twitter-bolsonaro",  msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e)
                    {

                        if(e != null)
                        {
                            LOG.error("Something wrong happened ");
                        }

                    }
                });
            }

            LOG.info("End of application");


        }
    }

    public Client createTwitterClient (BlockingQueue<String> msnQueue)
    {
        String consumerKey = "oeFS1Fe6INKjf6ve3aG10PUYg";
        String consumerSecret = "jjtWhUxNPBc7XFAciEv6o0IOP2YarqjMA9QdNzf6fVv0V90bmB";
        String token = "1087741394851119106-N6OuWhM5pRmg7FqMsZ2BQBjspdyylz";
        String secrets ="S5sXYSkOpNdeBRwcVvY1YCFUGzaCuGi9kRe73WYm2PCmg";


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();


        List<String> terms = Lists.newArrayList("bolsonaro");
        hoseBirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(
                consumerKey,consumerSecret,
                token,secrets);


        return  new ClientBuilder().
                name("Hosebird-Client-01").
                hosts(hosebirdHosts).
                authentication(hosebirdAuth).
                endpoint(hoseBirdEndpoint).
                processor(new StringDelimitedProcessor(msnQueue)).build();
    }

    public KafkaProducer<String,String> createKafkaProducer()
    {

        //Consumer configs
        String bootstrapServers = "127.0.0.1:9092";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");


        //high producer ( at expense of a bit of latency and CPU usage )
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"2000");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));



        KafkaProducer<String,String> producer = new  KafkaProducer<>(properties);

        return producer;

    }


}
