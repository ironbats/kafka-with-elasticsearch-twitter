package br.com.kafka.advanced.twitter;

import br.com.kafka.advanced.config.KafkaProducerConfig;
import br.com.kafka.advanced.config.KafkaSecret;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
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

    protected Client createTwitterClient (BlockingQueue<String> msnQueue)
    {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hoseBirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("bolsonaro");
        hoseBirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(
                KafkaSecret.KAFKA.KEY.KAFKA_KEY, KafkaSecret.KAFKA.SECRET.KAFKA_SECRET,
                KafkaSecret.KAFKA.TOKEN.KAFKA_TOKEN, KafkaSecret.KAFKA.TOKEN_SECRETS.SECRET_TOKEN_VALUE);

        return  new ClientBuilder().
                name("Hosebird-Client-01").
                hosts(hosebirdHosts).
                authentication(hosebirdAuth).
                endpoint(hoseBirdEndpoint).
                processor(new StringDelimitedProcessor(msnQueue)).build();
    }

    public KafkaProducer<String,String> createKafkaProducer()
    {
        return KafkaProducerConfig.getKafkaProducerConfig();
    }
}