package br.com.kafka.advanced.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static JsonParser jsonParser =  new JsonParser();


    public ElasticSearchConsumer() {

    }


    public static void main(String[] args) throws IOException,InterruptedException {

        Logger LOG = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String> consumer = createConsumer("twitter-bolsonaro");


        while(consumer != null ) {

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String,String> record : records) {

                //2 strategies to generate id
                //1  - kafka generic ID
                //String  kafkaId = record.topic() + record.partition() + record.offset();
                String id  = extractIdFromTweet(record.value());

                //create datas
                IndexRequest indexRequest = new IndexRequest("twitter2", "tweets2",id).
                        source(record.value(), XContentType.JSON);

                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                LOG.info(indexResponse.getId());
                Thread.sleep(1000);

            }

        }
    }

    private static  String extractIdFromTweet (String idTwtitter)
    {
        //using the jason parser to get a especific field to json twitter

        return jsonParser.parse(idTwtitter).getAsJsonObject().get("id_str").getAsString();


    }


    public static KafkaConsumer<String, String> createConsumer(String topic)
    {
        //Consumer configs
        String bootStrapServers  = "127.0.0.1:9092";
        String groupId  = "elasticsearch-demo";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServers );
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        //earliest/latest/none
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");


        //create consumer
        KafkaConsumer<String,String> kconsumer  = new KafkaConsumer<>(properties);
        kconsumer.subscribe(Arrays.asList(topic));

        return kconsumer;


    }


    private static RestHighLevelClient createClient()
    {
        // Full access https
        //user/password/url
        //https://clwldk3hr8:orpjjh76e8@kafka-tweets-8440815878.eu-west-1.bonsaisearch.net
        String hostname = "kafka-tweets-8440815878.eu-west-1.bonsaisearch.net";
        String username  = "clwldk3hr8";
        String password = "orpjjh76e8";


        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        RestClientBuilder build = RestClient.builder( new HttpHost(hostname,443,"https")).
                setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder)
                    {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client =  new RestHighLevelClient(build);

        return client;
    }
}
