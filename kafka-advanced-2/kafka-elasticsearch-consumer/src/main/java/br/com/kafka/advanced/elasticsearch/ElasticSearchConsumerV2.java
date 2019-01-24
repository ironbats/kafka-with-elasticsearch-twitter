package br.com.kafka.advanced.elasticsearch;

import br.com.kafka.advanced.config.KafkaConsumerConfig;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ElasticSearchConsumerV2 {

    private static JsonParser jsonParser =  new JsonParser();

    public ElasticSearchConsumerV2() {

    }

    public static void main(String[] args) throws IOException,InterruptedException {

        Logger LOG = LoggerFactory.getLogger(ElasticSearchConsumer.class);
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String> consumer = createConsumer("twitter-bolsonaro");

        while(client != null ) {

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordsCount  = records.count();
            LOG.info("received  " + recordsCount + " records ");
            BulkRequest bulkRequest = new BulkRequest();

            for(ConsumerRecord<String,String> record : records) {
                //2 strategies to generate id
                //1  - kafka generic ID
                //String  kafkaId = record.topic() + record.partition() + record.offset();
                try
                {
                    String id  = extractIdFromTweet(record.value());

                    //create datas
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets",id).
                            source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);

                }catch(NullPointerException cause)
                {
                  LOG.error("Skiping bad data " + record.value());
                }

            }

            if(recordsCount > 0) {

                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

                LOG.info("commiting offsets ");
                consumer.commitSync();
                LOG.info("Offsets have been commited ");
                Thread.sleep(1000);
            }
        }
    }

    protected static  String extractIdFromTweet (String idTwtitter)
    {
        //using the jason parser to get a especific field to json twitter
        return jsonParser.parse(idTwtitter).getAsJsonObject().get("id_str").getAsString();
    }

    protected static KafkaConsumer<String, String> createConsumer(String topic)
    {
        return KafkaConsumerConfig.getConfigKafkaConsumer(topic);
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
