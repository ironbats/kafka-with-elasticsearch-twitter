package br.com.kafka.advanced.filters;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafksFilterTweets {

    private static JsonParser jsonParser = new JsonParser();
    private static Logger LOG = LoggerFactory.getLogger(KafksFilterTweets.class);

    public static void main(String[] args) {

        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> inputStream =  streamsBuilder.stream("twitter-bolsonaro");
        KStream<String,String> filterStream = inputStream.filter((k,jsonTwitter ) ->
            extractUserFollowers(jsonTwitter) > 10
        );

        filterStream.to("important_tweets");
        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);
        //start streams application
        kafkaStreams.start();
    }

    private static  Integer extractUserFollowers (String idTwtitter)
    {
        //using the jason parser to get a especific field to json twitter
        try {
            return jsonParser.parse(idTwtitter).
                    getAsJsonObject().get("user")
                    .getAsJsonObject().get("followers_count")
                    .getAsInt();
        }catch (IllegalStateException cause)
        {
            LOG.error("Error with followers user  , "+ cause);

            return 0;
        }
        catch(NullPointerException cause)
        {
            LOG.error("Error with followers user  , "+ cause);
            return 0;
        }
    }
}
