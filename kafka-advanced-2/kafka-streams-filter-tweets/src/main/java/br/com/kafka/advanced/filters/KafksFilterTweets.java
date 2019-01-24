package br.com.kafka.advanced.filters;

import br.com.kafka.advanced.config.KafkaStreamConfig;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafksFilterTweets {

    private static JsonParser jsonParser = new JsonParser();
    private static Logger LOG = LoggerFactory.getLogger(KafksFilterTweets.class);

    public static void main(String[] args) {

        //create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String,String> inputStream =  streamsBuilder.stream("twitter-bolsonaro");
        KStream<String,String> filterStream = inputStream.filter((k,jsonTwitter ) ->
            extractUserFollowers(jsonTwitter) > 100
        );

        filterStream.to("important_tweets");
        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), KafkaStreamConfig.getStreamProperties());
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