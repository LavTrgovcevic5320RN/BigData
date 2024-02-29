package org.example.news;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Properties;

public class AlphaVantageProducer {
    /*     Aplha Vantage api key
           FVQWMJ69OH9P2DGL
           2BBNAOC9LPE1DG98
           GHW4VN1CHK5Z592V

    */
    private final static Logger log = LoggerFactory.getLogger(AlphaVantageProducer.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        String bootstrapServers = "127.0.0.1:9092";

        String topic = "alphavantage.news";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32*1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("https://www.alphavantage.co/query?function=NEWS_SENTIMENT&tickers=AAPL&apikey=2BBNAOC9LPE1DG98"))
//                .header("Authorization", "FVQWMJ69OH9P2DGL")
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        String jsonResponse = response.body();

        JSONObject jsonObject = new JSONObject(jsonResponse);
        JSONArray feedArray = jsonObject.getJSONArray("feed");
        for (int i = 0; i < feedArray.length(); i++) {
            JSONObject feedItem = feedArray.getJSONObject(i);
            log.info(feedItem.toString());

            producer.send(new ProducerRecord<>(topic, feedItem.toString()));
        }

        producer.flush();
        producer.close();
    }

}
