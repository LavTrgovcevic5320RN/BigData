package org.example.news;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Properties;

public class AplhaVantageConsumer {

    private final static Logger log = LoggerFactory.getLogger(AplhaVantageConsumer.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        log.info("My Alpha Vantage Consumer");

        String myTopic = "alphavantage.news";
        String groupId = "group2";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(myTopic));

        try (FileWriter fileWriter = new FileWriter("output.csv")) {
            String header = "Title,URL,Time Published,Authors,Summary,Banner Image,Source,Category Within Source,Source Domain,Topics,Overall Sentiment Score,Overall Sentiment Label,Ticker Sentiment\n";
            fileWriter.append(header);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1);

                for(ConsumerRecord<String, String> record : records){
                    JSONObject json = new JSONObject(record.value());
                    String title = "\"" + json.getString("title").replace("\"", "\"\"") + "\"";
                    String url = json.getString("url").replace(",", "");
                    String timePublished = json.getString("time_published");
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss");
                    LocalDateTime dateTime = LocalDateTime.parse(timePublished, formatter);
                    DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");
                    String formattedDate = dateTime.format(outputFormatter);
                    JSONArray authors = json.getJSONArray("authors");
                    String authorsString = authors.toString().replace(",", " ").replace("[","").replace("]", "").replace("\"", "");
                    String summary = "\"" + json.getString("summary").replace("\"", "\"\"").replace(",", "") + "\""; // Primer za summary // Zamenite zareze u sažetku kako ne bi došlo do konflikta u CSV formatu

                    String bannerImage = json.optString("banner_image", "null").replace(",", "").replace(";", "");
                    String source = json.getString("source");
                    String categoryWithinSource = json.getString("category_within_source");
                    String sourceDomain = json.getString("source_domain");
                    JSONArray topics = json.getJSONArray("topics");
                    String topicsString = topics.toString().replace(",", ";"); // Zamenjujemo zareze u nizu 'topics'
                    String overallSentimentScore = json.getNumber("overall_sentiment_score").toString();
                    String overallSentimentLabel = json.getString("overall_sentiment_label");
                    String tickerSentiment = json.getJSONArray("ticker_sentiment").toString();
                    String tickerSentimentString = tickerSentiment.replace(",", ";");

                    log.info(timePublished.substring(0, timePublished.indexOf('T')));
                    fileWriter.append(String.join(",", title, url, timePublished.substring(0, timePublished.indexOf('T')), authorsString, summary, bannerImage, source, categoryWithinSource, sourceDomain, topicsString, overallSentimentScore, overallSentimentLabel, tickerSentimentString));
                    fileWriter.append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
