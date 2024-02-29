package org.example.monthlytimeseries;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;
import java.util.Scanner;

public class AlphaVantageKafkaProducer {

    private final static Logger log = LoggerFactory.getLogger(AlphaVantageKafkaProducer.class.getSimpleName());

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(properties);

        try{
            String alphaVantageApiKey = "GHW4VN1CHK5Z592V";
            String apiURL = "https://www.alphavantage.co/query?function=TIME_SERIES_MONTHLY&symbol=IBM&apikey=" + alphaVantageApiKey;
            String jsonString = fetchDataFromApi(apiURL);

            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode jsonNode = objectMapper.readTree(jsonString);
            JsonNode timeSeriesNode = jsonNode.get("Monthly Time Series");

            timeSeriesNode.fields().forEachRemaining(entry ->{
                String date = entry.getKey();
                JsonNode data = entry.getValue();

                String messageValue = data.toString();

                ProducerRecord<String, String> record = new ProducerRecord<>("alphavantage.monthlytimeseries", date, messageValue);
                producer.send(record);

                log.info("Produced message for date: " + date);
            });
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            producer.flush();
            producer.close();
        }
    }

    private static String fetchDataFromApi(String apiURL) throws IOException{
        URL url = new URL(apiURL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        StringBuilder response = new StringBuilder();

        try(Scanner scanner = new Scanner(connection.getInputStream())){
            while(scanner.hasNextLine()){
                response.append(scanner.nextLine());
            }
        }
        return response.toString();
    }
}
