package org.example.monthlytimeseries;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class AlphaVantageKafkaConsumer {

    private final static Logger log = LoggerFactory.getLogger(AlphaVantageKafkaConsumer.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        log.info("My Alpha Vantage Consumer");

        String myTopic = "alphavantage.monthlytimeseries";
        String groupId = "group3";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG ,"earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(myTopic));

        try (FileWriter fileWriter = new FileWriter("outputMonthly.csv")) {
            String header = "Date,Open,High,Low,Close,Volume\n";
            fileWriter.append(header);

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1);

                for(ConsumerRecord<String, String> record : records){
                    JSONObject jsonObj = new JSONObject(record.value());
                    String open = jsonObj.getString("1. open");
                    String high = jsonObj.getString("2. high");
                    String low = jsonObj.getString("3. low");
                    String close = jsonObj.getString("4. close");
                    String volume = jsonObj.getString("5. volume");

                    fileWriter.append(String.join(",", record.key(), open, high, low, close, volume));
                    fileWriter.append("\n");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
