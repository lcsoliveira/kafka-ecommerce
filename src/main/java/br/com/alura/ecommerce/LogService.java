package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        /*
            we could pass more than one topic to consumer.subscribe(),
            but it is rarely used in most cases each service do only one thing

            we are using regex to listen all topics that start with ECOMMERCE
        */
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));
        while(true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Founded " + records.count() + " records");
                for (var record : records) {
                    System.out.println("---------------------------------------------");
                    System.out.println("LOG: " + record.topic());
                    System.out.println("KEY: " + record.key());
                    System.out.println("VALUE: " + record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        /*
            If two or more services has the same GROUP name
            then the messages will be distributed between them, and you don't know each one will receive the message
        */
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return properties;
    }
}
