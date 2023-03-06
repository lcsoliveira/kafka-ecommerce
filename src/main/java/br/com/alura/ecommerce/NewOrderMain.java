package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());

//        for (var i = 0; i < 100; i++ ) {
            // Producer message key and value
            var key = UUID.randomUUID().toString();
            var value = key + ",6565,1234";
            var email = "Thank you for your order! We are processing your order";

            // Producer record
            var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", key, value);
            var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

            // variable callback
            Callback callback = (data, ex) -> {
                if (ex != null) {
                    ex.printStackTrace();
                    return;
                }
                System.out.println("success sending " + data.topic() + ":::partition " + data.partition() + "/ offeset " + data.offset() + "/ timestamp " + data.timestamp());
            };

            // Class send will return a future then we need the get
            producer.send(record, callback).get();
            producer.send(emailRecord, callback).get();
//        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
