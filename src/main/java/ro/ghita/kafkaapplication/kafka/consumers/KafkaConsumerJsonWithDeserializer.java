package ro.ghita.kafkaapplication.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import ro.ghita.kafkaapplication.kafka.serializers.KafkaJsonDeserializer;
import ro.ghita.kafkaapplication.models.Product;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerJsonWithDeserializer {

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "ChocolateConsumer";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("group.id", "chocolates-group");
        properties.setProperty("enable.auto.commit", "true");
        properties.setProperty("client.id", CLIENT_ID);

        KafkaConsumer<String, Product> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new KafkaJsonDeserializer<>(Product.class));
        consumer.subscribe(Arrays.asList("chocolates"));
        while(true) {
            ConsumerRecords<String, Product> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> System.out.println(millisToLocalDateTime(record.timestamp()) +
                    " - key: " + record.key() + "         value: " + record.value().toString()));
        }
    }

    public static LocalDateTime millisToLocalDateTime(final Long time) {
        return Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

}