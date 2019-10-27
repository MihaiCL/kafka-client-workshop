package ro.ghita.kafkaapplication.kafka.producers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ro.ghita.kafkaapplication.models.Product;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerJsonWithSerializer {

    private static final List<String> PRODUCT_NAMES = Arrays.asList("Milka", "Heidi", "Merci", "Ferrero Rocher");

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "ChocolateProducer";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.setProperty("client.id", CLIENT_ID);
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "ro.ghita.kafkaapplication.kafka.serializers.KafkaJsonSerializer");

        final KafkaProducer<String, Product> producer = new KafkaProducer<>(properties);
        PRODUCT_NAMES.forEach(name -> {
            Product product = Product.builder()
                    .name(name)
                    .description(name)
                    .code(generateCode())
                    .build();
            System.out.println(product.toString());
            ProducerRecord<String, Product> producerRecord = new ProducerRecord<>("chocolates",
                    product.getName(), product);
            producer.send(producerRecord);
        });
        producer.flush();

    }

    private static Integer generateCode(){
        return new Random().ints(1, 781343545).findFirst().getAsInt();
    }

}