package ro.ghita.kafkaapplication.kafka.producers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ro.ghita.kafkaapplication.models.Product;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerJson {

    private static final List<String> PRODUCT_NAMES = Arrays.asList("Milka", "Heidi", "Merci", "Ferrero Rocher");

    private static final String KAFKA_SERVER_URL = "localhost";
    private static final int KAFKA_SERVER_PORT = 9092;
    private static final String CLIENT_ID = "ChocolateProducer";

    private final static ObjectMapper objectMapper =  new ObjectMapper();

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.setProperty("client.id", CLIENT_ID);
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        PRODUCT_NAMES.forEach(name -> {
            final Product product = Product.builder()
                    .name(name)
                    .description(name + "@esolutions.ro")
                    .code(generateCode())
                    .build();

            final String productJson = writeJson(product);

            final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("chocolates",
                    product.getName(), productJson);
            producer.send(producerRecord);
        });
        producer.flush();

    }

    private static String writeJson(final Product product) {
        try {
            return objectMapper.writeValueAsString(product);
        } catch (final JsonProcessingException exception) {
            throw new RuntimeException(exception);
        }
    }

    private static Integer generateCode(){
        return new Random().ints(1, 797987893).findFirst().getAsInt();
    }

}