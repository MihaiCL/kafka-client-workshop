package ro.ghita.kafkaapplication.kafka.transformer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import ro.ghita.kafkaapplication.models.Product;
import ro.ghita.kafkaapplication.models.ProductWithEan;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class KafkaTransformer {

    private static final String KAFKA_SERVER_URL = "localhost";
    private static final int KAFKA_SERVER_PORT = 9092;
    private static final String TRANSACTIONAL_ID = "chocolates-unique-id-transaction";
    private static final String CONSUMER_GROUP = "chocolates-transformer-group";

    private final static ObjectMapper objectMapper =  new ObjectMapper();

    public static void main(String[] args) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        consumerProperties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        consumerProperties.setProperty("isolation.level", "read_committed");
        consumerProperties.setProperty("group.id", CONSUMER_GROUP);

        final Properties producerProperties = new Properties();
        producerProperties.setProperty("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        producerProperties.setProperty("transactional.id", TRANSACTIONAL_ID);
        producerProperties.setProperty("enable.idempotence", "true");
        producerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        final KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties);
        producer.initTransactions();

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Arrays.asList("chocolates"));

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
            producer.beginTransaction();
            records.forEach(record -> {
                System.out.println(millisToLocalDateTime(record.timestamp()) +
                        " - key: " + record.key() + "         value: " + record.value());

                ProductWithEan productWithEan = processing(record);
                System.out.println(productWithEan.toString());

                final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("chocolatesWithEans",
                        productWithEan.getName(), writeJson(productWithEan));
                producer.send(producerRecord);
            });
            producer.flush();
            producer.sendOffsetsToTransaction(currentOffsets(records), CONSUMER_GROUP);
            producer.commitTransaction();
        }
    }

    private static ProductWithEan processing(final ConsumerRecord<String, String> record) {
        final String userJson = record.value();
        final Product product = jsonToProduct(userJson);
        return ProductWithEan.builder()
                .name(product.getName())
                .code(product.getCode())
                .description(product.getDescription())
                .ean("5688758")
                .build();
    }

    private static Map<TopicPartition, OffsetAndMetadata> currentOffsets(ConsumerRecords<String, String> records) {
        final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            final List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
            long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
            offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
        }
        return offsetsToCommit;
    }

    private static Product jsonToProduct(final String json){
        try {
            return objectMapper.readValue(json, Product.class);
        } catch (JsonProcessingException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static String writeJson(final ProductWithEan user) {
        try {
            return objectMapper.writeValueAsString(user);
        } catch (final JsonProcessingException exception) {
            throw new RuntimeException(exception);
        }
    }


    public static LocalDateTime millisToLocalDateTime(final Long time) {
        return Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

}