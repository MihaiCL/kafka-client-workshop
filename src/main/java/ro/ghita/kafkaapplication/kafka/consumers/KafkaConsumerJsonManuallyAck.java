package ro.ghita.kafkaapplication.kafka.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import ro.ghita.kafkaapplication.models.Product;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class KafkaConsumerJsonManuallyAck {

    public static final String KAFKA_SERVER_URL = "localhost";
    public static final int KAFKA_SERVER_PORT = 9092;
    public static final String CLIENT_ID = "ChocolateConsumer";

    private final static ObjectMapper objectMapper =  new ObjectMapper();

    public static void main(String[] args) {
        final Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_SERVER_URL + ":" + KAFKA_SERVER_PORT);
        properties.setProperty("auto.offset.reset","earliest");
        properties.setProperty("group.id", "chocolates-group");
        properties.setProperty("client.id", CLIENT_ID);
        properties.setProperty("enable.auto.commit", "false");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("chocolates"));
        while(true) {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(record -> {
                System.out.println(millisToLocalDateTime(record.timestamp()) +
                        " - key: " + record.key() + "         value: " + record.value());

                try {
                    Product product = jsonToProduct(record.value());
                    System.out.println(product.toString());
                } catch (RuntimeException e) {
                    System.out.println("Failed to process: " + record.key() + "    " + record.value());
                }
                System.out.println();
            });
            consumer.commitSync(currentOffsets(records));
            System.out.println("acknowledge");
        }
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


    public static LocalDateTime millisToLocalDateTime(final Long time) {
        return Instant.ofEpochMilli(time).atZone(ZoneId.systemDefault()).toLocalDateTime();
    }

}