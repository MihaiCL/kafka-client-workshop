package ro.ghita.kafkaapplication.kafka.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerPartitionAssignment {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumer-tutorial");
//        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
//        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RoundRobinAssignor");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        consumer.assignment().forEach(topicPartition -> {
            System.out.println(topicPartition.topic() + " " + topicPartition.partition());
        });
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                for (ConsumerRecord<String, String> record : records)
                    System.out.println(record.offset() + ": " + record.value());


                consumer.assignment().forEach(topicPartition -> {
                    System.out.println("      "  +  topicPartition.topic() + " " + topicPartition.partition());
                });
            }
        } catch (WakeupException e) {
            // ignore for shutdown
        } finally {
            consumer.close();
        }
    }

}
