package ro.ghita.kafkaapplication.kafka.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

@Slf4j
public class KafkaJsonSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T data) {
        final ObjectMapper objectMapper =  new ObjectMapper();
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            log.error(e.getMessage(), e);
        }
        return new byte[0];
    }
    
}
