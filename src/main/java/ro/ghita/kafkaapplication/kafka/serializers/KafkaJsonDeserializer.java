package ro.ghita.kafkaapplication.kafka.serializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

@Slf4j
public class KafkaJsonDeserializer<T> implements Deserializer<T> {

    private Class <T> type;

    public KafkaJsonDeserializer(Class type) {
        this.type = type;
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        final ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(data, type);
        } catch (IOException e) {
           log.error(e.getMessage(), e);
        }
        return null;
    }
}
