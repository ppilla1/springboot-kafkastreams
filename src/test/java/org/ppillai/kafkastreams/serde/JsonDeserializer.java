package org.ppillai.kafkastreams.serde;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {

    private Gson gson = new Gson();
    private Class<T> deserializedClass;

    public JsonDeserializer(Class<T> deserializedClass) {
        this.deserializedClass = deserializedClass;
    }

    public JsonDeserializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (null == deserializedClass){
            deserializedClass = (Class<T>)configs.get("serializedClass");
        }
    }

    @Override
    public T deserialize(String s, byte[] bytes) {

        if (null == bytes){
            return null;
        }

        return gson.fromJson(new String(bytes), deserializedClass);
    }

    @Override
    public void close() {

    }
}
