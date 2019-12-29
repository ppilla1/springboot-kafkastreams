package org.ppillai.kafkastreams.serde;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

@Slf4j
public class JsonSerializer<T> implements Serializer<T> {
    private Gson gson = new Gson();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        log.warn("Unreachable method called");
    }

    @Override
    public byte[] serialize(String s, T t) {
        return gson.toJson(t).getBytes(Charset.forName("UTF-8"));
    }


    @Override
    public void close() {

    }
}
