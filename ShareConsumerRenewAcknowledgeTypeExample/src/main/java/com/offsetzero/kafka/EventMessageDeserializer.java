package com.offsetzero.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;

public final class EventMessageDeserializer implements Deserializer<EventMessage> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public EventMessage deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            JsonNode json = objectMapper.readTree(new String(data, StandardCharsets.UTF_8));
            if (json == null || !json.isObject()) {
                throw new SerializationException("Event message must be a JSON object");
            }
            return objectMapper.treeToValue(json, EventMessage.class);
        } catch (Exception exception) {
            throw new SerializationException("Unable to deserialize event message", exception);
        }
    }
}