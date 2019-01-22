package com.mywork.kafkaproject.util;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class LocalDeserializer implements Deserializer<Object> {
  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
  }

  @Override
  public Object deserialize(String topic, byte[] data) {
    ObjectMapper mapper = new ObjectMapper();
    Object object = null;
    try {
      object = mapper.readValue(data, Object.class);
    } catch (Exception exception) {
      System.out.println("Error in deserializing bytes " + exception);
    }
    return object;
  }

  @Override
  public void close() {
  }
}