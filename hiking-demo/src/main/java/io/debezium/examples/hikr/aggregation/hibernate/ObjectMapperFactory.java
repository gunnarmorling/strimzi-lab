/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.examples.hikr.aggregation.hibernate;

import java.io.IOException;
import java.time.LocalDate;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

import io.debezium.examples.hikr.aggregation.connect.KafkaConnectArraySchemaSerializer;
import io.debezium.examples.hikr.aggregation.connect.KafkaConnectObjectSchemaSerializer;
import io.debezium.examples.hikr.aggregation.connect.KafkaConnectSchemaSerializer;
import io.debezium.examples.hikr.aggregation.connect.KafkaConnectSchemaFactoryWrapper.KafkaConnectArraySchemaAdapter;
import io.debezium.examples.hikr.aggregation.connect.KafkaConnectSchemaFactoryWrapper.KafkaConnectObjectSchemaAdapter;
import io.debezium.examples.hikr.aggregation.connect.KafkaConnectSchemaFactoryWrapper.KafkaConnectSchemaAdapter;

public class ObjectMapperFactory {

    public ObjectMapper buildObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        SimpleModule module = new SimpleModule();
        module.addSerializer(KafkaConnectSchemaAdapter.class, new KafkaConnectSchemaSerializer());
        module.addSerializer(KafkaConnectObjectSchemaAdapter.class, new KafkaConnectObjectSchemaSerializer());
        module.addSerializer(KafkaConnectArraySchemaAdapter.class, new KafkaConnectArraySchemaSerializer());
        module.addSerializer(LocalDate.class, new LocalDateAsEpochDaysSerializer());
        mapper.registerModule(module);

        return mapper;
    }

    private static class LocalDateAsEpochDaysSerializer extends JsonSerializer<LocalDate> {

        @Override
        public void serialize(LocalDate value, JsonGenerator gen, SerializerProvider serializers)
                throws IOException {
            gen.writeNumber(value.toEpochDay());
        }
    }
}
