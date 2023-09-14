package com.example.demo.utils;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.*;

public class AvroSerdes {


    /**
     * Will return automatically the correct serdes for any Avro Object
     *
     * @param <T> Avro model type
     * @return a serdes for any Avro Object
     */
    public static <T extends SpecificRecord> SpecificAvroSerde<T> get() {
        var specificSerdes = new SpecificAvroSerde<T>();

        var propMap = new HashMap<String, Object>();
        propMap.put(SCHEMA_REGISTRY_URL_CONFIG, StreamContext.get(SCHEMA_REGISTRY_URL_CONFIG));
        if (!StringUtils.isEmpty(StreamContext.get(BASIC_AUTH_CREDENTIALS_SOURCE)) && !StringUtils.isEmpty(StreamContext.get(USER_INFO_CONFIG))) {
            propMap.put(BASIC_AUTH_CREDENTIALS_SOURCE, StreamContext.get(BASIC_AUTH_CREDENTIALS_SOURCE));
            propMap.put(USER_INFO_CONFIG, StreamContext.get(USER_INFO_CONFIG));
        }

        specificSerdes.configure(propMap, false);
        return specificSerdes;
    }
}
