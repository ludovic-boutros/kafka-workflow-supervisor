package com.example.demo.topology.processor;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;

@Slf4j
public class SelectHeaderAsKeyProcessor extends ContextualProcessor<String, byte[], String, byte[]> {
    @NonNull
    private final String correlationIdHeaderName;

    @Builder
    public SelectHeaderAsKeyProcessor(@NonNull String correlationIdHeaderName) {
        this.correlationIdHeaderName = correlationIdHeaderName;
    }

    @Override
    public void process(Record<String, byte[]> record) {
        Header correlationIdHeader = record.headers().lastHeader(correlationIdHeaderName);
        if (correlationIdHeader == null || correlationIdHeader.value() == null) {
            log.error("Cannot get correlation id ({}) for record with key: {}", correlationIdHeaderName, record.key());
            return;
        }

        context().forward(new Record<>(new String(correlationIdHeader.value(), StandardCharsets.UTF_8),
                record.value(),
                record.timestamp(),
                record.headers()));
    }
}
