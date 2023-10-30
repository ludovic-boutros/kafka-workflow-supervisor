package com.example.demo.topology.processor;

import com.example.demo.utils.MayBeException;
import com.example.demo.utils.StreamException;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

import java.nio.charset.StandardCharsets;

@Slf4j
public class SelectHeaderAsKeyProcessor extends ContextualProcessor<String, byte[], String, MayBeException<byte[], byte[]>> {
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
            String errorMessage = "Cannot get correlation id (" + correlationIdHeader + ") for record with key: " + record.key();
            log.error(errorMessage);
            MayBeException<byte[], byte[]> exception = MayBeException.of(
                    StreamException.of(
                            new IllegalStateException(errorMessage),
                            record.value()
                    )
            );

            context().forward(
                    new Record<>(record.key(),
                            exception,
                            record.timestamp(),
                            record.headers()));
        } else {
            context().forward(
                    new Record<>(new String(correlationIdHeader.value(), StandardCharsets.UTF_8),
                            MayBeException.of(record.value()),
                            record.timestamp(),
                            record.headers()));
        }
    }
}
