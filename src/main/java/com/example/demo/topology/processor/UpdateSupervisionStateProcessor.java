package com.example.demo.topology.processor;

import com.example.demo.data.Event;
import com.example.demo.data.SupervisionRecord;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;

import static com.example.demo.Constants.SUPERVISION_STORE;

@Slf4j
public class UpdateSupervisionStateProcessor extends ContextualProcessor<String, byte[], String, SupervisionRecord> {
    @NonNull
    private final String correlationIdHeaderName;
    @NonNull
    private final String nodeName;
    private final boolean isFinal;
    private KeyValueStore<String, SupervisionRecord> store;

    @Builder
    public UpdateSupervisionStateProcessor(String correlationIdHeaderName, String nodeName, boolean isFinal) {
        this.correlationIdHeaderName = correlationIdHeaderName;
        this.nodeName = nodeName;
        this.isFinal = isFinal;
    }

    @Override
    public void init(ProcessorContext<String, SupervisionRecord> context) {
        super.init(context);
        store = context().getStateStore(SUPERVISION_STORE);

        context().schedule(Duration.ofMinutes(2), PunctuationType.WALL_CLOCK_TIME, ts -> {
            /*
             * Here you have access to the statetore every 2 minutes ( in this example )
             * You can :
             *  - Purge the statestore (older than X)
             *  - Forward data based on whatever business rule (you usually prefer to do this based on an input business event !)
             */
        });
    }

    @Override
    public void process(Record<String, byte[]> record) {
        Header header = record.headers().lastHeader(correlationIdHeaderName);

        if (header == null || header.value() == null || context().recordMetadata().isEmpty()) {
            // TODO !!!! Cannot manage supervision
            log.error("Cannot get correlation id ({}) for record with key: {}", correlationIdHeaderName, record.key());
            return;
        }

        String correlationId = new String(header.value(), StandardCharsets.UTF_8);

        SupervisionRecord supervisionRecord = store.get(correlationId);
        if (supervisionRecord == null) {
            supervisionRecord = new SupervisionRecord();
            supervisionRecord.setCorrelationId(correlationId);
            supervisionRecord.setEvents(new ArrayList<>());
        }

        Event event = new Event();
        event.setTimestamp(record.timestamp());
        context().recordMetadata().ifPresentOrElse(recordMetadata -> event.setTopic(recordMetadata.topic()),
                () -> log.warn("No record metadata found for record with key: {} and correlationId: {}",
                        record.key(),
                        correlationId));
        event.setNodeName(nodeName);
        supervisionRecord.getEvents().add(event);

        if (isFinal) {
            context().forward(new Record<>(correlationId, supervisionRecord, record.timestamp(), record.headers()));
            store.delete(correlationId);
        } else {
            store.put(correlationId, supervisionRecord);
        }
    }

    @Override
    public void close() {
        super.close();
    }
}
