package com.example.demo.topology.processor;

import com.example.demo.data.Event;
import com.example.demo.data.SupervisionRecord;
import com.example.demo.model.Node;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.example.demo.Constants.SUPERVISION_STORE;

@Slf4j
public class UpdateSupervisionStateProcessor extends ContextualProcessor<String, byte[], String, SupervisionRecord> {
    private static final String REPARTITION_SUFFIX = "-repartition";
    @NonNull
    private final String correlationIdHeaderName;
    @NonNull
    private final Node node;

    @NonNull
    private final Duration purgeSchedulingPeriod;

    @NonNull
    private final Duration eventTimeout;

    private KeyValueStore<String, SupervisionRecord> store;

    @Builder
    public UpdateSupervisionStateProcessor(String correlationIdHeaderName,
                                           @NonNull Node node,
                                           @NonNull Duration purgeSchedulingPeriod,
                                           @NonNull Duration eventTimeout) {
        this.correlationIdHeaderName = correlationIdHeaderName;
        this.node = node;
        this.purgeSchedulingPeriod = purgeSchedulingPeriod;
        this.eventTimeout = eventTimeout;
    }

    @Override
    public void init(ProcessorContext<String, SupervisionRecord> context) {
        super.init(context);
        store = context().getStateStore(SUPERVISION_STORE);

        context().schedule(purgeSchedulingPeriod, PunctuationType.WALL_CLOCK_TIME, ts -> {
            List<String> keysToBePurged = new ArrayList<>();

            try (KeyValueIterator<String, SupervisionRecord> all = store.all()) {
                while (all.hasNext()) {
                    KeyValue<String, SupervisionRecord> next = all.next();
                    if (ts - next.value.getLastUpdate() > eventTimeout.toMillis()) {
                        context().forward(new Record<>(next.value.getCorrelationId(),
                                next.value,
                                next.value.getLastUpdate()));
                        keysToBePurged.add(next.key);
                        log.warn("Purging event: {}", next.value);
                    }
                }
            }

            keysToBePurged.forEach(store::delete);
        });
    }

    @Override
    public void process(Record<String, byte[]> record) {
        Header header = record.headers().lastHeader(correlationIdHeaderName);

        if (header == null || header.value() == null || context().recordMetadata().isEmpty()) {
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
        context().recordMetadata().ifPresentOrElse(recordMetadata ->
                        event.setTopic(
                                getOriginalTopicNameFromRepartitioned(recordMetadata.topic(), context().applicationId())),
                () -> log.error("No record metadata found for record with key: {} and correlationId: {}",
                        record.key(),
                        correlationId));
        event.setNodeName(node.getName());
        supervisionRecord.getEvents().add(event);
        supervisionRecord.setLastUpdate(event.getTimestamp());

        // Are we in final state ?
        if (node.getOutputTopics().stream()
                .anyMatch(topic -> topic.getName().equals(event.getTopic()) && topic.isFinal())) {
            supervisionRecord.setFinalEventFound(true);
            supervisionRecord.setFinalWorkflowDepth(node.getDepthInWorkflow());
        }

        if (isWorkflowComplete(supervisionRecord)) {
            store.delete(correlationId);
            context().forward(new Record<>(correlationId, supervisionRecord, record.timestamp()));
        } else {
            store.put(correlationId, supervisionRecord);
        }
    }

    private String getOriginalTopicNameFromRepartitioned(String topic, String applicationId) {
        if (topic.endsWith(REPARTITION_SUFFIX)) {
            return topic.substring(applicationId.length() + 1, topic.length() - REPARTITION_SUFFIX.length());
        }
        return topic;
    }

    private boolean isWorkflowComplete(SupervisionRecord record) {
        // The process is complete if a final step happened AND all the previous steps happened as well
        // We have one event  for all previous nodes and two for the last final node
        return record.getFinalEventFound() && record.getEvents().size() == record.getFinalWorkflowDepth() + 1;
    }

    @Override
    public void close() {
        super.close();
    }
}
