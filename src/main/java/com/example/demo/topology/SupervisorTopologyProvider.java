package com.example.demo.topology;

import com.example.demo.data.SupervisionRecord;
import com.example.demo.model.Node;
import com.example.demo.model.Topic;
import com.example.demo.model.Workflow;
import com.example.demo.topology.processor.SelectHeaderAsKeyProcessor;
import com.example.demo.topology.processor.UpdateSupervisionStateProcessor;
import com.example.demo.utils.AvroSerdes;
import com.example.demo.utils.MayBeException;
import com.example.demo.utils.StreamExceptionCatcher;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.example.demo.Constants.SUPERVISION_STORE;

@Slf4j
public class SupervisorTopologyProvider implements Supplier<Topology> {
    private final Workflow workflowDefinition;
    private final String correlationIdHeaderName;
    private final String outputTopic;
    private final String dlqTopic;
    private final Duration purgeSchedulingPeriod;
    private final Duration eventTimeout;

    @Builder
    public SupervisorTopologyProvider(Workflow workflowDefinition,
                                      String correlationIdHeaderName,
                                      String outputTopic,
                                      String dlqTopic,
                                      Duration purgeSchedulingPeriod,
                                      Duration eventTimeout) {
        this.workflowDefinition = workflowDefinition;
        this.correlationIdHeaderName = correlationIdHeaderName;
        this.outputTopic = outputTopic;
        this.dlqTopic = dlqTopic;
        this.purgeSchedulingPeriod = purgeSchedulingPeriod;
        this.eventTimeout = eventTimeout;
    }

    private static List<String> getTopicNames(Supplier<List<Topic>> inputTopicNameSupplier) {
        return inputTopicNameSupplier.get().stream()
                .map(Topic::getName)
                .toList();
    }

    @Override
    public Topology get() {
        StreamsBuilder builder = new StreamsBuilder();

        // Create the statestores
        builder.addStateStore(
                Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(SUPERVISION_STORE),
                        Serdes.String(),
                        AvroSerdes.<SupervisionRecord>get()));

        workflowDefinition.forEach(node -> {
            // Consume input topics
            stream(builder, () -> getProcessor(node), node::getInputTopics, outputTopic);

            // Consume and process final topics
            stream(builder, () -> getProcessor(node),
                    () -> node.getOutputTopics().stream().filter(Topic::isFinal).collect(Collectors.toList()),
                    outputTopic);
        });

        return builder.build();
    }

    private UpdateSupervisionStateProcessor getProcessor(Node node) {
        return UpdateSupervisionStateProcessor.builder()
                .correlationIdHeaderName(correlationIdHeaderName)
                .purgeSchedulingPeriod(purgeSchedulingPeriod)
                .eventTimeout(purgeSchedulingPeriod)
                .node(node)
                .build();
    }

    // We should therefore save the key in a header and replace it with the correlationId
    private void stream(StreamsBuilder builder,
                        ProcessorSupplier<String, byte[], String, MayBeException<SupervisionRecord, byte[]>> processorSupplier,
                        Supplier<List<Topic>> inputTopicNameSupplier,
                        String outputTopic) {
        List<String> topicNames = getTopicNames(inputTopicNameSupplier);
        topicNames.forEach(topicName -> {
                    KStream<String, byte[]> repartitionStream =
                            StreamExceptionCatcher.<String, byte[], byte[]>of("repartition")
                                    .check(builder.stream(topicName, Consumed.with(Serdes.String(), Serdes.ByteArray()))
                                                    // We need to repartition using the correlation id
                                                    .process(() -> SelectHeaderAsKeyProcessor.builder()
                                                            .correlationIdHeaderName(correlationIdHeaderName).build()),
                                            dlqTopic)
                                    .repartition(
                                            Repartitioned.with(Serdes.String(), Serdes.ByteArray())
                                                    .withName(topicName)
                                                    // TODO: should be configurable
                                                    .withNumberOfPartitions(4)
                                    );

                    KStream<String, SupervisionRecord> stream =
                            StreamExceptionCatcher.<String, SupervisionRecord, byte[]>of("final-processing")
                                    .check(repartitionStream
                                                    .process(processorSupplier, SUPERVISION_STORE),
                                            dlqTopic);

                    if (outputTopic != null) {
                        stream.to(outputTopic, Produced.with(Serdes.String(), AvroSerdes.get()));
                    }
                }
        );
    }
}
