package com.example.demo.topology;

import com.example.demo.data.SupervisionRecord;
import com.example.demo.model.Node;
import com.example.demo.model.Topic;
import com.example.demo.model.Workflow;
import com.example.demo.topology.processor.SelectHeaderAsKeyProcessor;
import com.example.demo.topology.processor.UpdateSupervisionStateProcessor;
import com.example.demo.utils.AvroSerdes;
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
    private final Duration purgeSchedulingPeriod;
    private final Duration eventTimeout;

    @Builder
    public SupervisorTopologyProvider(Workflow workflowDefinition,
                                      String correlationIdHeaderName,
                                      String outputTopic, Duration purgeSchedulingPeriod, Duration eventTimeout) {
        this.workflowDefinition = workflowDefinition;
        this.correlationIdHeaderName = correlationIdHeaderName;
        this.outputTopic = outputTopic;
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
        // TODO: use DTL
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
                        ProcessorSupplier<String, byte[], String, SupervisionRecord> processorSupplier,
                        Supplier<List<Topic>> inputTopicNameSupplier,
                        String outputTopic) {
        List<String> topicNames = getTopicNames(inputTopicNameSupplier);
        topicNames.forEach(topicName -> {
                    KStream<String, SupervisionRecord> stream = builder.stream(topicName, Consumed.with(Serdes.String(), Serdes.ByteArray()))
                            // We need to repartition using the correlation id
                            .process(() -> SelectHeaderAsKeyProcessor.builder()
                                    .correlationIdHeaderName(correlationIdHeaderName).build())
                            .repartition(
                                    Repartitioned.with(Serdes.String(), Serdes.ByteArray())
                                            .withName(topicName)
                                            // TODO: should be configurable
                                            .withNumberOfPartitions(4)
                            )
                            .process(processorSupplier, SUPERVISION_STORE);
                    if (outputTopic != null) {
                        stream.to(outputTopic, Produced.with(Serdes.String(), AvroSerdes.get()));
                    }
                }
        );
    }
}
