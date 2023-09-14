package com.example.demo.topology;

import com.example.demo.configuration.model.Node;
import com.example.demo.configuration.model.Topic;
import com.example.demo.configuration.model.Workflow;
import com.example.demo.data.SupervisionRecord;
import com.example.demo.topology.processor.UpdateSupervisionStateProcessor;
import com.example.demo.utils.AvroSerdes;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.List;
import java.util.function.Supplier;

import static com.example.demo.Constants.SUPERVISION_STORE;

@Slf4j
public class SupervisorTopologyProvider implements Supplier<Topology> {
    private final Workflow workflowDefinition;
    private final String correlationIdHeaderName;

    private final String successOutputTopic;
    private final String warningOutputTopic;
    private final String errorOutputTopic;

    @Builder
    public SupervisorTopologyProvider(Workflow workflowDefinition,
                                      String correlationIdHeaderName,
                                      String successOutputTopic,
                                      String warningOutputTopic,
                                      String errorOutputTopic) {
        this.workflowDefinition = workflowDefinition;
        this.correlationIdHeaderName = correlationIdHeaderName;
        this.successOutputTopic = successOutputTopic;
        this.warningOutputTopic = warningOutputTopic;
        this.errorOutputTopic = errorOutputTopic;
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
            stream(builder, () -> getProcessor(node, false), node::getInputTopics);

            // Consume and process success topics
            stream(builder, () -> getProcessor(node, node.isFinal()), node::getSuccessOutputTopics, successOutputTopic);

            // Consume and process warning topics
            stream(builder, () -> getProcessor(node, true), node::getWarningOutputTopics, warningOutputTopic);

            // Consume and process error topics
            stream(builder, () -> getProcessor(node, true), node::getErrorOutputTopics, errorOutputTopic);
        });

        return builder.build();
    }

    private UpdateSupervisionStateProcessor getProcessor(Node node, boolean isFinal) {
        return UpdateSupervisionStateProcessor.builder()
                .correlationIdHeaderName(correlationIdHeaderName)
                .nodeName(node.getName())
                .isFinal(isFinal)
                .build();
    }

    private void stream(StreamsBuilder builder,
                        ProcessorSupplier<String, byte[], String, SupervisionRecord> processorSupplier,
                        Supplier<List<Topic>> inputTopicNameSupplier,
                        String outputTopic) {
        List<String> topicNames = getTopicNames(inputTopicNameSupplier);
        if (!topicNames.isEmpty()) {
            builder.stream(topicNames, Consumed.with(Serdes.String(), Serdes.ByteArray()))
                    .process(processorSupplier, SUPERVISION_STORE)
                    .to(outputTopic, Produced.with(Serdes.String(), AvroSerdes.get()));
        }
    }

    private void stream(StreamsBuilder builder,
                        ProcessorSupplier<String, byte[], String, SupervisionRecord> processorSupplier,
                        Supplier<List<Topic>> inputTopicNameSupplier) {
        List<String> topicNames = getTopicNames(inputTopicNameSupplier);

        if (topicNames.isEmpty()) {
            throw new IllegalArgumentException("Nodes must have at least one input topic.");
        }
        builder.stream(topicNames, Consumed.with(Serdes.String(), Serdes.ByteArray()))
                .process(processorSupplier, SUPERVISION_STORE);
    }
}
