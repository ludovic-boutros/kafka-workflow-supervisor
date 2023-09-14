package com.example.demo.topology;

import com.example.demo.configuration.WorkflowConfiguration;
import com.example.demo.data.SupervisionRecord;
import com.example.demo.utils.AvroSerdes;
import com.example.demo.utils.StreamContext;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

import static com.example.demo.utils.PropertyLoader.fromYaml;
import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SupervisorTopologyProviderTest {
    private static final Properties PROPERTIES = new Properties();

    static {
        PROPERTIES.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        PROPERTIES.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        PROPERTIES.put(SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:1234");
        StreamContext.setProperties(PROPERTIES);
    }

    // Workflow topics
    private TestInputTopic<String, byte[]> serviceAInputTopic;
    private TestInputTopic<String, byte[]> serviceASuccessTopic;
    private TestInputTopic<String, byte[]> serviceAErrorTopic;
    private TestInputTopic<String, byte[]> serviceBSuccessTopic;
    private TestInputTopic<String, byte[]> serviceBWarnTopic;
    // Supervisor topics
    private TestOutputTopic<String, SupervisionRecord> supervisorSuccessTopic;
    private TestOutputTopic<String, SupervisionRecord> supervisorWarnTopic;
    private TestOutputTopic<String, SupervisionRecord> supervisorErrorTopic;
    private TopologyTestDriver testDriver;
    private WorkflowConfiguration workflowConfiguration;

    @BeforeEach
    public void init() {
        workflowConfiguration = fromYaml("application-test.yml", "workflow", WorkflowConfiguration.class);

        // fixDefinitionForNullValues(workflowConfiguration.getDefinition());
        var topology = SupervisorTopologyProvider.builder()
                .successOutputTopic(workflowConfiguration.getSuccessOutputTopic())
                .errorOutputTopic(workflowConfiguration.getErrorOutputTopic())
                .warningOutputTopic(workflowConfiguration.getWarningOutputTopic())
                .workflowDefinition(workflowConfiguration.getDefinition())
                .correlationIdHeaderName(workflowConfiguration.getCorrelationIdHeaderName())
                .build()
                .get();

        System.out.println(topology.describe());

        testDriver = new TopologyTestDriver(topology, PROPERTIES);

        serviceAInputTopic = testDriver.createInputTopic("topic-a", Serdes.String().serializer(), Serdes.ByteArray().serializer());
        serviceASuccessTopic = testDriver.createInputTopic("topic-b", Serdes.String().serializer(), Serdes.ByteArray().serializer());
        serviceAErrorTopic = testDriver.createInputTopic("service-a.failed", Serdes.String().serializer(), Serdes.ByteArray().serializer());
        serviceBSuccessTopic = testDriver.createInputTopic("topic-c", Serdes.String().serializer(), Serdes.ByteArray().serializer());
        serviceBWarnTopic = testDriver.createInputTopic("service-b.failed", Serdes.String().serializer(), Serdes.ByteArray().serializer());

        // Supervisor topics
        supervisorSuccessTopic = testDriver.createOutputTopic("my-workflow.success", Serdes.String().deserializer(), AvroSerdes.<SupervisionRecord>get().deserializer());
        supervisorWarnTopic = testDriver.createOutputTopic("my-workflow.warning", Serdes.String().deserializer(), AvroSerdes.<SupervisionRecord>get().deserializer());
        supervisorErrorTopic = testDriver.createOutputTopic("my-workflow.error", Serdes.String().deserializer(), AvroSerdes.<SupervisionRecord>get().deserializer());
    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void shouldNotProduceOutputIfNotFinalState() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(workflowConfiguration.getCorrelationIdHeaderName(), "1234".getBytes(StandardCharsets.UTF_8));
        serviceAInputTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));

        assertTrue(supervisorSuccessTopic.readValuesToList().isEmpty());
        assertTrue(supervisorWarnTopic.readValuesToList().isEmpty());
        assertTrue(supervisorErrorTopic.readValuesToList().isEmpty());
    }

    @Test
    public void shouldProduceOutputInSuccessIfNotFinalStateSuccess() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(workflowConfiguration.getCorrelationIdHeaderName(), "1234".getBytes(StandardCharsets.UTF_8));
        serviceAInputTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));
        serviceASuccessTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));
        serviceBSuccessTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));

        List<SupervisionRecord> succesRecord = supervisorSuccessTopic.readValuesToList();
        assertFalse(succesRecord.isEmpty());
        // TODO: validate content

        assertTrue(supervisorWarnTopic.readValuesToList().isEmpty());
        assertTrue(supervisorErrorTopic.readValuesToList().isEmpty());
    }

    @Test
    public void shouldProduceOutputInWarningIfFinalStateWarning() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(workflowConfiguration.getCorrelationIdHeaderName(), "1234".getBytes(StandardCharsets.UTF_8));
        serviceAInputTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));
        serviceASuccessTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));
        serviceBWarnTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));

        assertTrue(supervisorSuccessTopic.readValuesToList().isEmpty());

        List<SupervisionRecord> warnRecord = supervisorWarnTopic.readValuesToList();
        assertFalse(warnRecord.isEmpty());
        // TODO: validate content

        assertTrue(supervisorErrorTopic.readValuesToList().isEmpty());
    }

    @Test
    public void shouldProduceOutputInErrorIfFinalStateError() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(workflowConfiguration.getCorrelationIdHeaderName(), "1234".getBytes(StandardCharsets.UTF_8));
        serviceAInputTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));
        serviceAErrorTopic.pipeInput(new TestRecord<>("key-1", "value".getBytes(StandardCharsets.UTF_8), headers));

        assertTrue(supervisorSuccessTopic.readValuesToList().isEmpty());
        assertTrue(supervisorWarnTopic.readValuesToList().isEmpty());

        List<SupervisionRecord> errorRecord = supervisorErrorTopic.readValuesToList();
        assertFalse(errorRecord.isEmpty());
        // TODO: validate content

        assertTrue(supervisorErrorTopic.readValuesToList().isEmpty());
    }
}