package com.example.demo;

import com.example.demo.configuration.WorkflowConfiguration;
import com.example.demo.topology.SupervisorTopologyProvider;
import com.example.demo.utils.StreamContext;
import com.ulisesbocchio.jasyptspringboot.annotation.EnableEncryptableProperties;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
@ConfigurationPropertiesScan("com.example.demo.configuration")
@EnableEncryptableProperties
@Slf4j
public class MainApplication implements CommandLineRunner {
    private final MeterRegistry meterRegistry;
    private final ConfigurableApplicationContext applicationContext;
    private final WorkflowConfiguration workflow;

    private KafkaStreams stream;

    public MainApplication(MeterRegistry meterRegistry,
                           ConfigurableApplicationContext applicationContext,
                           WorkflowConfiguration workflow) {
        this.meterRegistry = meterRegistry;
        this.applicationContext = applicationContext;
        this.workflow = workflow;
    }


    public static void main(String[] args) {
        SpringApplication.run(MainApplication.class, args);
    }

    @Override
    public void run(String... args) {
        // Get stream configuration from resource folder
        StreamContext.setEnvironment(applicationContext.getEnvironment());

        // Build topology
        SupervisorTopologyProvider topologyProvider = SupervisorTopologyProvider.builder()
                .workflowDefinition(workflow.getDefinition())
                .correlationIdHeaderName(workflow.getCorrelationIdHeaderName())
                .successOutputTopic(workflow.getSuccessOutputTopic())
                .warningOutputTopic(workflow.getWarningOutputTopic())
                .errorOutputTopic(workflow.getErrorOutputTopic())
                .build();
        stream = new KafkaStreams(topologyProvider.get(), StreamContext.getAsProperties());

        // Define handler in case of unmanaged exception
        stream.setUncaughtExceptionHandler(e -> {
            log.error("Uncaught exception occurred in Kafka Streams. Application will shutdown !", e);

            // Consider REPLACE_THREAD if the exception is retriable
            // Consider SHUTDOWN_APPLICATION if the exception may propagate to other instances after rebalance
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        // Hook the main application to Kafka Stream Lifecycle, to avoid zombie stream application
        stream.setStateListener(((newState, oldState) -> {
            if (newState == KafkaStreams.State.PENDING_ERROR) {
                // Stop the app in case of error
                if (applicationContext.isActive()) {
                    SpringApplication.exit(applicationContext, () -> 1);
                }
            }
        }));

        // Start stream execution
        stream.start();

        // Ensure your app respond gracefully to external shutdown signal
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}

