package com.example.demo.configuration;

import com.example.demo.model.Workflow;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "workflow")
@Getter
@Setter
public class WorkflowConfiguration {
    @NonNull
    private Workflow definition;

    @NonNull
    private String correlationIdHeaderName;

    @NonNull
    private String outputTopic;

    @NonNull
    private String dlqTopic;

    @NonNull
    private String name;

    @NonNull
    private Long purgeSchedulingPeriodSeconds;

    @NonNull
    private Long eventTimeoutSeconds;
}
