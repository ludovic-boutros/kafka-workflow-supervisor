package com.example.demo.model;

import lombok.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@EqualsAndHashCode
@NoArgsConstructor
public class Node {
    private final List<Node> nextNodes = new ArrayList<>();
    private final List<Node> previousNodes = new ArrayList<>();
    // filled later by post processing
    @Setter
    @Getter
    private int depthInWorkflow;
    @Getter
    @Setter
    private boolean isFinal;
    @Getter
    @Setter
    private boolean isInitial;
    // Filled by configuration
    @Getter
    @Setter
    private String name;
    @NonNull
    @Setter
    private List<Topic> inputTopics;

    @NonNull
    @Setter
    private List<Topic> outputTopics;

    public List<Topic> getInputTopics() {
        return Collections.unmodifiableList(inputTopics);
    }

    public List<Topic> getOutputTopics() {
        return Collections.unmodifiableList(outputTopics);
    }

    public List<Node> getNextNodes() {
        return Collections.unmodifiableList(nextNodes);
    }

    public List<Node> getPreviousNodes() {
        return Collections.unmodifiableList(previousNodes);
    }

    public void addNextNode(Node node) {
        nextNodes.add(node);
    }

    public void addPreviousNode(Node node) {
        previousNodes.add(node);
    }

    @Override
    public String toString() {
        return "Node{" +
                "nextNodes=" + nextNodes +
                ", depthInWorkflow=" + depthInWorkflow +
                ", isFinal=" + isFinal +
                ", isInitial=" + isInitial +
                ", name='" + name + '\'' +
                ", inputTopics=" + inputTopics +
                ", outputTopics=" + outputTopics +
                '}';
    }
}
