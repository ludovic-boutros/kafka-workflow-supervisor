package com.example.demo.model;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

public class Workflow extends ArrayList<Node> {

    public void computeWorkflowTreeDepth() {
        Multimap<Topic, Node> nodesPerInputTopic = LinkedListMultimap.create();
        Multimap<Topic, Node> nodesPerOutputTopic = LinkedListMultimap.create();

        forEach(node -> node.getInputTopics()
                .forEach(inputTopic -> nodesPerInputTopic.put(inputTopic, node)));

        forEach(node -> node.getOutputTopics()
                .forEach(outputTopic -> nodesPerOutputTopic.put(outputTopic, node)));

        // Get output topics which are not input topics and update nodes to set final tag
        nodesPerOutputTopic.entries().stream()
                .filter(e -> !nodesPerInputTopic.containsKey(e.getKey()))
                .forEach(e -> {
                    // This node is a potential candidate for final state.
                    // If it finally contains a non final topic, the node will be unflagged in the next step
                    e.getValue().setFinal(true);
                    e.getKey().setFinal(true);
                });

        // Get input topics which are also output topics and update nodes to set final tag
        nodesPerOutputTopic.entries().stream()
                .filter(e -> nodesPerInputTopic.containsKey(e.getKey()))
                .forEach(e -> e.getValue().setFinal(false));

        // Get input topics which are not output topics and update nodes to set initial tag
        nodesPerInputTopic.entries().stream()
                .filter(e -> !nodesPerOutputTopic.containsKey(e.getKey()))
                .forEach(e -> {
                    e.getValue().setInitial(true);
                    // Now go through the nodes and fill additional workflow tree data
                    // We do NOT support multiple paths with different depth
                    fillDepth(e.getValue(), 1, nodesPerInputTopic);
                });

        // Then we can have nodes without any input topic or output topic
        // They are initial and final topics respectively
        stream().filter(node -> node.getInputTopics().isEmpty())
                .forEach(node -> node.setInitial(true));

        stream().filter(node -> node.getOutputTopics().isEmpty())
                .forEach(node -> node.setFinal(true));
    }

    private void fillDepth(Node node, int depth, Multimap<Topic, Node> nodesPerInputTopic) {
        if (node.getDepthInWorkflow() != 0 && node.getDepthInWorkflow() != depth) {
            throw new IllegalArgumentException("The workflow tree is too complex and contains multiple paths/nodes with different depths: " + node + " -> " + depth);
        }
        node.setDepthInWorkflow(depth);
        final int nextDepth = depth + 1;

        node.getOutputTopics().stream()
                .map(nodesPerInputTopic::get)
                .filter(Predicate.not(Collection::isEmpty))
                .flatMap(Collection::stream)
                .forEach(nextNode -> {
                    node.addNextNode(nextNode);
                    nextNode.addPreviousNode(node);
                    fillDepth(nextNode, nextDepth, nodesPerInputTopic);
                });
    }
}
