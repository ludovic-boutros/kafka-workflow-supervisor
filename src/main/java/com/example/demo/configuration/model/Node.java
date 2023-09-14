package com.example.demo.configuration.model;

import lombok.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


@EqualsAndHashCode
@NoArgsConstructor
public class Node {
    @Getter
    @Setter
    private String name;
    @NonNull
    @Setter
    private List<Topic> inputTopics;
    @NonNull
    @Setter
    private List<Topic> successOutputTopics;
    @NonNull
    private List<Topic> warningOutputTopics = new ArrayList<>();
    @NonNull
    private List<Topic> errorOutputTopics = new ArrayList<>();
    @Getter
    private boolean isFinal;

    public List<Topic> getInputTopics() {
        return Collections.unmodifiableList(inputTopics);
    }

    public List<Topic> getSuccessOutputTopics() {
        return Collections.unmodifiableList(successOutputTopics);
    }

    public List<Topic> getWarningOutputTopics() {
        return Collections.unmodifiableList(warningOutputTopics);
    }

    public void setWarningOutputTopics(List<Topic> warningOutputTopics) {
        if (warningOutputTopics != null) {
            this.warningOutputTopics = warningOutputTopics;
        }
    }

    public void setIsFinal(boolean value) {
        isFinal = value;
    }

    public List<Topic> getErrorOutputTopics() {
        return Collections.unmodifiableList(errorOutputTopics);
    }

    public void setErrorOutputTopics(List<Topic> errorOutputTopics) {
        if (errorOutputTopics != null) {
            this.errorOutputTopics = errorOutputTopics;
        }
    }

    @Override
    public String toString() {
        return "Node{" +
                "name='" + name + '\'' +
                ", inputTopics=" + inputTopics +
                ", successOutputTopics=" + successOutputTopics +
                ", warningOutputTopics=" + warningOutputTopics +
                ", errorOutputTopics=" + errorOutputTopics +
                '}';
    }
}
