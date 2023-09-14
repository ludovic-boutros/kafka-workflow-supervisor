package com.example.demo.configuration.model;


import lombok.*;

@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class Topic {
    @NonNull
    private String name;
    @NonNull
    private Type type;

    public enum Type {
        INPUT,
        SUCCESS,
        WARNING,
        ERROR
    }
}
