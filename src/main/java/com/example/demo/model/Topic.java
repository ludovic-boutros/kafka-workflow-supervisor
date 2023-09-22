package com.example.demo.model;


import lombok.*;

@NoArgsConstructor
@Getter
@Setter
@ToString
@EqualsAndHashCode
public class Topic {
    @NonNull
    private String name;

    @EqualsAndHashCode.Exclude
    private boolean isFinal;
}
