package com.example.demo.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import java.util.concurrent.atomic.AtomicInteger;

public class StreamExceptionCatcher<SK, SV, FV> {

    public static final String EXCEPTION_BRANCH_NAME = "exception";
    public static final String SUCCESS_BRANCH_NAME = "success";
    public static final String BRANCH_PREFIX = "exception-catcher-";

    private static final AtomicInteger CATCHER_INSTANCE_COUNT = new AtomicInteger();
    private final String name;
    private final int catcherInstanceCount;

    public StreamExceptionCatcher(String name) {
        this.catcherInstanceCount = CATCHER_INSTANCE_COUNT.incrementAndGet();
        this.name = name;
    }

    public static <SK, SV, FV> StreamExceptionCatcher<SK, SV, FV> of(String name) {
        return new StreamExceptionCatcher<>(name);
    }

    /**
     *
     */
    public KStream<SK, SV> check(KStream<SK, MayBeException<SV, FV>> stream, String dlqTopic) {
        var branches =
                stream.split(Named.as(getBranchPrefix(catcherInstanceCount)))
                        .branch((key, value) -> value.streamException != null, Branched.as(EXCEPTION_BRANCH_NAME))
                        .defaultBranch(Branched.as(SUCCESS_BRANCH_NAME));

        branches.get(getBranchPrefix(catcherInstanceCount) + EXCEPTION_BRANCH_NAME)
                .map((k, v) -> KeyValue.pair(k.toString(), v.streamException.toString()))
                .to(dlqTopic, Produced.with(Serdes.String(), Serdes.String()));

        return branches.get(getBranchPrefix(catcherInstanceCount) + SUCCESS_BRANCH_NAME)
                .mapValues(v -> v.streamValue);
    }

    private String getBranchPrefix(int catcherInstanceCount) {
        return BRANCH_PREFIX + name + "-" + catcherInstanceCount + "-";
    }
}