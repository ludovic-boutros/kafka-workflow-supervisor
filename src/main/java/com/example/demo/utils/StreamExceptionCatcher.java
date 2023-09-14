package com.example.demo.utils;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

public class StreamExceptionCatcher {

    public static final String EXCEPTION_BRANCH_NAME = "exception";
    public static final String SUCCESS_BRANCH_NAME = "success";
    public static String DLQ_NAME = "dlq.topic";

    /**
     *
     */
    public static <SK, SV, FV> KStream<SK, SV> check(KStream<SK, MayBeException<SV, FV>> stream) {
        var branches =
                stream.split()
                        .branch((key, value) -> value.streamException != null, Branched.as(EXCEPTION_BRANCH_NAME))
                        .defaultBranch(Branched.as(SUCCESS_BRANCH_NAME));

        branches.get(EXCEPTION_BRANCH_NAME)
                .map((k, v) -> KeyValue.pair(k.toString(), v.streamException.toString()))
                .to(StreamContext.get(DLQ_NAME), Produced.with(Serdes.String(), Serdes.String()));

        return branches.get(SUCCESS_BRANCH_NAME)
                .mapValues(v -> v.streamValue);
    }
}