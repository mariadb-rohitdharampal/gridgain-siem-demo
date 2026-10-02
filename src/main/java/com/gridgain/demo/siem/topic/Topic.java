package com.gridgain.demo.siem.topic;

import java.util.List;

public interface Topic<T> {
    String name();

    void publish(T message);

    default void publishAll(List<T> messages) {
        messages.forEach(this::publish);
    }

    List<T> drain();

    int size();
}
