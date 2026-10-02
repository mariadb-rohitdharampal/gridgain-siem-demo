package com.gridgain.demo.siem.topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class InMemoryTopic<T> implements Topic<T> {
    private final String name;
    private final List<T> messages = new ArrayList<>();

    public InMemoryTopic(String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void publish(T message) {
        messages.add(Objects.requireNonNull(message, "message"));
    }

    @Override
    public List<T> drain() {
        List<T> drained = List.copyOf(messages);
        messages.clear();
        return drained;
    }

    @Override
    public int size() {
        return messages.size();
    }
}
