package com.gridgain.demo.siem.generator;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;

import java.util.List;

public interface LogGenerator {
    LogSourceType sourceType();

    List<LogEvent> generate(int count);
}
