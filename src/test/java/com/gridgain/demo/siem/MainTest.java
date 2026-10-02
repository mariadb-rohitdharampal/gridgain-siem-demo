package com.gridgain.demo.siem;

import org.junit.jupiter.api.Test;
import org.apache.kafka.common.errors.WakeupException;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MainTest {
    @Test
    void gridGainReducerAcceptsOneNodeDebugModeFromCli() {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
        try {
            Main.main(new String[]{
                    "--events", "40",
                    "--target-reduction", "40",
                    "--reducer", "gridgain",
                    "--ignite-nodes", "1"
            });
        } finally {
            System.setOut(originalOut);
        }

        String report = output.toString(StandardCharsets.UTF_8);
        assertTrue(report.contains("Reducer mode: gridgain / embedded Apache Ignite"));
        assertTrue(report.contains("- embedded node count: 1"));
        assertTrue(report.contains("- backup count: 0"));
        assertTrue(report.contains("Security events preserved:"));
    }

    @Test
    void kafkaSimulationAcceptsStreamingWindowArguments() {
        PrintStream originalOut = System.out;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        System.setOut(new PrintStream(output, true, StandardCharsets.UTF_8));
        try {
            Main.main(new String[]{
                    "--mode", "kafka-sim",
                    "--events", "40",
                    "--target-reduction", "40",
                    "--reducer", "inmemory",
                    "--streaming",
                    "--window-seconds", "60"
            });
        } finally {
            System.setOut(originalOut);
        }

        String report = output.toString(StandardCharsets.UTF_8);
        assertTrue(report.contains("Streaming mode: simulated time windows"));
        assertTrue(report.contains("Window size: 60 seconds"));
        assertTrue(report.contains("Window Metrics"));
    }

    @Test
    void streamingRequiresKafkaSimulationMode() {
        assertThrows(IllegalArgumentException.class, () -> Main.main(new String[]{
                "--mode", "direct",
                "--streaming"
        }));
    }

    @Test
    void kafkaReduceDefaultsToGridgainReducer() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce"
        });

        assertEquals(Main.ReducerType.GRIDGAIN, config.reducerType());
    }

    @Test
    void kafkaReduceAcceptsConsumerGroupId() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--consumer-group-id", "siem-demo-test"
        });

        assertEquals("siem-demo-test", config.consumerGroupId());
        assertEquals(Main.ReducerType.GRIDGAIN, config.reducerType());
    }

    @Test
    void kafkaReduceAcceptsVerboseWindowsFlag() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--verbose-windows"
        });

        assertTrue(config.verboseWindows());
        assertEquals(Main.ReducerType.GRIDGAIN, config.reducerType());
    }

    @Test
    void kafkaReduceAcceptsContinuousMode() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--continuous"
        });

        assertTrue(config.continuous());
        assertEquals(60L, config.runSeconds());
        assertEquals(10L, config.metricsIntervalSeconds());
        assertEquals(Main.ReducerType.GRIDGAIN, config.reducerType());
    }

    @Test
    void kafkaReduceTreatsRunSecondsZeroAsContinuousMode() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--run-seconds", "0"
        });

        assertTrue(config.continuous());
        assertEquals(0L, config.runSeconds());
    }

    @Test
    void kafkaReduceAcceptsMetricsInterval() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--continuous",
                "--metrics-interval-seconds", "15"
        });

        assertTrue(config.continuous());
        assertEquals(15L, config.metricsIntervalSeconds());
    }

    @Test
    void kafkaReduceAcceptsDashboardPort() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--continuous",
                "--dashboard",
                "--dashboard-port", "9090"
        });

        assertTrue(config.dashboardEnabled());
        assertEquals(9090, config.dashboardPort());
    }

    @Test
    void kafkaReduceDashboardRequiresContinuousMode() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--dashboard"
        }));

        assertTrue(ex.getMessage().contains("--mode kafka-reduce --dashboard requires --continuous or --run-seconds 0."));
    }

    @Test
    void runSecondsZeroAllowsKafkaReduceDashboard() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--run-seconds", "0",
                "--dashboard",
                "--dashboard-port", "0"
        });

        assertTrue(config.continuous());
        assertTrue(config.dashboardEnabled());
        assertEquals(0, config.dashboardPort());
    }

    @Test
    void gracefulKafkaShutdownCompletionMessageIsEmitted() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        Main.printGracefulKafkaShutdownComplete(new PrintStream(output, true, StandardCharsets.UTF_8));

        assertEquals(
                Main.GRACEFUL_KAFKA_SHUTDOWN_COMPLETE + System.lineSeparator(),
                output.toString(StandardCharsets.UTF_8)
        );
    }

    @Test
    void expectedKafkaShutdownRequiresStopRequest() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-reduce",
                "--continuous"
        });
        com.gridgain.demo.siem.kafka.KafkaReductionRunner runner =
                new com.gridgain.demo.siem.kafka.KafkaReductionRunner(Duration.ofSeconds(60));

        assertFalse(Main.isExpectedKafkaShutdown(config, runner, new WakeupException()));

        runner.requestStop();

        assertTrue(Main.isExpectedKafkaShutdown(config, runner, new WakeupException()));
    }

    @Test
    void continuousKafkaOptionsRequireKafkaReduceMode() {
        assertThrows(IllegalArgumentException.class, () -> Main.DemoConfig.parse(new String[]{
                "--mode", "direct",
                "--continuous"
        }));
        assertThrows(IllegalArgumentException.class, () -> Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-produce",
                "--run-seconds", "0"
        }));
        assertThrows(IllegalArgumentException.class, () -> Main.DemoConfig.parse(new String[]{
                "--mode", "kafka-sim",
                "--metrics-interval-seconds", "5"
        }));
    }

    @Test
    void existingDirectModeDefaultReducerRemainsInMemory() {
        Main.DemoConfig config = Main.DemoConfig.parse(new String[]{
                "--mode", "direct"
        });

        assertEquals(Main.ReducerType.INMEMORY, config.reducerType());
        assertFalse(config.verboseWindows());
        assertFalse(config.continuous());
    }
}
