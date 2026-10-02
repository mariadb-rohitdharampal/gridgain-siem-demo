package com.gridgain.demo.siem;

import com.gridgain.demo.siem.dashboard.DashboardModel;
import com.gridgain.demo.siem.dashboard.DashboardRenderer;
import com.gridgain.demo.siem.dashboard.SavingsEstimate;
import com.gridgain.demo.siem.dashboard.SavingsEstimator;
import com.gridgain.demo.siem.dashboard.live.LiveKafkaDashboardProgressReporter;
import com.gridgain.demo.siem.dashboard.live.LiveKafkaDashboardServer;
import com.gridgain.demo.siem.dashboard.live.LiveKafkaDashboardState;
import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.generator.ActiveDirectoryLogGenerator;
import com.gridgain.demo.siem.generator.CloudZeroTrustJsonLogGenerator;
import com.gridgain.demo.siem.generator.DnsSyslogLogGenerator;
import com.gridgain.demo.siem.generator.FirewallCefLogGenerator;
import com.gridgain.demo.siem.generator.LogGenerator;
import com.gridgain.demo.siem.kafka.CompositeKafkaReductionProgressReporter;
import com.gridgain.demo.siem.kafka.KafkaReductionProgressReporter;
import com.gridgain.demo.siem.kafka.KafkaIntegrationConfig;
import com.gridgain.demo.siem.kafka.KafkaProduceResult;
import com.gridgain.demo.siem.kafka.KafkaProducerRunner;
import com.gridgain.demo.siem.kafka.KafkaReductionResult;
import com.gridgain.demo.siem.kafka.KafkaReductionRunOptions;
import com.gridgain.demo.siem.kafka.KafkaReductionRunner;
import com.gridgain.demo.siem.kafka.ConsoleKafkaReductionProgressReporter;
import com.gridgain.demo.siem.metrics.DemoMetrics;
import com.gridgain.demo.siem.pipeline.KafkaSimulationPipeline;
import com.gridgain.demo.siem.pipeline.PipelineResult;
import com.gridgain.demo.siem.pipeline.WindowedKafkaSimulationPipeline;
import com.gridgain.demo.siem.reduction.ReductionResult;
import com.gridgain.demo.siem.reduction.ReductionService;
import com.gridgain.demo.siem.reduction.gridgain.GridGainReductionService;
import com.gridgain.demo.siem.reduction.inmemory.InMemoryReductionService;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public final class Main {
    private static final int DEFAULT_TOTAL_EVENTS = 10_000;
    private static final double DEFAULT_TARGET_REDUCTION_PERCENTAGE = ReductionService.DEFAULT_TARGET_REDUCTION_PERCENTAGE;
    private static final String DEFAULT_DASHBOARD_FILE = "target/demo-dashboard.html";
    private static final long DEFAULT_AVERAGE_EVENT_BYTES = 1_200L;
    private static final double DEFAULT_COST_PER_TB = 500.0;
    private static final int DEFAULT_RETENTION_DAYS = 30;
    private static final int DEFAULT_IGNITE_NODES = 3;
    private static final int DEFAULT_WINDOW_SECONDS = 60;
    private static final String DEFAULT_BOOTSTRAP_SERVERS = KafkaIntegrationConfig.DEFAULT_BOOTSTRAP_SERVERS;
    private static final long DEFAULT_POLL_MS = KafkaIntegrationConfig.DEFAULT_POLL_MS;
    private static final long DEFAULT_RUN_SECONDS = KafkaIntegrationConfig.DEFAULT_RUN_SECONDS;
    private static final long DEFAULT_METRICS_INTERVAL_SECONDS = 10L;
    private static final int DEFAULT_DASHBOARD_PORT = 8080;
    private static final String DEFAULT_CONSUMER_GROUP_ID = KafkaIntegrationConfig.DEFAULT_CONSUMER_GROUP_ID;
    private static final int DEFAULT_TOPIC_PARTITIONS = KafkaIntegrationConfig.DEFAULT_TOPIC_PARTITIONS;
    private static final short DEFAULT_TOPIC_REPLICATION_FACTOR = KafkaIntegrationConfig.DEFAULT_TOPIC_REPLICATION_FACTOR;
    static final String GRACEFUL_KAFKA_SHUTDOWN_COMPLETE = "Graceful Kafka reducer shutdown complete.";
    private static final String KAFKA_SIMULATION_LABEL = "Demo mode: kafka-sim (simulated Kafka topics, no Kafka broker)";
    private static final String STREAMING_SIMULATION_LABEL = "Streaming mode: simulated time windows, no Kafka consumer groups, offsets, retries, or delivery semantics";

    private Main() {
    }

    public static void main(String[] args) {
        DemoConfig config = DemoConfig.parse(args);
        List<LogGenerator> generators = List.of(
                new FirewallCefLogGenerator(),
                new DnsSyslogLogGenerator(),
                new ActiveDirectoryLogGenerator(),
                new CloudZeroTrustJsonLogGenerator()
        );

        if (config.demoMode() == DemoMode.KAFKA_SIM) {
            runKafkaSimulation(config, generators);
        } else if (config.demoMode() == DemoMode.KAFKA_PRODUCE) {
            runKafkaProduce(config, generators);
        } else if (config.demoMode() == DemoMode.KAFKA_REDUCE) {
            runKafkaReduce(config);
        } else {
            runDirect(config, generators);
        }
    }

    private static void runDirect(DemoConfig config, List<LogGenerator> generators) {
        List<LogEvent> rawEvents = new ArrayList<>();
        for (int i = 0; i < generators.size(); i++) {
            int eventCount = eventsForSource(config.totalEvents(), generators.size(), i);
            rawEvents.addAll(generators.get(i).generate(eventCount));
        }

        ReductionResult result;
        Duration processingTime;
        String reducerMode;
        Map<String, String> proofMetrics;
        try (ReductionService reductionService = createReductionService(config.reducerType(), config.igniteNodeCount())) {
            reducerMode = reductionService.reducerMode();
            long startNanos = System.nanoTime();
            result = reductionService.reduce(rawEvents, config.targetReductionPercentage());
            processingTime = Duration.ofNanos(System.nanoTime() - startNanos);
            proofMetrics = reductionService.proofMetrics();
        }

        DemoMetrics metrics = DemoMetrics.from(rawEvents, result.reducedEvents(), processingTime, reducerMode, proofMetrics);
        System.out.println("Target reduction: %.2f%%".formatted(config.targetReductionPercentage()));
        System.out.println(metrics.toReport());
        renderDashboardIfRequested(config, metrics, false);
    }

    private static void runKafkaSimulation(DemoConfig config, List<LogGenerator> generators) {
        PipelineResult result;
        try (ReductionService reductionService = createReductionService(config.reducerType(), config.igniteNodeCount())) {
            result = config.streamingEnabled()
                    ? new WindowedKafkaSimulationPipeline(generators, Duration.ofSeconds(config.windowSeconds()))
                    .run(config.totalEvents(), config.targetReductionPercentage(), reductionService)
                    : new KafkaSimulationPipeline(generators)
                    .run(config.totalEvents(), config.targetReductionPercentage(), reductionService);
        }

        DemoMetrics metrics = DemoMetrics.from(
                result.rawEvents(),
                result.reducedEvents(),
                result.processingTime(),
                result.reducerMode(),
                result.proofMetrics(),
                result.topicMetrics(),
                result.windowMetrics()
        );
        System.out.println("Target reduction: %.2f%%".formatted(config.targetReductionPercentage()));
        System.out.println(KAFKA_SIMULATION_LABEL);
        if (config.streamingEnabled()) {
            System.out.println(STREAMING_SIMULATION_LABEL);
            System.out.println("Window size: %d seconds".formatted(config.windowSeconds()));
        }
        System.out.println(metrics.toReport());
        renderDashboardIfRequested(config, metrics, true);
    }

    private static void runKafkaProduce(DemoConfig config, List<LogGenerator> generators) {
        KafkaIntegrationConfig kafkaConfig = new KafkaIntegrationConfig(
                config.bootstrapServers(),
                config.pollMs(),
                config.runSeconds(),
                config.createTopics(),
                KafkaIntegrationConfig.defaults().rawTopicNames(),
                KafkaIntegrationConfig.defaults().cleanTopicNames(),
                config.topicPartitions(),
                config.topicReplicationFactor(),
                config.consumerGroupId(),
                KafkaIntegrationConfig.DEFAULT_AUTO_OFFSET_RESET,
                KafkaIntegrationConfig.DEFAULT_COMMIT_STRATEGY
        );

        System.out.println("Kafka producer mode: real Kafka raw topics only");
        System.out.println("No reduction consumer is running in K2");
        System.out.println("Bootstrap servers: " + kafkaConfig.bootstrapServers());
        System.out.println("Create topics: " + kafkaConfig.createTopics());
        if (kafkaConfig.createTopics()) {
            System.out.println("Topic partitions: " + kafkaConfig.topicPartitions());
            System.out.println("Topic replication factor: " + kafkaConfig.topicReplicationFactor());
        }

        KafkaProduceResult result = new KafkaProducerRunner(generators).run(config.totalEvents(), kafkaConfig);

        System.out.println("Produced events: " + result.totalProduced());
        System.out.println("Raw topic counts:");
        result.perTopicProduced().forEach((topicName, count) ->
                System.out.println("- %s: %d".formatted(topicName, count)));
    }

    private static void runKafkaReduce(DemoConfig config) {
        KafkaIntegrationConfig kafkaConfig = new KafkaIntegrationConfig(
                config.bootstrapServers(),
                config.pollMs(),
                config.runSeconds(),
                config.createTopics(),
                KafkaIntegrationConfig.defaults().rawTopicNames(),
                KafkaIntegrationConfig.defaults().cleanTopicNames(),
                config.topicPartitions(),
                config.topicReplicationFactor(),
                config.consumerGroupId(),
                KafkaIntegrationConfig.DEFAULT_AUTO_OFFSET_RESET,
                KafkaIntegrationConfig.DEFAULT_COMMIT_STRATEGY
        );

        System.out.println("Kafka reducer mode: real Kafka raw topics to clean topics");
        if (config.continuous()) {
            System.out.println("Runtime: continuous until Ctrl+C");
            System.out.println("Metrics interval: %d seconds".formatted(config.metricsIntervalSeconds()));
            System.out.println("Shutdown: Ctrl+C closes windows, flushes clean output, commits offsets, and closes Kafka/Ignite resources");
        } else {
            System.out.println("Bounded runtime: %d seconds".formatted(kafkaConfig.runSeconds()));
        }
        System.out.println("Poll interval: %d ms".formatted(kafkaConfig.pollMs()));
        System.out.println("Window size: %d seconds".formatted(config.windowSeconds()));
        System.out.println("Bootstrap servers: " + kafkaConfig.bootstrapServers());
        System.out.println("Consumer group id: " + kafkaConfig.consumerGroupId());
        System.out.println("Create topics: " + kafkaConfig.createTopics());
        System.out.println("Offset strategy: manual synchronous commits, at-least-once output");
        System.out.println(liveDashboardEnabled(config)
                ? "Dashboard integration: local live dashboard enabled"
                : "Dashboard integration: not enabled for real Kafka mode");
        if (config.verboseWindows()) {
            System.out.println("Verbose window diagnostics: enabled");
        }

        LiveKafkaDashboardState dashboardState = liveDashboardEnabled(config) ? new LiveKafkaDashboardState() : null;
        try (
                LiveKafkaDashboardServer dashboardServer = startLiveDashboardIfRequested(config, dashboardState);
                ReductionService reductionService = createReductionService(config.reducerType(), config.igniteNodeCount())
        ) {
            KafkaReductionRunner runner = new KafkaReductionRunner(Duration.ofSeconds(config.windowSeconds()));
            CountDownLatch stopped = new CountDownLatch(1);
            Thread shutdownHook = continuousShutdownHook(config, runner, stopped);
            if (shutdownHook != null) {
                Runtime.getRuntime().addShutdownHook(shutdownHook);
            }
            try {
                KafkaReductionRunOptions runOptions = config.continuous()
                        ? KafkaReductionRunOptions.continuous(Duration.ofSeconds(config.metricsIntervalSeconds()))
                        : KafkaReductionRunOptions.bounded(Duration.ofSeconds(kafkaConfig.runSeconds()));
                KafkaReductionResult result = runner.run(
                        kafkaConfig,
                        config.targetReductionPercentage(),
                        reductionService,
                        runOptions,
                        kafkaReductionProgressReporter(config, dashboardState)
                );
                System.out.println(result.toReport(config.verboseWindows()));
                if (config.continuous() && runner.stopRequested()) {
                    printGracefulKafkaShutdownComplete(System.out);
                }
            } catch (RuntimeException ex) {
                if (!isExpectedKafkaShutdown(config, runner, ex)) {
                    throw ex;
                }
                preserveInterruptFlagIfNeeded(ex);
                System.out.println("Shutdown summary: Kafka reducer stop was requested before a final report was available.");
                System.out.println("Any uncommitted offsets may be replayed on the next run.");
                printGracefulKafkaShutdownComplete(System.out);
            } finally {
                stopped.countDown();
                removeShutdownHook(shutdownHook);
            }
        }
    }

    private static KafkaReductionProgressReporter kafkaReductionProgressReporter(
            DemoConfig config,
            LiveKafkaDashboardState dashboardState
    ) {
        if (!config.continuous()) {
            return KafkaReductionProgressReporter.noop();
        }
        KafkaReductionProgressReporter consoleReporter = new ConsoleKafkaReductionProgressReporter();
        if (!liveDashboardEnabled(config)) {
            return consoleReporter;
        }
        return new CompositeKafkaReductionProgressReporter(List.of(
                consoleReporter,
                new LiveKafkaDashboardProgressReporter(dashboardState)
        ));
    }

    private static LiveKafkaDashboardServer startLiveDashboardIfRequested(
            DemoConfig config,
            LiveKafkaDashboardState dashboardState
    ) {
        if (!liveDashboardEnabled(config)) {
            return null;
        }

        try {
            LiveKafkaDashboardServer server = new LiveKafkaDashboardServer(dashboardState, config.dashboardPort()).start();
            System.out.println("Live dashboard: http://localhost:%d/".formatted(server.port()));
            return server;
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to start live dashboard on port " + config.dashboardPort(), ex);
        }
    }

    private static boolean liveDashboardEnabled(DemoConfig config) {
        return config.demoMode() == DemoMode.KAFKA_REDUCE && config.continuous() && config.dashboardEnabled();
    }

    static void printGracefulKafkaShutdownComplete(PrintStream out) {
        out.println(GRACEFUL_KAFKA_SHUTDOWN_COMPLETE);
    }

    static boolean isExpectedKafkaShutdown(DemoConfig config, KafkaReductionRunner runner, RuntimeException ex) {
        return config.continuous()
                && runner.stopRequested()
                && (hasCause(ex, WakeupException.class) || hasCause(ex, InterruptedException.class));
    }

    private static boolean hasCause(Throwable ex, Class<? extends Throwable> type) {
        Throwable current = ex;
        while (current != null) {
            if (type.isInstance(current)) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static void preserveInterruptFlagIfNeeded(Throwable ex) {
        if (hasCause(ex, InterruptedException.class)) {
            Thread.currentThread().interrupt();
        }
    }

    private static Thread continuousShutdownHook(
            DemoConfig config,
            KafkaReductionRunner runner,
            CountDownLatch stopped
    ) {
        if (!config.continuous()) {
            return null;
        }

        return new Thread(() -> {
            System.out.println("Ctrl+C received; requesting graceful Kafka reducer shutdown...");
            runner.requestStop();
            try {
                if (!stopped.await(60, TimeUnit.SECONDS)) {
                    System.err.println("Kafka reducer shutdown is still in progress after 60 seconds.");
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, "kafka-reducer-shutdown");
    }

    private static void removeShutdownHook(Thread shutdownHook) {
        if (shutdownHook == null) {
            return;
        }

        try {
            Runtime.getRuntime().removeShutdownHook(shutdownHook);
        } catch (IllegalStateException ignored) {
            // JVM shutdown is already running; the hook will finish naturally.
        }
    }

    private static void renderDashboardIfRequested(DemoConfig config, DemoMetrics metrics, boolean kafkaSimulationMode) {
        if (!config.dashboardEnabled()) {
            return;
        }

        SavingsEstimate savingsEstimate = new SavingsEstimator().estimate(
                metrics,
                config.averageEventBytes(),
                config.costPerTb(),
                config.retentionDays()
        );
        DashboardModel model = new DashboardModel(
                metrics,
                savingsEstimate,
                kafkaSimulationMode,
                config.averageEventBytes(),
                config.costPerTb()
        );

        try {
            Path outputPath = new DashboardRenderer().render(model, Path.of(config.dashboardFile()));
            System.out.println("Dashboard written to " + outputPath.toAbsolutePath());
        } catch (IOException ex) {
            throw new IllegalStateException("Failed to write dashboard to " + config.dashboardFile(), ex);
        }
    }

    private static int eventsForSource(int totalEvents, int sourceCount, int sourceIndex) {
        int baseEvents = totalEvents / sourceCount;
        int remainder = totalEvents % sourceCount;
        return baseEvents + (sourceIndex < remainder ? 1 : 0);
    }

    private static ReductionService createReductionService(ReducerType reducerType, int igniteNodeCount) {
        return switch (reducerType) {
            case INMEMORY -> new InMemoryReductionService();
            case GRIDGAIN -> new GridGainReductionService(igniteNodeCount);
        };
    }

    enum ReducerType {
        INMEMORY,
        GRIDGAIN
    }

    enum DemoMode {
        DIRECT,
        KAFKA_SIM,
        KAFKA_PRODUCE,
        KAFKA_REDUCE
    }

    record DemoConfig(
            int totalEvents,
            double targetReductionPercentage,
            ReducerType reducerType,
            DemoMode demoMode,
            boolean dashboardEnabled,
            String dashboardFile,
            long averageEventBytes,
            double costPerTb,
            int retentionDays,
            int igniteNodeCount,
            boolean streamingEnabled,
            int windowSeconds,
            String bootstrapServers,
            long pollMs,
            long runSeconds,
            long metricsIntervalSeconds,
            String consumerGroupId,
            boolean createTopics,
            int topicPartitions,
            short topicReplicationFactor,
            boolean verboseWindows,
            boolean continuous,
            int dashboardPort
    ) {
        static DemoConfig parse(String[] args) {
            int totalEvents = DEFAULT_TOTAL_EVENTS;
            double targetReductionPercentage = DEFAULT_TARGET_REDUCTION_PERCENTAGE;
            ReducerType reducerType = ReducerType.INMEMORY;
            DemoMode demoMode = DemoMode.DIRECT;
            boolean dashboardEnabled = false;
            String dashboardFile = DEFAULT_DASHBOARD_FILE;
            long averageEventBytes = DEFAULT_AVERAGE_EVENT_BYTES;
            double costPerTb = DEFAULT_COST_PER_TB;
            int retentionDays = DEFAULT_RETENTION_DAYS;
            int igniteNodeCount = DEFAULT_IGNITE_NODES;
            boolean streamingEnabled = false;
            int windowSeconds = DEFAULT_WINDOW_SECONDS;
            String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
            long pollMs = DEFAULT_POLL_MS;
            long runSeconds = DEFAULT_RUN_SECONDS;
            long metricsIntervalSeconds = DEFAULT_METRICS_INTERVAL_SECONDS;
            String consumerGroupId = DEFAULT_CONSUMER_GROUP_ID;
            boolean createTopics = KafkaIntegrationConfig.DEFAULT_CREATE_TOPICS;
            int topicPartitions = DEFAULT_TOPIC_PARTITIONS;
            short topicReplicationFactor = DEFAULT_TOPIC_REPLICATION_FACTOR;
            boolean verboseWindows = false;
            boolean continuous = false;
            boolean reducerExplicitlySet = false;
            boolean metricsIntervalExplicitlySet = false;
            int dashboardPort = DEFAULT_DASHBOARD_PORT;

            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--streaming", "--windowed" -> streamingEnabled = true;
                    case "--create-topics" -> createTopics = true;
                    case "--verbose-windows" -> verboseWindows = true;
                    case "--continuous" -> continuous = true;
                    case "--dashboard" -> dashboardEnabled = true;
                    case "--dashboard-file" -> {
                        i = requireValue(args, i);
                        dashboardFile = args[i];
                    }
                    case "--avg-event-bytes" -> {
                        i = requireValue(args, i);
                        averageEventBytes = parsePositiveLong(args[i], "--avg-event-bytes");
                    }
                    case "--cost-per-tb" -> {
                        i = requireValue(args, i);
                        costPerTb = parseNonNegativeDouble(args[i], "--cost-per-tb");
                    }
                    case "--retention-days" -> {
                        i = requireValue(args, i);
                        retentionDays = parsePositiveInteger(args[i], "--retention-days");
                    }
                    case "--mode" -> {
                        i = requireValue(args, i);
                        demoMode = parseDemoMode(args[i]);
                    }
                    case "--events" -> {
                        i = requireValue(args, i);
                        totalEvents = parsePositiveInteger(args[i], "--events");
                    }
                    case "--target-reduction" -> {
                        i = requireValue(args, i);
                        targetReductionPercentage = parseReductionPercentage(args[i]);
                    }
                    case "--reducer" -> {
                        i = requireValue(args, i);
                        reducerType = parseReducerType(args[i]);
                        reducerExplicitlySet = true;
                    }
                    case "--ignite-nodes" -> {
                        i = requireValue(args, i);
                        igniteNodeCount = parseIgniteNodeCount(args[i]);
                    }
                    case "--window-seconds" -> {
                        i = requireValue(args, i);
                        windowSeconds = parsePositiveInteger(args[i], "--window-seconds");
                    }
                    case "--bootstrap-servers" -> {
                        i = requireValue(args, i);
                        bootstrapServers = parseText(args[i], "--bootstrap-servers");
                    }
                    case "--poll-ms" -> {
                        i = requireValue(args, i);
                        pollMs = parsePositiveLong(args[i], "--poll-ms");
                    }
                    case "--run-seconds" -> {
                        i = requireValue(args, i);
                        runSeconds = parseNonNegativeLong(args[i], "--run-seconds");
                    }
                    case "--metrics-interval-seconds" -> {
                        i = requireValue(args, i);
                        metricsIntervalSeconds = parsePositiveLong(args[i], "--metrics-interval-seconds");
                        metricsIntervalExplicitlySet = true;
                    }
                    case "--dashboard-port" -> {
                        i = requireValue(args, i);
                        dashboardPort = parseDashboardPort(args[i]);
                    }
                    case "--consumer-group-id" -> {
                        i = requireValue(args, i);
                        consumerGroupId = parseText(args[i], "--consumer-group-id");
                    }
                    case "--topic-partitions" -> {
                        i = requireValue(args, i);
                        topicPartitions = parsePositiveInteger(args[i], "--topic-partitions");
                    }
                    case "--topic-replication-factor" -> {
                        i = requireValue(args, i);
                        topicReplicationFactor = parsePositiveShort(args[i], "--topic-replication-factor");
                    }
                    case "--help", "-h" -> throw new IllegalArgumentException(usage());
                    default -> throw new IllegalArgumentException("Unknown argument: " + args[i] + System.lineSeparator() + usage());
                }
            }

            if (streamingEnabled && demoMode != DemoMode.KAFKA_SIM) {
                throw new IllegalArgumentException("--streaming and --windowed require --mode kafka-sim.");
            }

            if (continuous && demoMode != DemoMode.KAFKA_REDUCE) {
                throw new IllegalArgumentException("--continuous requires --mode kafka-reduce.");
            }

            if (runSeconds == 0 && demoMode != DemoMode.KAFKA_REDUCE) {
                throw new IllegalArgumentException("--run-seconds 0 requires --mode kafka-reduce.");
            }

            if (metricsIntervalExplicitlySet && demoMode != DemoMode.KAFKA_REDUCE) {
                throw new IllegalArgumentException("--metrics-interval-seconds requires --mode kafka-reduce.");
            }

            if (demoMode == DemoMode.KAFKA_REDUCE && !reducerExplicitlySet) {
                reducerType = ReducerType.GRIDGAIN;
            }

            continuous = continuous || (demoMode == DemoMode.KAFKA_REDUCE && runSeconds == 0);

            if (dashboardEnabled && demoMode == DemoMode.KAFKA_REDUCE && !continuous) {
                throw new IllegalArgumentException("--mode kafka-reduce --dashboard requires --continuous or --run-seconds 0.");
            }

            return new DemoConfig(
                    totalEvents,
                    targetReductionPercentage,
                    reducerType,
                    demoMode,
                    dashboardEnabled,
                    dashboardFile,
                    averageEventBytes,
                    costPerTb,
                    retentionDays,
                    igniteNodeCount,
                    streamingEnabled,
                    windowSeconds,
                    bootstrapServers,
                    pollMs,
                    runSeconds,
                    metricsIntervalSeconds,
                    consumerGroupId,
                    createTopics,
                    topicPartitions,
                    topicReplicationFactor,
                    verboseWindows,
                    continuous,
                    dashboardPort
            );
        }

        private static int requireValue(String[] args, int flagIndex) {
            int valueIndex = flagIndex + 1;
            if (valueIndex >= args.length || args[valueIndex].startsWith("--")) {
                throw new IllegalArgumentException("Missing value for " + args[flagIndex] + System.lineSeparator() + usage());
            }
            return valueIndex;
        }

        private static int parsePositiveInteger(String value, String flagName) {
            try {
                int parsed = Integer.parseInt(value);
                if (parsed <= 0) {
                    throw new IllegalArgumentException(flagName + " must be positive.");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(flagName + " must be a positive integer.", ex);
            }
        }

        private static long parsePositiveLong(String value, String flagName) {
            try {
                long parsed = Long.parseLong(value);
                if (parsed <= 0) {
                    throw new IllegalArgumentException(flagName + " must be positive.");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(flagName + " must be a positive integer.", ex);
            }
        }

        private static long parseNonNegativeLong(String value, String flagName) {
            try {
                long parsed = Long.parseLong(value);
                if (parsed < 0) {
                    throw new IllegalArgumentException(flagName + " must not be negative.");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(flagName + " must be a non-negative integer.", ex);
            }
        }

        private static short parsePositiveShort(String value, String flagName) {
            int parsed = parsePositiveInteger(value, flagName);
            if (parsed > Short.MAX_VALUE) {
                throw new IllegalArgumentException(flagName + " must be less than or equal to " + Short.MAX_VALUE + ".");
            }
            return (short) parsed;
        }

        private static double parseReductionPercentage(String value) {
            try {
                double parsed = Double.parseDouble(value);
                if (parsed < 0.0 || parsed > 100.0) {
                    throw new IllegalArgumentException("--target-reduction must be between 0 and 100.");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("--target-reduction must be a number between 0 and 100.", ex);
            }
        }

        private static double parseNonNegativeDouble(String value, String flagName) {
            try {
                double parsed = Double.parseDouble(value);
                if (parsed < 0.0) {
                    throw new IllegalArgumentException(flagName + " must not be negative.");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(flagName + " must be a number.", ex);
            }
        }

        private static ReducerType parseReducerType(String value) {
            return switch (value.toLowerCase()) {
                case "inmemory" -> ReducerType.INMEMORY;
                case "gridgain" -> ReducerType.GRIDGAIN;
                default -> throw new IllegalArgumentException("--reducer must be inmemory or gridgain.");
            };
        }

        private static DemoMode parseDemoMode(String value) {
            return switch (value.toLowerCase()) {
                case "direct" -> DemoMode.DIRECT;
                case "kafka-sim" -> DemoMode.KAFKA_SIM;
                case "kafka-produce" -> DemoMode.KAFKA_PRODUCE;
                case "kafka-reduce" -> DemoMode.KAFKA_REDUCE;
                default -> throw new IllegalArgumentException("--mode must be direct, kafka-sim, kafka-produce, or kafka-reduce.");
            };
        }

        private static String parseText(String value, String flagName) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException(flagName + " must not be blank.");
            }
            return value;
        }

        private static int parseIgniteNodeCount(String value) {
            int parsed = parsePositiveInteger(value, "--ignite-nodes");
            if (parsed > 3) {
                throw new IllegalArgumentException("--ignite-nodes must be between 1 and 3 for this local embedded demo.");
            }
            return parsed;
        }

        private static int parseDashboardPort(String value) {
            try {
                int parsed = Integer.parseInt(value);
                if (parsed < 0 || parsed > 65_535) {
                    throw new IllegalArgumentException("--dashboard-port must be between 0 and 65535.");
                }
                return parsed;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException("--dashboard-port must be an integer between 0 and 65535.", ex);
            }
        }

        private static String usage() {
            return "Usage: java -cp target/gridgain-siem-demo-1.0.0-SNAPSHOT.jar "
                    + "com.gridgain.demo.siem.Main [--mode direct|kafka-sim|kafka-produce|kafka-reduce] "
                    + "[--events 10000] [--target-reduction 40] [--reducer inmemory|gridgain] "
                    + "[--ignite-nodes 3] [--streaming|--windowed] [--window-seconds 60] "
                    + "[--dashboard] [--dashboard-file target/demo-dashboard.html] "
                    + "[--avg-event-bytes 1200] [--cost-per-tb 500] [--retention-days 30] "
                    + "[--bootstrap-servers localhost:9092] [--run-seconds 60] [--poll-ms 1000] "
                    + "[--consumer-group-id gridgain-siem-reducer] [--create-topics] "
                    + "[--topic-partitions 3] [--topic-replication-factor 1] [--verbose-windows] "
                    + "[--continuous] [--metrics-interval-seconds 10] [--dashboard-port 8080]";
        }
    }
}
