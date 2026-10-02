package com.gridgain.demo.siem.dashboard.live;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public final class LiveKafkaDashboardServer implements AutoCloseable {
    private final HttpServer server;
    private final ExecutorService executor;
    private final LiveKafkaDashboardState state;
    private final LiveKafkaDashboardRenderer renderer;
    private final LiveKafkaDashboardJson json;

    public LiveKafkaDashboardServer(LiveKafkaDashboardState state, int port) throws IOException {
        this.state = state;
        this.renderer = new LiveKafkaDashboardRenderer();
        this.json = new LiveKafkaDashboardJson();
        this.server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), 0);
        this.executor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "live-kafka-dashboard");
            thread.setDaemon(true);
            return thread;
        });
        server.setExecutor(executor);
        server.createContext("/", this::handleRoot);
        server.createContext("/api/progress", this::handleProgress);
        server.createContext("/health", this::handleHealth);
    }

    public LiveKafkaDashboardServer start() {
        server.start();
        return this;
    }

    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
        executor.shutdownNow();
    }

    private void handleRoot(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod()) || !"/".equals(exchange.getRequestURI().getPath())) {
            send(exchange, 404, "not found", "text/plain; charset=utf-8");
            return;
        }
        send(exchange, 200, renderer.html(), "text/html; charset=utf-8");
    }

    private void handleProgress(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "method not allowed", "text/plain; charset=utf-8");
            return;
        }
        send(exchange, 200, json.json(state), "application/json; charset=utf-8");
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, "method not allowed", "text/plain; charset=utf-8");
            return;
        }
        send(exchange, 200, "ok", "text/plain; charset=utf-8");
    }

    private static void send(HttpExchange exchange, int status, String body, String contentType) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(bytes);
        }
    }
}
