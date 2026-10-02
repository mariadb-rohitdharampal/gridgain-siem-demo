package com.gridgain.demo.siem.kafka;

import com.gridgain.demo.siem.event.LogEvent;
import com.gridgain.demo.siem.event.LogSourceType;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.Map;

public final class KafkaLogEventJsonCodec {
    private static final int SUPPORTED_SCHEMA_VERSION = 1;

    public String toJson(LogEvent event) {
        StringBuilder json = new StringBuilder(512);
        json.append('{');
        appendNumber(json, "schemaVersion", SUPPORTED_SCHEMA_VERSION);
        appendString(json, "id", event.id());
        appendString(json, "timestamp", event.timestamp().toString());
        appendString(json, "source", event.sourceType().name());
        appendString(json, "sourceDisplayName", event.sourceType().displayName());
        appendString(json, "eventType", event.eventType());
        appendString(json, "type", event.eventType());
        appendString(json, "reductionKey", event.reductionKey());
        appendString(json, "severity", event.securityRelevant() ? "SECURITY" : "INFO");
        appendBoolean(json, "securityRelevant", event.securityRelevant());
        appendNumber(json, "representedEventCount", event.representedEventCount());
        appendString(json, "payload", event.rawPayload());
        appendString(json, "message", event.rawPayload());
        json.append('}');
        return json.toString();
    }

    public LogEvent fromJson(String json) {
        try {
            Map<String, Object> values = new Parser(json).parse();
            int schemaVersion = intValue(values, "schemaVersion");
            if (schemaVersion != SUPPORTED_SCHEMA_VERSION) {
                throw new UnsupportedSchemaException("Unsupported Kafka log event schemaVersion: " + schemaVersion);
            }

            String eventType = optionalStringValue(values, "eventType");
            if (eventType == null) {
                eventType = stringValue(values, "type");
            }

            String reductionKey = stringValue(values, "reductionKey");

            String payload = optionalStringValue(values, "payload");
            if (payload == null) {
                payload = stringValue(values, "message");
            }

            return new LogEvent(
                    stringValue(values, "id"),
                    sourceTypeValue(values, "source"),
                    timestampValue(values, "timestamp"),
                    eventType,
                    payload,
                    booleanValue(values, "securityRelevant"),
                    reductionKey,
                    intValue(values, "representedEventCount")
            );
        } catch (UnsupportedSchemaException ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("Malformed Kafka log event JSON: " + ex.getMessage(), ex);
        }
    }

    private static String stringValue(Map<String, Object> values, String name) {
        Object value = values.get(name);
        if (value instanceof String stringValue) {
            return stringValue;
        }
        throw new IllegalArgumentException("missing or non-string field '" + name + "'");
    }

    private static String optionalStringValue(Map<String, Object> values, String name) {
        Object value = values.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof String stringValue) {
            return stringValue;
        }
        throw new IllegalArgumentException("non-string field '" + name + "'");
    }

    private static int intValue(Map<String, Object> values, String name) {
        Object value = values.get(name);
        if (value instanceof Long longValue && longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
            return longValue.intValue();
        }
        throw new IllegalArgumentException("missing or non-integer field '" + name + "'");
    }

    private static boolean booleanValue(Map<String, Object> values, String name) {
        Object value = values.get(name);
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        throw new IllegalArgumentException("missing or non-boolean field '" + name + "'");
    }

    private static LogSourceType sourceTypeValue(Map<String, Object> values, String name) {
        String value = stringValue(values, name);
        try {
            return LogSourceType.valueOf(value);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException("unsupported source '" + value + "'", ex);
        }
    }

    private static Instant timestampValue(Map<String, Object> values, String name) {
        String value = stringValue(values, name);
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException ex) {
            throw new IllegalArgumentException("invalid timestamp '" + value + "'", ex);
        }
    }

    private static void appendString(StringBuilder json, String name, String value) {
        appendName(json, name);
        json.append('"');
        appendEscaped(json, value);
        json.append('"');
    }

    private static void appendBoolean(StringBuilder json, String name, boolean value) {
        appendName(json, name);
        json.append(value);
    }

    private static void appendNumber(StringBuilder json, String name, long value) {
        appendName(json, name);
        json.append(value);
    }

    private static void appendName(StringBuilder json, String name) {
        if (json.length() > 1) {
            json.append(',');
        }
        json.append('"').append(name).append("\":");
    }

    private static void appendEscaped(StringBuilder json, String value) {
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '"' -> json.append("\\\"");
                case '\\' -> json.append("\\\\");
                case '\b' -> json.append("\\b");
                case '\f' -> json.append("\\f");
                case '\n' -> json.append("\\n");
                case '\r' -> json.append("\\r");
                case '\t' -> json.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        json.append("\\u%04x".formatted((int) ch));
                    } else {
                        json.append(ch);
                    }
                }
            }
        }
    }

    private static final class UnsupportedSchemaException extends RuntimeException {
        private UnsupportedSchemaException(String message) {
            super(message);
        }
    }

    private static final class Parser {
        private final String json;
        private int position;

        private Parser(String json) {
            if (json == null || json.isBlank()) {
                throw new IllegalArgumentException("json must not be blank");
            }
            this.json = json;
        }

        private Map<String, Object> parse() {
            Map<String, Object> values = new LinkedHashMap<>();
            skipWhitespace();
            expect('{');
            skipWhitespace();
            if (peek('}')) {
                position++;
                ensureComplete();
                return values;
            }

            while (true) {
                skipWhitespace();
                String name = parseString();
                skipWhitespace();
                expect(':');
                skipWhitespace();
                values.put(name, parseValue());
                skipWhitespace();
                if (peek(',')) {
                    position++;
                    continue;
                }
                expect('}');
                ensureComplete();
                return values;
            }
        }

        private Object parseValue() {
            if (peek('"')) {
                return parseString();
            }
            if (startsWith("true")) {
                position += 4;
                return Boolean.TRUE;
            }
            if (startsWith("false")) {
                position += 5;
                return Boolean.FALSE;
            }
            if (peek('-') || isDigit(current())) {
                return parseNumber();
            }
            throw error("expected string, boolean, or integer value");
        }

        private String parseString() {
            expect('"');
            StringBuilder value = new StringBuilder();
            while (position < json.length()) {
                char ch = json.charAt(position++);
                if (ch == '"') {
                    return value.toString();
                }
                if (ch == '\\') {
                    value.append(parseEscape());
                } else {
                    if (ch < 0x20) {
                        throw error("unescaped control character in string");
                    }
                    value.append(ch);
                }
            }
            throw error("unterminated string");
        }

        private char parseEscape() {
            if (position >= json.length()) {
                throw error("unterminated escape sequence");
            }
            char escaped = json.charAt(position++);
            return switch (escaped) {
                case '"' -> '"';
                case '\\' -> '\\';
                case '/' -> '/';
                case 'b' -> '\b';
                case 'f' -> '\f';
                case 'n' -> '\n';
                case 'r' -> '\r';
                case 't' -> '\t';
                case 'u' -> parseUnicodeEscape();
                default -> throw error("unsupported escape sequence '\\" + escaped + "'");
            };
        }

        private char parseUnicodeEscape() {
            if (position + 4 > json.length()) {
                throw error("incomplete unicode escape");
            }
            String hex = json.substring(position, position + 4);
            position += 4;
            try {
                return (char) Integer.parseInt(hex, 16);
            } catch (NumberFormatException ex) {
                throw error("invalid unicode escape");
            }
        }

        private Long parseNumber() {
            int start = position;
            if (peek('-')) {
                position++;
            }
            if (position >= json.length() || !isDigit(json.charAt(position))) {
                throw error("invalid integer value");
            }
            while (position < json.length() && isDigit(json.charAt(position))) {
                position++;
            }
            try {
                return Long.parseLong(json.substring(start, position));
            } catch (NumberFormatException ex) {
                throw error("invalid integer value");
            }
        }

        private void expect(char expected) {
            if (!peek(expected)) {
                throw error("expected '" + expected + "'");
            }
            position++;
        }

        private boolean peek(char expected) {
            return position < json.length() && json.charAt(position) == expected;
        }

        private boolean startsWith(String value) {
            return json.startsWith(value, position);
        }

        private char current() {
            return position < json.length() ? json.charAt(position) : '\0';
        }

        private void skipWhitespace() {
            while (position < json.length() && Character.isWhitespace(json.charAt(position))) {
                position++;
            }
        }

        private void ensureComplete() {
            skipWhitespace();
            if (position != json.length()) {
                throw error("unexpected trailing content");
            }
        }

        private IllegalArgumentException error(String message) {
            return new IllegalArgumentException(message + " at position " + position);
        }

        private static boolean isDigit(char ch) {
            return ch >= '0' && ch <= '9';
        }
    }
}
