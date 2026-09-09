package com.synehq.jdbc.redis;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for Redis commands from SQL-like syntax.
 * Converts SQL-like commands to Redis command format.
 */
public class RedisCommandParser {
    

    /**
     * Parse a Redis command from SQL-like syntax.
     * Examples:
     * - "GET key" -> RedisCommand("GET", ["key"])
     * - "SET key value" -> RedisCommand("SET", ["key", "value"])
     * - "HSET hash field value" -> RedisCommand("HSET", ["hash", "field", "value"])
     */
    public static RedisCommand parse(String sql) throws IllegalArgumentException {
        if (sql == null || sql.trim().isEmpty()) {
            throw new IllegalArgumentException("Empty Redis command");
        }
        
        String trimmed = sql.trim();
        List<String> tokens = tokenize(trimmed);
        
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("No tokens found in Redis command");
        }
        
        String command = tokens.get(0).toUpperCase();
        String[] args = tokens.subList(1, tokens.size()).toArray(new String[0]);
        
        return new RedisCommand(command, args);
    }

    /**
     * Tokenize the input string, handling quoted strings properly.
     */
    private static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        StringBuilder token = new StringBuilder();
        char quote = 0;
        boolean escaped = false, started = false;
        for (int i = 0; i < input.length(); i++) {
            char current = input.charAt(i);
            if (escaped) { token.append(current); escaped = false; started = true; }
            else if (current == '\\') { escaped = true; started = true; }
            else if (quote != 0) {
                if (current == quote) quote = 0;
                else token.append(current);
            } else if (current == '\'' || current == '"') { quote = current; started = true; }
            else if (Character.isWhitespace(current)) {
                if (started) { tokens.add(token.toString()); token.setLength(0); started = false; }
            } else { token.append(current); started = true; }
        }
        if (quote != 0 || escaped) throw new IllegalArgumentException("Unterminated Redis argument");
        if (started) tokens.add(token.toString());
        return tokens;
    }

}
