package com.syne.jdbc.redis;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parser for Redis commands from SQL-like syntax.
 * Converts SQL-like commands to Redis command format.
 */
public class RedisCommandParser {
    
    private static final Pattern QUOTED_STRING = Pattern.compile("\"([^\"]*)\"|'([^']*)'");
    private static final Pattern UNQUOTED_WORD = Pattern.compile("\\S+");

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
    private static List<String> tokens = new ArrayList<>();
    
    private static List<String> tokenize(String input) {
        List<String> tokens = new ArrayList<>();
        
        // First, extract quoted strings
        Matcher quotedMatcher = QUOTED_STRING.matcher(input);
        int lastEnd = 0;
        
        while (quotedMatcher.find()) {
            // Add unquoted text before this match
            String beforeMatch = input.substring(lastEnd, quotedMatcher.start()).trim();
            if (!beforeMatch.isEmpty()) {
                addUnquotedTokens(tokens, beforeMatch);
            }
            
            // Add the quoted string (without quotes)
            String quotedValue = quotedMatcher.group(1);
            if (quotedValue == null) {
                quotedValue = quotedMatcher.group(2);
            }
            tokens.add(quotedValue);
            
            lastEnd = quotedMatcher.end();
        }
        
        // Add remaining unquoted text
        String remaining = input.substring(lastEnd).trim();
        if (!remaining.isEmpty()) {
            addUnquotedTokens(tokens, remaining);
        }
        
        return tokens;
    }
    
    private static void addUnquotedTokens(List<String> tokens, String text) {
        Matcher matcher = UNQUOTED_WORD.matcher(text);
        while (matcher.find()) {
            tokens.add(matcher.group());
        }
    }

}
