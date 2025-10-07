package com.syne.jdbc.gateway.driver.redis;

/**
 * Redis command representation.
 */
public class RedisCommand {
    private final String command;
    private final String[] args;

    public RedisCommand(String command, String[] args) {
        this.command = command;
        this.args = args;
    }

    public String getCommand() {
        return command;
    }

    public String[] getArgs() {
        return args;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(command);
        for (String arg : args) {
            sb.append(" ").append(arg);
        }
        return sb.toString();
    }
}
