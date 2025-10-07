package com.synehq.jdbc.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;
import java.nio.charset.StandardCharsets;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Redis JDBC Statement implementation.
 * Executes Redis commands and returns results as ResultSets.
 */
public class RedisStatement implements Statement {
    
    private final RedisConnection connection;
    private boolean closed = false;
    private int maxRows = 0;
    private int fetchSize = 0;
    private int queryTimeout = 0;
    private boolean escapeProcessing = true;
    private int maxFieldSize = 0;
    private ResultSet resultSet;
    private int updateCount = -1;
    private final AtomicInteger resultSetCounter = new AtomicInteger(0);

    public RedisStatement(RedisConnection connection) {
        this.connection = connection;
    }

	/**
	 * Minimal wrapper to support arbitrary/raw Redis commands (e.g., module commands like JSON.GET)
	 * that are not present in Jedis's Protocol.Command enum.
	 */
	private static class RawCommand implements redis.clients.jedis.commands.ProtocolCommand {
		private final byte[] raw;

		private RawCommand(String name) {
			this.raw = name.getBytes(StandardCharsets.UTF_8);
		}

		@Override
		public byte[] getRaw() {
			return raw;
		}
	}

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        
        try {
            RedisCommand command = RedisCommandParser.parse(sql);
            RedisResultSet rs = executeRedisCommand(command);
            this.resultSet = rs;
            this.updateCount = -1;
            return rs;
        } catch (Exception e) {
            throw new SQLException("Failed to execute Redis query: " + e.getMessage(), e);
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        
        try {
            RedisCommand command = RedisCommandParser.parse(sql);
            String result = executeRedisCommand(command).getStringResult();
            
            // For commands that return "OK", return 1
            if ("OK".equals(result)) {
                this.updateCount = 1;
                return 1;
            }
            
            // For numeric results, try to parse as integer
            try {
                this.updateCount = Integer.parseInt(result);
                return this.updateCount;
            } catch (NumberFormatException e) {
                this.updateCount = 1;
                return 1;
            }
        } catch (Exception e) {
            throw new SQLException("Failed to execute Redis update: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            if (resultSet != null) {
                resultSet.close();
            }
            closed = true;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return maxFieldSize;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
        this.maxFieldSize = max;
    }

    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
        this.escapeProcessing = enable;
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return queryTimeout;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        checkClosed();
        this.queryTimeout = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        checkClosed();
        // Redis doesn't support query cancellation
        throw new SQLFeatureNotSupportedException("Redis does not support query cancellation");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Redis does not support cursor names");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        
        try {
            RedisCommand command = RedisCommandParser.parse(sql);
            RedisResultSet rs = executeRedisCommand(command);
            this.resultSet = rs;
            this.updateCount = -1;
            return true; // Always returns a result set for Redis commands
        } catch (Exception e) {
            throw new SQLException("Failed to execute Redis command: " + e.getMessage(), e);
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        return false; // Redis commands return single results
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Redis only supports forward fetch direction");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Redis does not support batch operations");
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Redis does not support batch operations");
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Redis does not support batch operations");
    }

    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Redis does not support generated keys");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
        // Redis statements are not poolable
    }

    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        checkClosed();
        // Not supported in Redis
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }

    private RedisResultSet executeRedisCommand(RedisCommand command) throws SQLException {
        try (Jedis jedis = connection.getJedis()) {
            String[] args = command.getArgs();
            String commandName = command.getCommand().toUpperCase();
            
            Object result;
            
            switch (commandName) {
                case "GET":
                    result = jedis.get(args[0]);
                    break;
                case "SET":
                    if (args.length == 2) {
                        result = jedis.set(args[0], args[1]);
                    } else if (args.length == 4) {
                        // SET key value EX seconds
                        result = jedis.setex(args[0], Long.parseLong(args[3]), args[1]);
                    } else {
                        throw new IllegalArgumentException("SET command requires 2 or 4 arguments");
                    }
                    break;
                case "DEL":
                    result = jedis.del(args);
                    break;
                case "EXISTS":
                    result = jedis.exists(args);
                    break;
                case "KEYS":
                    result = jedis.keys(args[0]);
                    break;
                case "TYPE":
                    result = jedis.type(args[0]);
                    break;
                case "TTL":
                    result = jedis.ttl(args[0]);
                    break;
                case "EXPIRE":
                    result = jedis.expire(args[0], Integer.parseInt(args[1]));
                    break;
                case "PERSIST":
                    result = jedis.persist(args[0]);
                    break;
                case "PING":
                    result = jedis.ping();
                    break;
                case "INFO":
                    result = jedis.info(args.length > 0 ? args[0] : null);
                    break;
                case "DBSIZE":
                    result = jedis.dbSize();
                    break;
                case "FLUSHDB":
                    result = jedis.flushDB();
                    break;
                case "FLUSHALL":
                    result = jedis.flushAll();
                    break;
                case "SELECT":
                    result = jedis.select(Integer.parseInt(args[0]));
                    break;
                case "HSET":
                    if (args.length == 3) {
                        result = jedis.hset(args[0], args[1], args[2]);
                    } else if (args.length > 3 && args.length % 2 == 1) {
                        // Multiple field-value pairs: HSET key field1 value1 field2 value2 ...
                        Map<String, String> fieldValues = new HashMap<>();
                        for (int i = 1; i < args.length; i += 2) {
                            fieldValues.put(args[i], args[i + 1]);
                        }
                        result = jedis.hset(args[0], fieldValues);
                    } else {
                        throw new IllegalArgumentException("HSET command requires 3 arguments or odd number > 3");
                    }
                    break;
                case "HGET":
                    result = jedis.hget(args[0], args[1]);
                    break;
                case "HGETALL":
                    result = jedis.hgetAll(args[0]);
                    break;
                case "HDEL":
                    String[] hdelArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, hdelArgs, 0, hdelArgs.length);
                    result = jedis.hdel(args[0], hdelArgs);
                    break;
                case "LPUSH":
                    String[] lpushArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, lpushArgs, 0, lpushArgs.length);
                    result = jedis.lpush(args[0], lpushArgs);
                    break;
                case "RPUSH":
                    String[] rpushArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, rpushArgs, 0, rpushArgs.length);
                    result = jedis.rpush(args[0], rpushArgs);
                    break;
                case "LPOP":
                    result = jedis.lpop(args[0]);
                    break;
                case "RPOP":
                    result = jedis.rpop(args[0]);
                    break;
                case "LLEN":
                    result = jedis.llen(args[0]);
                    break;
                case "LRANGE":
                    result = jedis.lrange(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]));
                    break;
                case "SADD":
                    String[] saddArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, saddArgs, 0, saddArgs.length);
                    result = jedis.sadd(args[0], saddArgs);
                    break;
                case "SMEMBERS":
                    result = jedis.smembers(args[0]);
                    break;
                case "SREM":
                    String[] sremArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, sremArgs, 0, sremArgs.length);
                    result = jedis.srem(args[0], sremArgs);
                    break;
                case "SCARD":
                    result = jedis.scard(args[0]);
                    break;
                case "ZADD":
                    if (args.length == 3) {
                        result = jedis.zadd(args[0], Double.parseDouble(args[1]), args[2]);
                    } else {
                        // Multiple score-member pairs
                        java.util.Map<String, Double> scoreMembers = new java.util.HashMap<>();
                        for (int i = 1; i < args.length; i += 2) {
                            scoreMembers.put(args[i + 1], Double.parseDouble(args[i]));
                        }
                        result = jedis.zadd(args[0], scoreMembers);
                    }
                    break;
                case "ZRANGE":
                    result = jedis.zrange(args[0], Long.parseLong(args[1]), Long.parseLong(args[2]));
                    break;
                case "ZCARD":
                    result = jedis.zcard(args[0]);
                    break;
                case "ZREM":
                    String[] zremArgs = new String[args.length - 1];
                    System.arraycopy(args, 1, zremArgs, 0, zremArgs.length);
                    result = jedis.zrem(args[0], zremArgs);
                    break;
				default:
					// For unknown commands, first try built-in enum; if absent (e.g., JSON.GET), send as raw command
					try {
						result = jedis.sendCommand(redis.clients.jedis.Protocol.Command.valueOf(commandName), args);
					} catch (IllegalArgumentException e) {
						result = jedis.sendCommand(new RawCommand(commandName), args);
					}
					break;
            }

            return new RedisResultSet(this, command, result, resultSetCounter.incrementAndGet());
            
        } catch (JedisException e) {
            throw new SQLException("Redis command failed: " + e.getMessage(), e);
        }
    }
}
