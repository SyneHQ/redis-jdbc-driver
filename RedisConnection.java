package com.syne.jdbc.gateway.driver.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Redis JDBC Connection implementation.
 * Handles both standalone Redis and Redis Cluster connections.
 */
public class RedisConnection implements Connection {
    
    private final String url;
    private final Properties properties;
    private final boolean isCluster;
    private final RedisConnectionInfo connectionInfo;
    private final String effectiveUsername;
    private final String effectivePassword;
    
    private JedisPool jedisPool;
    private JedisCluster jedisCluster;
    private boolean closed = false;
    private boolean autoCommit = true;
    private String catalog;
    private String schema;
    private int transactionIsolation = Connection.TRANSACTION_NONE;
    
    private final Map<String, Class<?>> typeMap = new ConcurrentHashMap<>();

    public RedisConnection(String url, Properties info) throws SQLException {
        this.url = url;
        this.properties = info != null ? new Properties(info) : new Properties();
        this.connectionInfo = parseConnectionUrl(url);
        this.isCluster = url.startsWith("jdbc:redis:cluster://");
        
        // Determine effective credentials. Prefer URL credentials, then properties.
        String propUser = this.properties.getProperty("user");
        if (propUser == null) {
            propUser = this.properties.getProperty("username");
        }
        this.effectiveUsername = this.connectionInfo.getUsername() != null ? this.connectionInfo.getUsername() : propUser;
        
        String propPassword = this.properties.getProperty("password");
        this.effectivePassword = this.connectionInfo.getPassword() != null ? this.connectionInfo.getPassword() : propPassword;
        
        initializeConnection();
    }

    private void initializeConnection() throws SQLException {
        try {
            if (isCluster) {
                initializeClusterConnection();
            } else {
                initializeStandaloneConnection();
            }
        } catch (Exception e) {
            throw new SQLException("Failed to initialize Redis connection: " + e.getMessage(), e);
        }
    }

    private void initializeStandaloneConnection() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        
        int timeout = Integer.parseInt(properties.getProperty("connectionTimeout", "2000"));
        int socketTimeout = Integer.parseInt(properties.getProperty("socketTimeout", "2000"));
        int blockingSocketTimeout = Integer.parseInt(properties.getProperty("blockingSocketTimeout", "0"));
        
        String password = effectivePassword;
        int database = connectionInfo.getDatabase();
        
        jedisPool = new JedisPool(poolConfig, 
            connectionInfo.getHost(), 
            connectionInfo.getPort(), 
            timeout, 
            password, 
            database, 
            connectionInfo.getClientName());
    }

    private void initializeClusterConnection() {
        // For cluster, we'll use a simple approach with the first host
        // In a real implementation, you'd parse all cluster nodes
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        
        int timeout = Integer.parseInt(properties.getProperty("connectionTimeout", "2000"));
        int socketTimeout = Integer.parseInt(properties.getProperty("socketTimeout", "2000"));
        
        String password = effectivePassword;
        
        jedisPool = new JedisPool(poolConfig, 
            connectionInfo.getHost(), 
            connectionInfo.getPort(), 
            timeout, 
            password, 
            0, // Cluster doesn't use database numbers
            connectionInfo.getClientName());
    }

    private RedisConnectionInfo parseConnectionUrl(String url) throws SQLException {
        try {
            // Remove jdbc:redis:// or jdbc:redis:cluster:// prefix
            String cleanUrl = url.replaceFirst("jdbc:redis://", "").replaceFirst("jdbc:redis:cluster://", "");
            
            // Parse authentication
            String username = null;
            String password = null;
            if (cleanUrl.contains("@")) {
                String[] parts = cleanUrl.split("@", 2);
                String authPart = parts[0];
                cleanUrl = parts[1];
                
                if (authPart.contains(":")) {
                    String[] authParts = authPart.split(":", 2);
                    username = authParts[0];
                    password = authParts[1];
                } else {
                    password = authPart;
                }
            }
            
            // Parse host and port
            String host = "localhost";
            int port = 6379;
            
            if (cleanUrl.contains("/")) {
                String[] parts = cleanUrl.split("/", 2);
                String hostPort = parts[0];
                
                if (hostPort.contains(":")) {
                    String[] hostPortParts = hostPort.split(":", 2);
                    host = hostPortParts[0];
                    port = Integer.parseInt(hostPortParts[1]);
                } else {
                    host = hostPort;
                }
            }
            
            // Parse database number
            int database = 0;
            if (cleanUrl.contains("/")) {
                String[] parts = cleanUrl.split("/", 2);
                if (parts.length > 1) {
                    String dbPart = parts[1].split("\\?")[0]; // Remove query parameters
                    try {
                        database = Integer.parseInt(dbPart);
                    } catch (NumberFormatException e) {
                        database = 0;
                    }
                }
            }
            
            // Parse query parameters
            String clientName = properties.getProperty("clientName");
            
            return new RedisConnectionInfo(host, port, database, username, password, clientName);
            
        } catch (Exception e) {
            throw new SQLException("Invalid Redis URL format: " + url, e);
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new RedisStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new RedisPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support stored procedures");
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql; // Redis commands are passed through as-is
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        // Redis doesn't support transactions in the traditional sense
        // This is a no-op for Redis
    }

    @Override
    public void rollback() throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Redis does not support rollback operations");
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            if (jedisPool != null && !jedisPool.isClosed()) {
                jedisPool.close();
            }
            if (jedisCluster != null) {
                jedisCluster.close();
            }
            closed = true;
        }
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new RedisDatabaseMetaData(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        checkClosed();
        // Redis connections are always read-write
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        checkClosed();
        this.catalog = catalog;
    }

    @Override
    public String getCatalog() throws SQLException {
        checkClosed();
        return catalog;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        if (level != Connection.TRANSACTION_NONE) {
            throw new SQLFeatureNotSupportedException("Redis does not support transaction isolation levels");
        }
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return transactionIsolation;
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
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return new RedisStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        checkClosed();
        return new RedisPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support stored procedures");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        checkClosed();
        return typeMap;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        checkClosed();
        typeMap.clear();
        if (map != null) {
            typeMap.putAll(map);
        }
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        checkClosed();
        throw new SQLFeatureNotSupportedException("Redis does not support holdability");
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support savepoints");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support savepoints");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support savepoints");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support savepoints");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        return new RedisStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        checkClosed();
        return new RedisPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support stored procedures");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return new RedisPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        return new RedisPreparedStatement(this, sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        return new RedisPreparedStatement(this, sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support CLOB");
    }

    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support BLOB");
    }

    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support NCLOB");
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support SQLXML");
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (closed) {
            return false;
        }
        
        try (Jedis jedis = getJedis()) {
            return "PONG".equals(jedis.ping());
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // Redis doesn't support client info
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // Redis doesn't support client info
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support arrays");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support structs");
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        checkClosed();
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        checkClosed();
        // Redis timeout is handled at the Jedis level
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        checkClosed();
        return Integer.parseInt(properties.getProperty("socketTimeout", "2000"));
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
            throw new SQLException("Connection is closed");
        }
    }

    public Jedis getJedis() throws SQLException {
        checkClosed();
        if (jedisPool == null) {
            throw new SQLException("Redis connection not initialized");
        }
        Jedis jedis = jedisPool.getResource();
        // If a username is provided, perform ACL AUTH explicitly (pool constructor cannot pass username)
        if (effectiveUsername != null && effectivePassword != null) {
            try {
                jedis.auth(effectiveUsername, effectivePassword);
            } catch (Exception ignored) {
                // If already authenticated or server doesn't support ACL-style auth, ignore
            }
        }
        return jedis;
    }

    public RedisConnectionInfo getConnectionInfo() {
        return connectionInfo;
    }

    public boolean isCluster() {
        return isCluster;
    }

    /**
     * Inner class to hold Redis connection information
     */
    public static class RedisConnectionInfo {
        private final String host;
        private final int port;
        private final int database;
        private final String username;
        private final String password;
        private final String clientName;

        public RedisConnectionInfo(String host, int port, int database, String username, String password, String clientName) {
            this.host = host;
            this.port = port;
            this.database = database;
            this.username = username;
            this.password = password;
            this.clientName = clientName;
        }

        public String getHost() { return host; }
        public int getPort() { return port; }
        public int getDatabase() { return database; }
        public String getUsername() { return username; }
        public String getPassword() { return password; }
        public String getClientName() { return clientName; }
    }
}
