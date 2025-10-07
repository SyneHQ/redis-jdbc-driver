package com.syne.jdbc.redis;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * Redis JDBC Driver implementation based on the DataGrip Redis JDBC Driver.
 * This driver provides JDBC connectivity to Redis databases.
 */
public class RedisDriver implements Driver {
    
    private static final String DRIVER_NAME = "Redis JDBC Driver";
    private static final String DRIVER_VERSION = "1.5";
    private static final int DRIVER_MAJOR_VERSION = 1;
    private static final int DRIVER_MINOR_VERSION = 5;
    
    static {
        try {
            DriverManager.registerDriver(new RedisDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register Redis JDBC driver", e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }
        
        try {
            return new RedisConnection(url, info);
        } catch (Exception e) {
            throw new SQLException("Failed to connect to Redis: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && (url.startsWith("jdbc:redis://") || url.startsWith("jdbc:redis:cluster://"));
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[]{
            createPropertyInfo("user", "Username for Redis authentication", null),
            createPropertyInfo("password", "Password for Redis authentication", null),
            createPropertyInfo("database", "Redis database number (0-15)", "0"),
            createPropertyInfo("connectionTimeout", "Connection timeout in milliseconds", "2000"),
            createPropertyInfo("socketTimeout", "Socket timeout in milliseconds", "2000"),
            createPropertyInfo("blockingSocketTimeout", "Blocking socket timeout in milliseconds", "0"),
            createPropertyInfo("clientName", "Client name for Redis connection", null),
            createPropertyInfo("ssl", "Enable SSL connection", "false"),
            createPropertyInfo("verifyServerCertificate", "Verify server certificate", "true"),
            createPropertyInfo("hostAndPortMapping", "Host and port mapping for port forwarding", null),
            createPropertyInfo("verifyConnectionMode", "Verify connection mode", "true")
        };
    }

    private DriverPropertyInfo createPropertyInfo(String name, String description, String defaultValue) {
        DriverPropertyInfo info = new DriverPropertyInfo(name, defaultValue);
        info.description = description;
        info.required = false;
        return info;
    }

    @Override
    public int getMajorVersion() {
        return DRIVER_MAJOR_VERSION;
    }

    @Override
    public int getMinorVersion() {
        return DRIVER_MINOR_VERSION;
    }

    @Override
    public boolean jdbcCompliant() {
        return false; // Redis is not a traditional SQL database
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Redis driver does not support parent logger");
    }
}
