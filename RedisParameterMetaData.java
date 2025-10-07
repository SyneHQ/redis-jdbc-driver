package com.syne.jdbc.gateway.driver.redis;

import java.sql.ParameterMetaData;
import java.sql.SQLException;

/**
 * Redis JDBC ParameterMetaData implementation.
 * Provides minimal parameter metadata for Redis prepared statements.
 */
public class RedisParameterMetaData implements ParameterMetaData {

    @Override
    public int getParameterCount() throws SQLException {
        return 0; // Redis doesn't support traditional parameters
    }

    @Override
    public int isNullable(int param) throws SQLException {
        return parameterNullable;
    }

    @Override
    public boolean isSigned(int param) throws SQLException {
        return true;
    }

    @Override
    public int getPrecision(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int param) throws SQLException {
        return 0;
    }

    @Override
    public int getParameterType(int param) throws SQLException {
        return java.sql.Types.VARCHAR;
    }

    @Override
    public String getParameterTypeName(int param) throws SQLException {
        return "VARCHAR";
    }

    @Override
    public String getParameterClassName(int param) throws SQLException {
        return String.class.getName();
    }

    @Override
    public int getParameterMode(int param) throws SQLException {
        return parameterModeIn;
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
}
