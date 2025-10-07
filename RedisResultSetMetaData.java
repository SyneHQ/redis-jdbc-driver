package com.syne.jdbc.gateway.driver.redis;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

/**
 * Redis JDBC ResultSetMetaData implementation.
 * Provides metadata information about Redis result sets.
 */
public class RedisResultSetMetaData implements ResultSetMetaData {
    
    private final List<String> columnNames;
    private final List<Integer> columnTypes;

    public RedisResultSetMetaData() {
        this.columnNames = List.of("result");
        this.columnTypes = List.of(java.sql.Types.VARCHAR);
    }

    public RedisResultSetMetaData(List<String> columnNames, List<Integer> columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int sqlType = getColumnType(column);
        return sqlType == java.sql.Types.INTEGER || 
               sqlType == java.sql.Types.BIGINT || 
               sqlType == java.sql.Types.DECIMAL ||
               sqlType == java.sql.Types.NUMERIC ||
               sqlType == java.sql.Types.FLOAT ||
               sqlType == java.sql.Types.DOUBLE ||
               sqlType == java.sql.Types.REAL;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 255; // Default display size
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumnIndex(column);
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        int sqlType = getColumnType(column);
        if (sqlType == java.sql.Types.DECIMAL || sqlType == java.sql.Types.NUMERIC) {
            return 10; // Default precision
        }
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        int sqlType = getColumnType(column);
        if (sqlType == java.sql.Types.DECIMAL || sqlType == java.sql.Types.NUMERIC) {
            return 0; // Default scale
        }
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkColumnIndex(column);
        return columnTypes.get(column - 1);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int sqlType = getColumnType(column);
        switch (sqlType) {
            case java.sql.Types.VARCHAR:
                return "VARCHAR";
            case java.sql.Types.BIGINT:
                return "BIGINT";
            case java.sql.Types.INTEGER:
                return "INTEGER";
            case java.sql.Types.DECIMAL:
                return "DECIMAL";
            case java.sql.Types.NUMERIC:
                return "NUMERIC";
            case java.sql.Types.FLOAT:
                return "FLOAT";
            case java.sql.Types.DOUBLE:
                return "DOUBLE";
            case java.sql.Types.REAL:
                return "REAL";
            case java.sql.Types.BOOLEAN:
                return "BOOLEAN";
            case java.sql.Types.BIT:
                return "BIT";
            case java.sql.Types.BINARY:
                return "BINARY";
            case java.sql.Types.VARBINARY:
                return "VARBINARY";
            case java.sql.Types.DATE:
                return "DATE";
            case java.sql.Types.TIME:
                return "TIME";
            case java.sql.Types.TIMESTAMP:
                return "TIMESTAMP";
            default:
                return "VARCHAR";
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int sqlType = getColumnType(column);
        switch (sqlType) {
            case java.sql.Types.VARCHAR:
            case java.sql.Types.CHAR:
            case java.sql.Types.LONGVARCHAR:
                return String.class.getName();
            case java.sql.Types.BIGINT:
                return Long.class.getName();
            case java.sql.Types.INTEGER:
                return Integer.class.getName();
            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                return java.math.BigDecimal.class.getName();
            case java.sql.Types.FLOAT:
                return Float.class.getName();
            case java.sql.Types.DOUBLE:
                return Double.class.getName();
            case java.sql.Types.REAL:
                return Float.class.getName();
            case java.sql.Types.BOOLEAN:
            case java.sql.Types.BIT:
                return Boolean.class.getName();
            case java.sql.Types.BINARY:
            case java.sql.Types.VARBINARY:
                return byte[].class.getName();
            case java.sql.Types.DATE:
                return java.sql.Date.class.getName();
            case java.sql.Types.TIME:
                return java.sql.Time.class.getName();
            case java.sql.Types.TIMESTAMP:
                return java.sql.Timestamp.class.getName();
            default:
                return String.class.getName();
        }
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

    private void checkColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Column index out of range: " + column);
        }
    }
}
