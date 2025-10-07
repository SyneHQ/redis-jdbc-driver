package com.synehq.jdbc.redis;

import java.sql.*;
import java.util.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Redis JDBC ResultSet implementation.
 * Converts Redis command results into JDBC ResultSet format.
 */
public class RedisResultSet implements ResultSet {
    
    private final RedisStatement statement;
    private final RedisCommand command;
    private final Object result;
    private final int resultSetNumber;
    
    private boolean closed = false;
    private boolean beforeFirst = true;
    private boolean afterLast = false;
    private int currentRow = 0;
    private List<Map<String, Object>> rows;
    private Map<String, Object> currentRowData;
    private List<String> columnNames;
    private List<Integer> columnTypes;

    public RedisResultSet(RedisStatement statement, RedisCommand command, Object result, int resultSetNumber) {
        this.statement = statement;
        this.command = command;
        this.result = result;
        this.resultSetNumber = resultSetNumber;
        
        initializeResultData();
    }

    private void initializeResultData() {
        this.rows = new ArrayList<>();
        this.columnNames = new ArrayList<>();
        this.columnTypes = new ArrayList<>();
        
        // Convert Redis result to tabular format
        if (result == null) {
            // Null result
            columnNames.add("result");
            columnTypes.add(Types.VARCHAR);
            Map<String, Object> row = new HashMap<>();
            row.put("result", null);
            rows.add(row);
        } else if (result instanceof String) {
            // String result
            columnNames.add("value");
            columnTypes.add(Types.VARCHAR);
            Map<String, Object> row = new HashMap<>();
            row.put("value", result);
            rows.add(row);
        } else if (result instanceof byte[]) {
            // Bulk string as bytes
            columnNames.add("value");
            columnTypes.add(Types.VARCHAR);
            Map<String, Object> row = new HashMap<>();
            row.put("value", new String((byte[]) result, StandardCharsets.UTF_8));
            rows.add(row);
        } else if (result instanceof Long) {
            // Numeric result
            columnNames.add("count");
            columnTypes.add(Types.BIGINT);
            Map<String, Object> row = new HashMap<>();
            row.put("count", result);
            rows.add(row);
        } else if (result instanceof List) {
            // List result (e.g., KEYS, LRANGE, SMEMBERS)
            List<?> list = (List<?>) result;
            if (list.isEmpty()) {
                columnNames.add("value");
                columnTypes.add(Types.VARCHAR);
                Map<String, Object> row = new HashMap<>();
                row.put("value", null);
                rows.add(row);
            } else {
                columnNames.add("value");
                columnTypes.add(Types.VARCHAR);
                for (Object item : list) {
                    Map<String, Object> row = new HashMap<>();
                    if (item instanceof byte[]) {
                        row.put("value", new String((byte[]) item, StandardCharsets.UTF_8));
                    } else {
                        row.put("value", item != null ? item.toString() : null);
                    }
                    rows.add(row);
                }
            }
        } else if (result instanceof Map) {
            // Map result (e.g., HGETALL)
            Map<?, ?> map = (Map<?, ?>) result;
            if (map.isEmpty()) {
                columnNames.add("field");
                columnNames.add("value");
                columnTypes.add(Types.VARCHAR);
                columnTypes.add(Types.VARCHAR);
                Map<String, Object> row = new HashMap<>();
                row.put("field", null);
                row.put("value", null);
                rows.add(row);
            } else {
                columnNames.add("field");
                columnNames.add("value");
                columnTypes.add(Types.VARCHAR);
                columnTypes.add(Types.VARCHAR);
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Map<String, Object> row = new HashMap<>();
                    Object k = entry.getKey();
                    Object v = entry.getValue();
                    if (k instanceof byte[]) {
                        row.put("field", new String((byte[]) k, StandardCharsets.UTF_8));
                    } else {
                        row.put("field", k != null ? k.toString() : null);
                    }
                    if (v instanceof byte[]) {
                        row.put("value", new String((byte[]) v, StandardCharsets.UTF_8));
                    } else {
                        row.put("value", v != null ? v.toString() : null);
                    }
                    rows.add(row);
                }
            }
        } else if (result instanceof Set) {
            // Set result (e.g., SMEMBERS)
            Set<?> set = (Set<?>) result;
            columnNames.add("value");
            columnTypes.add(Types.VARCHAR);
            for (Object item : set) {
                Map<String, Object> row = new HashMap<>();
                if (item instanceof byte[]) {
                    row.put("value", new String((byte[]) item, StandardCharsets.UTF_8));
                } else {
                    row.put("value", item != null ? item.toString() : null);
                }
                rows.add(row);
            }
        } else {
            // Generic object result
            columnNames.add("result");
            columnTypes.add(Types.VARCHAR);
            Map<String, Object> row = new HashMap<>();
            if (result instanceof byte[]) {
                row.put("result", new String((byte[]) result, StandardCharsets.UTF_8));
            } else {
                row.put("result", result.toString());
            }
            rows.add(row);
        }
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        
        if (beforeFirst) {
            beforeFirst = false;
            if (rows.isEmpty()) {
                afterLast = true;
                return false;
            }
            currentRow = 0;
            currentRowData = rows.get(currentRow);
            return true;
        }
        
        if (afterLast) {
            return false;
        }
        
        currentRow++;
        if (currentRow >= rows.size()) {
            afterLast = true;
            return false;
        }
        
        currentRowData = rows.get(currentRow);
        return true;
    }

    @Override
    public void close() throws SQLException {
        closed = true;
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return currentRowData == null;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        return value != null ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return false;
        if (value instanceof Boolean) return (Boolean) value;
        if (value instanceof String) return Boolean.parseBoolean((String) value);
        if (value instanceof Number) return ((Number) value).intValue() != 0;
        return false;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).byteValue();
        if (value instanceof String) return Byte.parseByte((String) value);
        return 0;
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).shortValue();
        if (value instanceof String) return Short.parseShort((String) value);
        return 0;
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).intValue();
        if (value instanceof String) return Integer.parseInt((String) value);
        return 0;
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).longValue();
        if (value instanceof String) return Long.parseLong((String) value);
        return 0;
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).floatValue();
        if (value instanceof String) return Float.parseFloat((String) value);
        return 0;
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return 0;
        if (value instanceof Number) return ((Number) value).doubleValue();
        if (value instanceof String) return Double.parseDouble((String) value);
        return 0;
    }

    @Override
    public java.math.BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return null;
        if (value instanceof java.math.BigDecimal) return (java.math.BigDecimal) value;
        if (value instanceof Number) return new java.math.BigDecimal(value.toString());
        if (value instanceof String) return new java.math.BigDecimal((String) value);
        return null;
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return null;
        if (value instanceof byte[]) return (byte[]) value;
        if (value instanceof String) return ((String) value).getBytes();
        return value.toString().getBytes();
    }

    @Override
    public java.sql.Date getDate(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return null;
        if (value instanceof java.sql.Date) return (java.sql.Date) value;
        if (value instanceof String) return java.sql.Date.valueOf((String) value);
        return null;
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return null;
        if (value instanceof Time) return (Time) value;
        if (value instanceof String) return Time.valueOf((String) value);
        return null;
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        checkClosed();
        Object value = getColumnValue(columnIndex);
        if (value == null) return null;
        if (value instanceof Timestamp) return (Timestamp) value;
        if (value instanceof String) return Timestamp.valueOf((String) value);
        return null;
    }

    @Override
    public java.io.InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support ASCII streams");
    }

    @Override
    public java.io.InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support Unicode streams");
    }

    @Override
    public java.io.InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support binary streams");
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public java.math.BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public java.sql.Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public java.io.InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public java.io.InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public java.io.InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
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
    public String getCursorName() throws SQLException {
        checkClosed();
        return "redis_cursor_" + resultSetNumber;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new RedisResultSetMetaData(columnNames, columnTypes);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        checkClosed();
        return getColumnValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        checkClosed();
        for (int i = 0; i < columnNames.size(); i++) {
            if (columnNames.get(i).equalsIgnoreCase(columnLabel)) {
                return i + 1;
            }
        }
        throw new SQLException("Column '" + columnLabel + "' not found");
    }

    @Override
    public java.io.Reader getCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support character streams");
    }

    @Override
    public java.io.Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public java.math.BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        return getBigDecimal(columnIndex, 0);
    }

    @Override
    public java.math.BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return beforeFirst;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return afterLast;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        return !beforeFirst && !afterLast && currentRow == 0;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        return !beforeFirst && !afterLast && currentRow == rows.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        checkClosed();
        beforeFirst = true;
        afterLast = false;
        currentRow = 0;
        currentRowData = null;
    }

    @Override
    public void afterLast() throws SQLException {
        checkClosed();
        beforeFirst = false;
        afterLast = true;
        currentRow = rows.size();
        currentRowData = null;
    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        if (rows.isEmpty()) {
            return false;
        }
        beforeFirst = false;
        afterLast = false;
        currentRow = 0;
        currentRowData = rows.get(currentRow);
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        if (rows.isEmpty()) {
            return false;
        }
        beforeFirst = false;
        afterLast = false;
        currentRow = rows.size() - 1;
        currentRowData = rows.get(currentRow);
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        if (beforeFirst || afterLast) {
            return 0;
        }
        return currentRow + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        if (rows.isEmpty()) {
            return false;
        }
        
        if (row == 0) {
            beforeFirst();
            return false;
        }
        
        if (row > 0) {
            if (row > rows.size()) {
                afterLast();
                return false;
            }
            beforeFirst = false;
            afterLast = false;
            currentRow = row - 1;
            currentRowData = rows.get(currentRow);
            return true;
        } else {
            // Negative row number - count from end
            int absRow = rows.size() + row + 1;
            return absolute(absRow);
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        return absolute(getRow() + rows);
    }

    @Override
    public boolean previous() throws SQLException {
        checkClosed();
        if (beforeFirst) {
            return false;
        }
        if (currentRow <= 0) {
            beforeFirst();
            return false;
        }
        currentRow--;
        currentRowData = rows.get(currentRow);
        afterLast = false;
        return true;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
        if (direction != FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Redis only supports forward fetch direction");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
        // Redis doesn't support fetch size
    }

    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        checkClosed();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        checkClosed();
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBigDecimal(int columnIndex, java.math.BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateDate(int columnIndex, java.sql.Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public void updateBigDecimal(String columnLabel, java.math.BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public void updateDate(String columnLabel, java.sql.Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, int length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader x, int length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support row refresh");
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public Statement getStatement() throws SQLException {
        checkClosed();
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support REF types");
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support BLOB types");
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support CLOB types");
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support ARRAY types");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public java.sql.Date getDate(int columnIndex, java.util.Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public java.sql.Date getDate(String columnLabel, java.util.Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, java.util.Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, java.util.Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, java.util.Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, java.util.Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support URL types");
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(findColumn(columnLabel), x);
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(findColumn(columnLabel), x);
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(findColumn(columnLabel), x);
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(findColumn(columnLabel), x);
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support ROWID types");
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    @Override
    public int getHoldability() throws SQLException {
        checkClosed();
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        updateString(columnIndex, nString);
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        updateString(findColumn(columnLabel), nString);
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        updateNClob(findColumn(columnLabel), nClob);
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support NCLOB types");
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support SQLXML types");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public java.io.Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis does not support NCharacter streams");
    }

    @Override
    public java.io.Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, java.io.Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x, long length) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x, long length) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream, long length) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public void updateClob(int columnIndex, java.io.Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader, long length) throws SQLException {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public void updateNCharacterStream(int columnIndex, java.io.Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, java.io.Reader reader) throws SQLException {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateAsciiStream(int columnIndex, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateAsciiStream(String columnLabel, java.io.InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateBinaryStream(int columnIndex, java.io.InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBinaryStream(String columnLabel, java.io.InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public void updateCharacterStream(int columnIndex, java.io.Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateCharacterStream(String columnLabel, java.io.Reader reader) throws SQLException {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public void updateBlob(int columnIndex, java.io.InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateBlob(String columnLabel, java.io.InputStream inputStream) throws SQLException {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public void updateClob(int columnIndex, java.io.Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateClob(String columnLabel, java.io.Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public void updateNClob(int columnIndex, java.io.Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Redis ResultSet is read-only");
    }

    @Override
    public void updateNClob(String columnLabel, java.io.Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        Object value = getObject(columnIndex);
        if (value == null) return null;
        if (type.isAssignableFrom(value.getClass())) {
            return type.cast(value);
        }
        throw new SQLException("Cannot convert value to " + type.getName());
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
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
            throw new SQLException("ResultSet is closed");
        }
    }

    private Object getColumnValue(int columnIndex) throws SQLException {
        if (currentRowData == null) {
            throw new SQLException("No current row");
        }
        if (columnIndex < 1 || columnIndex > columnNames.size()) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
        String columnName = columnNames.get(columnIndex - 1);
        return currentRowData.get(columnName);
    }

    public String getStringResult() {
        if (result instanceof String) {
            return (String) result;
        }
        return result != null ? result.toString() : null;
    }
}
