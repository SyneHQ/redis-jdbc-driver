# Redis JDBC Driver

A JDBC driver implementation for Redis databases that provides a SQL-like interface to Redis operations.

## Features

- Full JDBC 4.2 compliance
- Support for Redis standalone and cluster connections
- SQL-like command interface to Redis operations
- Comprehensive Redis command support including:
  - String operations (GET, SET, DEL, etc.)
  - Hash operations (HSET, HGET, HGETALL, etc.)
  - List operations (LPUSH, RPUSH, LRANGE, etc.)
  - Set operations (SADD, SMEMBERS, SREM, etc.)
  - Sorted Set operations (ZADD, ZRANGE, ZCARD, etc.)
  - Server operations (PING, INFO, DBSIZE, etc.)
- Connection pooling with Jedis
- Authentication support (password and ACL-style)
- Prepared statement support with parameter binding
- Result set conversion for tabular data display

## Requirements

- Java 8 or higher
- Redis 6.0 or higher
- Jedis 4.4.3

## Installation

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.synehq</groupId>
    <artifactId>redis-jdbc-driver</artifactId>
    <version>1.0.0</version>
</dependency>
```

### Gradle

Add the following to your `build.gradle`:

```gradle
implementation 'com.synehq:redis-jdbc-driver:1.0.0'
```

## Usage

### Basic Connection

```java
import java.sql.*;

public class RedisJdbcExample {
    public static void main(String[] args) {
        try {
            // Register the driver
            Class.forName("com.synehq.redis.RedisDriver");
            
            // Connect to Redis
            String url = "jdbc:redis://localhost:6379/0";
            Connection conn = DriverManager.getConnection(url);
            
            // Create a statement
            Statement stmt = conn.createStatement();
            
            // Execute Redis commands
            stmt.execute("SET mykey myvalue");
            ResultSet rs = stmt.executeQuery("GET mykey");
            
            while (rs.next()) {
                System.out.println("Value: " + rs.getString("value"));
            }
            
            rs.close();
            stmt.close();
            conn.close();
            
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
```

### Connection URLs

#### Standalone Redis
```
jdbc:redis://[username:password@]host[:port]/database
```

Examples:
- `jdbc:redis://localhost:6379/0`
- `jdbc:redis://user:pass@redis.example.com:6379/1`
- `jdbc:redis://localhost/0` (default port 6379)

#### Redis Cluster
```
jdbc:redis:cluster://[username:password@]host[:port]/database
```

Example:
- `jdbc:redis:cluster://cluster.example.com:6379/0`

### Connection Properties

You can set additional connection properties:

```java
Properties props = new Properties();
props.setProperty("user", "myuser");
props.setProperty("password", "mypassword");
props.setProperty("connectionTimeout", "5000");
props.setProperty("socketTimeout", "5000");
props.setProperty("clientName", "MyApp");

Connection conn = DriverManager.getConnection(url, props);
```

Supported properties:
- `user` / `username`: Redis username (for ACL authentication)
- `password`: Redis password
- `database`: Redis database number (0-15, default: 0)
- `connectionTimeout`: Connection timeout in milliseconds (default: 2000)
- `socketTimeout`: Socket timeout in milliseconds (default: 2000)
- `clientName`: Client name for Redis connection

### Prepared Statements

```java
PreparedStatement pstmt = conn.prepareStatement("SET ? ?");
pstmt.setString(1, "mykey");
pstmt.setString(2, "myvalue");
pstmt.executeUpdate();

pstmt = conn.prepareStatement("GET ?");
pstmt.setString(1, "mykey");
ResultSet rs = pstmt.executeQuery();
```

### Supported Redis Commands

The driver supports most Redis commands through SQL-like syntax:

#### String Operations
```sql
SET key value
GET key
DEL key
EXISTS key
KEYS pattern
TYPE key
TTL key
EXPIRE key seconds
```

#### Hash Operations
```sql
HSET key field value
HGET key field
HGETALL key
HDEL key field [field ...]
```

#### List Operations
```sql
LPUSH key element [element ...]
RPUSH key element [element ...]
LPOP key
RPOP key
LLEN key
LRANGE key start stop
```

#### Set Operations
```sql
SADD key member [member ...]
SMEMBERS key
SREM key member [member ...]
SCARD key
```

#### Sorted Set Operations
```sql
ZADD key score member [score member ...]
ZRANGE key start stop
ZCARD key
ZREM key member [member ...]
```

#### Server Operations
```sql
PING
INFO [section]
DBSIZE
FLUSHDB
FLUSHALL
SELECT database
```

### Result Sets

Redis command results are converted to tabular format:

- Single values: One row with a `value` column
- Lists/Sets: Multiple rows with a `value` column
- Hashes: Multiple rows with `field` and `value` columns
- Numeric results: One row with a `count` column

Example:
```java
ResultSet rs = stmt.executeQuery("HGETALL myhash");
while (rs.next()) {
    String field = rs.getString("field");
    String value = rs.getString("value");
    System.out.println(field + " = " + value);
}
```

## Building from Source

```bash
git clone <repository-url>
cd redis-jdbc-driver
mvn clean package
```

This will create:
- `target/redis-jdbc-driver-1.5.0.jar` - Main JAR file
- `target/redis-jdbc-driver-1.5.0-sources.jar` - Source JAR
- `target/redis-jdbc-driver-1.5.0-javadoc.jar` - Javadoc JAR

## Limitations

- Redis is not a traditional SQL database, so some JDBC features are not supported:
  - Transactions (except Redis transactions)
  - Stored procedures
  - Batch operations
  - Cursors (except basic forward-only)
  - Read-only result sets
- Complex SQL queries are not supported - use native Redis commands
- Some advanced Redis features may not be available through the JDBC interface

## License

MIT License - see LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit pull requests or open issues.

## Support

For issues and questions, please open an issue in the GitHub repository.
