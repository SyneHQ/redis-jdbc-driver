package com.synehq.jdbc.redis;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisClientConfig;
import java.util.Properties;

public class RedisSecurityTest {
    private static void check(boolean value) { if (!value) throw new AssertionError(); }

    @Test public void parametersRemainSingleArguments() throws Exception {
        RedisPreparedStatement statement = new RedisPreparedStatement(null, "SET ? ?");
        statement.setString(1, "key?\\quoted\"");
        statement.setString(2, "value\" other NX\\tail");
        java.lang.reflect.Method method = RedisPreparedStatement.class.getDeclaredMethod("processParameters", String.class);
        method.setAccessible(true);
        RedisCommand command = RedisCommandParser.parse((String) method.invoke(statement, "SET ? ?"));
        check(command.getArgs().length == 2);
        check(command.getArgs()[0].equals("key?\\quoted\""));
        check(command.getArgs()[1].equals("value\" other NX\\tail"));
        check(RedisCommandParser.parse("SET key \"\"").getArgs()[1].isEmpty());
        try { RedisCommandParser.parse("SET key \"unterminated"); throw new AssertionError(); }
        catch (IllegalArgumentException expected) { }
        try { statement.setString(1001, "value"); throw new AssertionError(); }
        catch (java.sql.SQLException expected) { }
    }

    @Test public void tlsAndAclCredentialsReachClientConfiguration() throws Exception {
        check(RedisDriver.enforcesTlsAndAclAuthentication());
        Properties props = new Properties();
        props.setProperty("user", "fixture-user");
        props.setProperty("password", "fixture-password");
        props.setProperty("ssl", "true");
        RedisConnection connection = new RedisConnection("jdbc:redis://localhost:6379/0", props);
        java.lang.reflect.Field field = RedisConnection.class.getDeclaredField("jedisPool"); field.setAccessible(true);
        JedisPool pool = (JedisPool) field.get(connection);
        Object factory = pool.getFactory();
        java.lang.reflect.Field configField = factory.getClass().getDeclaredField("clientConfig"); configField.setAccessible(true);
        JedisClientConfig config = (JedisClientConfig) configField.get(factory);
        check(config.isSsl());
        check(config.getUser().equals("fixture-user"));
        check(config.getPassword().equals("fixture-password"));
        check(config.getHostnameVerifier() != null);
        connection.close();
        props.setProperty("verifyServerCertificate", "false");
        try { new RedisConnection("jdbc:redis://localhost:6379/0", props); throw new AssertionError(); }
        catch (java.sql.SQLException expected) { }
        try { new RedisConnection("jdbc:redis://private:credential@invalid host/0", props); throw new AssertionError(); }
        catch (java.sql.SQLException expected) { check(!expected.getMessage().contains("credential")); }
    }

    @Test public void authenticationFailureIsNotIgnored() throws Exception {
        java.net.ServerSocket server = new java.net.ServerSocket(0, 1, java.net.InetAddress.getLoopbackAddress());
        server.setSoTimeout(3000);
        java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        Thread responder = new Thread(() -> {
            try (java.net.Socket socket = server.accept()) {
                socket.setSoTimeout(3000);
                check(socket.getInputStream().read() == '*');
                socket.getOutputStream().write("-WRONGPASS invalid credentials\r\n".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                socket.getOutputStream().flush();
            } catch (Throwable error) { failure.set(error); }
        });
        responder.start();
        Properties props = new Properties();
        props.setProperty("user", "fixture-user"); props.setProperty("password", "fixture-password");
        try (RedisConnection connection = new RedisConnection("jdbc:redis://127.0.0.1:" + server.getLocalPort() + "/0", props)) {
            try { connection.getJedis(); throw new AssertionError(); }
            catch (java.sql.SQLException expected) { check(!expected.getMessage().contains("fixture-password")); }
        } finally { server.close(); responder.join(4000); }
        check(!responder.isAlive() && failure.get() == null);
    }

    public static void main(String[] args) throws Exception {
        RedisSecurityTest tests = new RedisSecurityTest();
        tests.parametersRemainSingleArguments();
        tests.tlsAndAclCredentialsReachClientConfiguration();
        tests.authenticationFailureIsNotIgnored();
        System.out.println("Redis security checks passed");
    }
}
