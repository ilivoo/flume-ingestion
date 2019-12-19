package com.ilivoo.flume.sink.jdbc;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

public class TestJDBCSink {

    String connectionString = "jdbc:mysql://127.0.0.1:3306/test?useSSL=false&user=root&password=root";
    String driver = "com.mysql.jdbc.Driver";
    Connection conn = null;
    @Before
    public void setUp() throws Exception {
        Class.forName(driver).newInstance();
        conn = DriverManager.getConnection(connectionString);
        try {
            conn.createStatement().execute("drop table tmq;");
            conn.prepareStatement("CREATE TABLE tmq(myId int AUTO_INCREMENT, myInteger int, myString varchar(50), PRIMARY KEY(myId));").execute();
        } catch (Exception ex) {
            conn.prepareStatement("CREATE TABLE tmq(myId int AUTO_INCREMENT, myInteger int, myString varchar(50), PRIMARY KEY(myId));").execute();
        }
        try {
            conn.createStatement().execute("drop table tmq2;");
            conn.prepareStatement("CREATE TABLE tmq2(myId int AUTO_INCREMENT, myInteger int, myString varchar(50), PRIMARY KEY(myId));").execute();
        } catch (Exception ex) {
            conn.prepareStatement("CREATE TABLE tmq2(myId int AUTO_INCREMENT, myInteger int, myString varchar(50), PRIMARY KEY(myId));").execute();
        }
    }

    @After
    public void tearDown() throws Exception {
        conn.close();
    }

    @Test
    public void testMappingQuery() throws Exception {
        Context ctx = new Context();
        ctx.put("driver", driver);
        ctx.put("conn.jdbcUrl", connectionString);
        ctx.put("batchSize", "2");
        ctx.put("dataFormat", "bodyJson");
        JDBCSink jdbcSink = new JDBCSink();
        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");
        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("table", "tmq");
        String body = "{\"myInteger\":1,\"myString\":\"welcome\"}";
        Event event = EventBuilder.withBody(body.getBytes("UTF-8"), headers);
        channel.put(event);
        Map<String, String> headers2 = new HashMap<String, String>();
        headers2.put("table", "tmq");
        String body2 = "{\"myInteger\":2,\"myString\":\"hello\"}";
        Event event2 = EventBuilder.withBody(body2.getBytes("UTF-8"), headers2);
        channel.put(event2);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();

        ResultSet rs = conn.prepareStatement("SELECT count(*) AS count FROM tmq").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 2);
        }

        rs = conn.prepareStatement("SELECT * FROM tmq").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
            assertTrue(rs.getInt(2) == 1);
            assertTrue(rs.getString(3).equals("welcome"));
        }
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 2);
            assertTrue(rs.getInt(2) == 2);
            assertTrue(rs.getString(3).equals("hello"));
        }
    }

    @Test
    public void testMappingQueryJsonBody() throws Exception {
        Context ctx = new Context();
        ctx.put("driver", driver);
        ctx.put("conn.jdbcUrl", connectionString);
        ctx.put("batchSize", "2");
        JDBCSink jdbcSink = new JDBCSink();
        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");
        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("table", "tmq");
        headers.put("myInteger", "1");
        headers.put("myString", "hello");
        Event event = EventBuilder.withBody(new byte[0], headers);
        channel.put(event);
        Map<String, String> headers2 = new HashMap<String, String>();
        headers2.put("table", "tmq2");
        headers2.put("myInteger", "2");
        headers2.put("myString", "hello");
        Event event2 = EventBuilder.withBody(new byte[0], headers2);
        channel.put(event2);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();

        ResultSet rs = conn.prepareStatement("SELECT count(*) AS count FROM tmq").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
        }

        rs = conn.prepareStatement("SELECT * FROM tmq").executeQuery();
        while (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
            assertTrue(rs.getInt(2) == 1);
            assertTrue(rs.getString(3).equals("hello"));
        }

        rs = conn.prepareStatement("SELECT count(*) AS count FROM tmq2").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
        }

        rs = conn.prepareStatement("SELECT * FROM tmq2").executeQuery();
        while (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
            assertTrue(rs.getInt(2) == 2);
            assertTrue(rs.getString(3).equals("hello"));
        }
    }

    @Test
    public void testTemplateQueryJsonBody() throws Exception {
        Context ctx = new Context();
        ctx.put("driver", driver);
        ctx.put("conn.jdbcUrl", connectionString);
        ctx.put("batchSize", "2");
        ctx.put("dataFormat", "bodyJson");
        ctx.put("sql", "INSERT INTO $${tmq} (${myInteger}, ${myString}) VALUES (#{header.myInteger}, #{body})");
        JDBCSink jdbcSink = new JDBCSink();
        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");
        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        Map<String, String> headers = new HashMap<String, String>();
        String body = "{\"myInteger\":\"2\",\"myString\":\"hello\"}";
        Event event = EventBuilder.withBody(body.getBytes("UTF-8"), headers);
        channel.put(event);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();

        ResultSet rs = conn.prepareStatement("SELECT count(*) AS count FROM tmq").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
        }

        rs = conn.prepareStatement("SELECT * FROM tmq").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
            assertTrue(rs.getInt(2) == 2);
            assertTrue(rs.getString(3).equals(body));
        }
    }

    @Test
    public void testTemplateQuery() throws Exception {
        Context ctx = new Context();
        ctx.put("driver", driver);
        ctx.put("conn.jdbcUrl", connectionString);
        ctx.put("batchSize", "2");
        ctx.put("sql", "INSERT INTO $${tmq} (${myInteger}, ${myString}) VALUES (#{body}, #{header.myString})");
        JDBCSink jdbcSink = new JDBCSink();
        Configurables.configure(jdbcSink, ctx);

        Context channelContext = new Context();
        channelContext.put("capacity", "10000");
        channelContext.put("transactionCapacity", "200");
        Channel channel = new MemoryChannel();
        channel.setName("junitChannel");
        Configurables.configure(channel, channelContext);

        jdbcSink.setChannel(channel);

        channel.start();
        jdbcSink.start();

        Transaction tx = channel.getTransaction();
        tx.begin();
        Map<String, String> headers = new HashMap<String, String>();
        headers.put("myString", "hello");
        Event event = EventBuilder.withBody("2".getBytes("UTF-8"), headers);
        channel.put(event);

        tx.commit();
        tx.close();

        jdbcSink.process();

        jdbcSink.stop();
        channel.stop();

        ResultSet rs = conn.prepareStatement("SELECT count(*) AS count FROM tmq").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
        }

        rs = conn.prepareStatement("SELECT * FROM tmq").executeQuery();
        if (rs.next()) {
            assertTrue(rs.getInt(1) == 1);
            assertTrue(rs.getInt(2) == 2);
            assertTrue(rs.getString(3).equals("hello"));
        }
    }
}
