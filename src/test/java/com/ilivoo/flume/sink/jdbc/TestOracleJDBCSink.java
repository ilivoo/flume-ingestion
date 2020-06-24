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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class TestOracleJDBCSink {

    String connectionString = "jdbc:oracle:thin:@192.168.4.101:1521:orcl";
    String driver = "oracle.jdbc.OracleDriver";
    String user = "scott";
    String password = "tepia2020";
    String database = "scott";
    Connection conn = null;
    String batchSize = "2";
    @Before
    public void setUp() throws Exception {
        Class.forName(driver).newInstance();
        conn = DriverManager.getConnection(connectionString, user, password);
        try {
            conn.createStatement().execute("drop table tmq");
            conn.prepareStatement("CREATE TABLE tmq(myId number(10) primary key, myInteger number(10), myString varchar2(50))").execute();
        } catch (Exception ex) {
            conn.prepareStatement("CREATE TABLE tmq(myId number(10) primary key, myInteger number(10), myString varchar2(50))").execute();
        }
        try {
            conn.createStatement().execute("drop table tmq2");
            conn.prepareStatement("CREATE TABLE tmq2(myId number(10) primary key, myInteger number(10), myString varchar2(50))").execute();
        } catch (Exception ex) {
            conn.prepareStatement("CREATE TABLE tmq2(myId number(10) primary key, myInteger number(10), myString varchar2(50))").execute();
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
        ctx.put("conn.dataSource.user", user);
        ctx.put("conn.dataSource.password", password);
        ctx.put("database", database);
        ctx.put("batchSize", batchSize);
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
        String body = "{\"myId\": 1, \"myInteger\":1,\"myString\":\"welcome\"}";
        Event event = EventBuilder.withBody(body.getBytes("UTF-8"), headers);
        channel.put(event);
        Map<String, String> headers2 = new HashMap<String, String>();
        headers2.put("table", "tmq");
        String body2 = "{\"myId\": 2, \"myInteger\":2,\"myString\":\"hello\"}";
//        String body2 = "{\"myInteger\":2}";
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
}
