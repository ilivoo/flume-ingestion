package com.ilivoo.flume.utils;

import com.google.gson.Gson;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

public class TestJsonUtil {

    @Test
    public void testJsonToObjectMap() {
        Hello hello = new Hello();
        Gson gson = new Gson();
        String json = gson.toJson(hello);
        System.out.println(json);
        Map<String, Object> map = JsonUtil.jsonToObjectMap(json);
        System.out.println(map);
    }

    private static class Hello {
        private boolean aBoolean = false;
        private byte aByte = 1;
        private char aChar = 97;
        private short aShort = 1;
        private int anInt = 1;
        private long aLong = 1;
        private float aFloat = 1;
        private double aDouble = 1;
        private BigInteger bigInteger = new BigInteger("1");
        private BigDecimal bigDecimal = new BigDecimal(1);
        private String aString = "hello";
        private int[] intArray = new int[]{1, 1};
        private String[] stringArray = new String[]{"hello", "hello"};
        private World world = new World();
        private String nullString;
        private World nullWorld;
    }

    private static class World {
        private int anInt = 2;
        private String aString = "world";
    }
}
