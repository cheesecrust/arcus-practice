package com.example.basic;

import net.spy.memcached.ops.StatusCode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class BasicApplicationTests {

    HelloArcus helloArcus = new HelloArcus("127.0.0.1:2181", "test");

    @BeforeEach
    public void sayHello() {
        helloArcus.sayHello();
    }

    @Test
    public void contextLoads() {
        Assertions.assertEquals("Hello, Arcus!", helloArcus.listenHello());
    }

    @Test
    public void setTest() {
        Assertions.assertTrue(helloArcus.setTest());
    }

    @Test
    public void addTest() {
        Assertions.assertTrue(helloArcus.addTest());
    }

    @Test
    public void bulkTest(){
        System.out.println(helloArcus.bulkTest());
    }

    @Test
    public void getTest() {
        Assertions.assertEquals("testValue", helloArcus.getTest());
    }

    @Test
    public void deleteTest() {
        Assertions.assertTrue(helloArcus.deleteTest());
    }

    @Test
    public void setListTest(){
        helloArcus.setListTest();
    }

    @Test
    public void getListTest(){
        helloArcus.getListTest();
    }

    @Test
    public void insertListTest(){
        helloArcus.insertListTest();
    }

    @Test
    public void createSetTest() {
        helloArcus.createSetTest();
    }

    @Test
    public void insertSetTest() {
        helloArcus.insertSetTest();
    }

    @Test
    public void getSetTest() {
        helloArcus.getSetTest();
    }

    @Test
    public void deleteSetTest() {
        helloArcus.deleteSetTest();
    }

    @Test
    public void insertMapTest() {
        helloArcus.insertMapTest();
    }

    
}
