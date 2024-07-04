package com.example.basic;

import net.spy.memcached.ArcusClient;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.collection.CollectionAttributes;
import net.spy.memcached.collection.CollectionResponse;
import net.spy.memcached.collection.ElementValueType;
import net.spy.memcached.internal.CollectionFuture;
import net.spy.memcached.ops.*;
import net.spy.memcached.transcoders.CollectionTranscoder;
import net.spy.memcached.transcoders.SerializingTranscoder;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class HelloArcus {

    private static final String ARCUS_ADMIN = "127.0.0.1:2181";
    private static final String SERVICE_CODE = "test";
    private final ArcusClient arcusClient;

    public HelloArcus(String arcusAdmin, String serviceCode) {
        ConnectionFactoryBuilder cfb = new ConnectionFactoryBuilder();

        SerializingTranscoder trans = new CollectionTranscoder();
        trans.setCharset("UTF-8");
        trans.setCompressionThreshold(4096);

        cfb.setTranscoder(trans);

        // log4j logger를 사용하도록 설정합니다.
        // 코드에 직접 추가하지 않고 아래의 JVM 환경변수를 사용해도 됩니다.
        //   -Dnet.spy.log.LoggerImpl=net.spy.memcached.compat.log.Log4JLogger
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");

        // Arcus 클라이언트 객체를 생성합니다.
        // - arcusAdmin : Arcus 캐시 서버들의 그룹을 관리하는 admin 서버(ZooKeeper)의 주소입니다.
        // - serviceCode : 사용자에게 할당된 Arcus 캐시 서버들의 집합에 대한 코드값입니다.
        // - connectionFactoryBuilder : 클라이언트 생성 옵션을 지정할 수 있습니다.
        //
        // 정리하면 arcusAdmin과 serviceCode의 조합을 통해 유일한 캐시 서버들의 집합을 얻어 연결할 수 있는 것입니다.
        this.arcusClient = ArcusClient.createArcusClient(arcusAdmin, serviceCode, cfb);
    }

    public boolean setTest() {
        Future<Boolean> future = null;

        try {
            future = arcusClient.set("sample:testKey", 10, "testValue");
        } catch (IllegalStateException e) {
            // client operation queue 문제로 등록이 안 되었을 경우
        }

        if (future == null) return false;

        try {
            return future.get(500L, TimeUnit.MILLISECONDS); // (3)
        } catch (TimeoutException te) { // (4)
            future.cancel(true);
        } catch (ExecutionException re) { // (5)
            // operation queue 대기작업 취소
            future.cancel(true);
        } catch (InterruptedException ie) { // (6)
            // 해당 스레드가 interrupted
            future.cancel(true);
        }

        return false;
    }

    public void closeArcusConnection() {
        arcusClient.shutdown();
    }

    public String listenHello() {
        Future<Object> future = null;
        String result = "Not OK.";

        // Arcus의 "test:hello" 키의 값을 조회합니다.
        // Arcus에서는 가능한 모든 명령에 명시적으로 timeout 값을 지정하도록 가이드 하고 있으며
        // 사용자는 set을 제외한 모든 요청에 async로 시작하는 API를 사용하셔야 합니다.
        future = this.arcusClient.asyncGet("test:hello");

        try {
            result = (String)future.get(700L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }

        return result;
    }

    public boolean sayHello() {
        Future<Boolean> future = null;
        boolean setSuccess = false;

        // Arcus의 "test:hello" 키에 "Hello, Arcus!"라는 값을 저장합니다.
        // 그리고 Arcus의 거의 모든 API는 Future를 리턴하도록 되어 있으므로
        // 비동기 처리에 특화된 서버가 아니라면 반드시 명시적으로 future.get()을 수행하여
        // 반환되는 응답을 기다려야 합니다.
        future = this.arcusClient.set("test:hello", 600, "Hello, Arcus!");

        try {
            setSuccess = future.get(700L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }

        return setSuccess;
    }

    public boolean addTest() {
        Future<Boolean> future ;
        boolean addSuccess = false;

        future = this.arcusClient.add("test:add", 600, "Hello, Arcus!");

        try {
            addSuccess = future.get(700L, TimeUnit.MILLISECONDS);
        } catch (Exception e){
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }

        return addSuccess;
    }

    public StatusCode bulkTest() {
        Future<Map<String, OperationStatus>> future;
        StatusCode bulkSuccess = StatusCode.SUCCESS;

        List<String> keys = new LinkedList<>();

        keys.add("first");
        keys.add("second");
        keys.add("third");
        keys.add("fourth");

        future = this.arcusClient.asyncStoreBulk(StoreType.set, keys, 60, "day");

        try {
            Map<String, OperationStatus> result = future.get(700L, TimeUnit.MILLISECONDS);
            System.out.println("result = " + result);
            OperationStatus status = result.get(keys.get(0));

            Object first = this.arcusClient.get("first");
            System.out.println("first = " + first);
        } catch (Exception e){
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }

        return bulkSuccess;
    }

    public String getTest() {
        Future<Object> future = null;
        String result = "Not OK.";

        arcusClient.set("sample:testKey", 10, "testValue");

        future = this.arcusClient.asyncGet("sample:testKey");

        try {
            result = (String)future.get(700L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }

        return result;
    }

    public boolean deleteTest() {
        Future<Boolean> future = null;
        boolean deleteSuccess = false;

        this.arcusClient.set("sample:testKey", 10, "testValue");


        future = this.arcusClient.delete("sample:testKey");

        try {
            deleteSuccess = future.get(700L, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            if (future != null) future.cancel(true);
            e.printStackTrace();
        }

        return deleteSuccess;
    }

    public void setListTest() {
        String key = "Sample:List";

        CollectionFuture<Boolean> future = null;
        CollectionAttributes attribute = new CollectionAttributes();

        try {
            future = arcusClient.asyncLopCreate(key, ElementValueType.OTHERS, attribute); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }

        if (future == null)
            return;

        try {
            Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println(result);
            System.out.println(future.getOperationStatus().getResponse()); // (3)
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }

    }

    public void getListTest() {
        String key = "Sample:List";
        int indexFrom = 0;
        int indexTo = 5;
        boolean withDelete = false;
        boolean dropIfEmpty = false;
        CollectionFuture<List<Object>> future = null;

        try {
            future = arcusClient.asyncLopGet(key, indexFrom, indexTo, withDelete, dropIfEmpty); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }

        if (future == null)
            return;

        try {
            List<Object> result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println(result);
            CollectionResponse response = future.getOperationStatus().getResponse();  // (3)
            System.out.println(response);

            if (response.equals(CollectionResponse.NOT_FOUND)) {
                System.out.println("Key가 없습니다.(Key를 가진 아이템이 없습니다.)");
            } else if (response.equals(CollectionResponse.NOT_FOUND_ELEMENT)) {
                System.out.println("Key는 존재하지만 List에 저장된 값 중 조건에 맞는 것이 없습니다.");
            }

        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }
    }

    public void insertListTest() {
        String key = "Sample:List";
        int index = -1;
        String value = "This is a value.";
        CollectionAttributes attributesForCreate = new CollectionAttributes();
        CollectionFuture<Boolean> future = null;

        try {
            future = arcusClient.asyncLopInsert(key, index, value, attributesForCreate); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }

        if (future == null)
            return;

        try {
            Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println(result);
            System.out.println(future.getOperationStatus().getResponse()); // (3)
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }
    }

    public void createSetTest() {
        String key = "Sample:EmptySet";
        CollectionFuture<Boolean> future = null;
        CollectionAttributes attribute = new CollectionAttributes();
        try {
            future = arcusClient.asyncSopCreate(key, ElementValueType.OTHERS,
                    attribute); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }
        if (future == null)
            return;
        try {
            Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println(result);
            System.out.println(future.getOperationStatus().getResponse()); // (3)
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }
    }

    public void insertSetTest() {
        String key = "Sample:Set";
        String value = "This is a value.";
        CollectionAttributes attributesForCreate = new CollectionAttributes();
        CollectionFuture<Boolean> future = null;

        try {
            future = arcusClient.asyncSopInsert(key, value, attributesForCreate); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }

        if (future == null)
            return;

        try {
            Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println(result); // (3)
            System.out.println(future.getOperationStatus().getResponse()); // (4)
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }
    }

    public void getSetTest() {
        String key = "Sample:Set";
        int count = 10;
        boolean withDelete = false;
        boolean dropIfEmpty = false;

        CollectionFuture<Set<Object>> future = null;

        try {
            future = arcusClient.asyncSopGet(key, count, withDelete, dropIfEmpty); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }

        if (future == null)
            return;

        try {
            Set<Object> result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println("result: " + result);
            System.out.println("status response: " + future.getOperationStatus().getResponse()); // (3)
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }

    }

    public void deleteSetTest() {
        String key = "Sample:Set";
        String value = "This is a value.";
        boolean dropIfEmpty = false;
        CollectionFuture<Boolean> future = null;

        try {
            future = arcusClient.asyncSopDelete(key, value, dropIfEmpty); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }

        if (future == null)
            return;

        try {
            boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println(result);
            CollectionResponse response = future.getOperationStatus().getResponse(); // (3)
            System.out.println(response);
        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }
    }

    public void insertMapTest() {
        String key = "Prefix:MapKey";
        String mkey = "mkey";
        String value = "This is a value.";

        CollectionAttributes attributesForCreate = new CollectionAttributes();
        CollectionFuture<Boolean> future = null;

        try {
            future = arcusClient.asyncMopInsert(key, mkey, value, attributesForCreate); // (1)
        } catch (IllegalStateException e) {
            // handle exception
        }

        if (future == null)
            return;

        try {
            Boolean result = future.get(1000L, TimeUnit.MILLISECONDS); // (2)
            System.out.println(result);
            System.out.println(future.getOperationStatus().getResponse()); // (3)
        } catch (TimeoutException e) {
            future.cancel(true);
        } catch (InterruptedException e) {
            future.cancel(true);
        } catch (ExecutionException e) {
            future.cancel(true);
        }
    }
}
