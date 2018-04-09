package com.mongo2es.mongo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongo2es.inter.watcherInterface;
import com.mongo2es.utils.File2Json;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import static java.util.Arrays.asList;

/**
 * Mongo oplog
 * @author ZhangJianxin
 */
public class MongodbDriver  implements  Runnable{

    private MongoClient mongoClient = null;
    public static ConcurrentHashMap<String, Cursor> cursorList;
    public static MongodbDriver mongodbDriver = null;
    public Semaphore binary = new Semaphore(2);
    private MongodbDriver() {
        MongoClientOptions.Builder mcob = MongoClientOptions.builder();
        mcob.connectionsPerHost(1000);
        mcob.readPreference(ReadPreference.primary());
        MongoClientOptions mco = mcob.build();
        String[] clusterIp = "192.168.1.170,192.168.1.170".split(",");
        int port = 27017;
        List<ServerAddress> seeds = new ArrayList<ServerAddress>();
        for (String ip : clusterIp) {
            seeds.add(new ServerAddress(ip, port));
        }
        mongoClient = new MongoClient(seeds, mco);
    }

    public synchronized static MongodbDriver getMongodbDriver() {
        return mongodbDriver == null ? new MongodbDriver() : mongodbDriver;
    }
    /**
     *
     * @param watcherInterface
     *
     * */

    public void watchOplog(watcherInterface watcherInterface) throws InterruptedException {
        MongoDatabase db = mongoClient.getDatabase("toddb");
        MongoCollection coll = db.getCollection("testdb");

        List<Bson> filter = Collections.singletonList(
        (Aggregates.match(Filters.or(
                Filters.in("operationType", asList("insert","delete","update")))))

        );
        MongoCursor tsFind = coll.watch(filter).iterator();
        while(tsFind.hasNext()){
            ChangeStreamDocument result = (ChangeStreamDocument) (tsFind.next());
            JSONObject tableInfo = new JSONObject();
            tableInfo.put("table",result.getNamespace().getCollectionName());
            tableInfo.put("db",result.getNamespace().getDatabaseName());
            switch (result.getOperationType().getValue()){
                case "insert" :
                    try {
                        watcherInterface.insert(result.getFullDocument().toString(),tableInfo);
                        break;
                    }catch (Exception e){
                        System.out.println("Insert Exception :"+ e.getMessage());
                    }
                case "delete" :
                    try{
                        watcherInterface.delete(result.getDocumentKey().toString(),tableInfo);
                        break;
                    }catch (Exception e1){
                        System.out.println("Delete Exception :" + e1.getMessage());
                    }

                case "update" :
                    try {
                        watcherInterface.updata(result.getUpdateDescription().toString(),tableInfo);
                        break;
                    }catch (Exception e){
                        System.out.println("update Exception :" + e.getMessage());
                    }

                default:
                    System.out.println("Null");
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        MongodbDriver mongodbDriver = MongodbDriver.getMongodbDriver();
        Thread thread = new Thread(mongodbDriver);
        thread.start();
    }


    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        MongodbDriver d = new MongodbDriver();
        try {
            binary.acquire();
            d.watchOplog(new watcherInterface() {
                @Override
                public int insert(String inJson,JSONObject dbTable) {
                    System.out.println("insert");
                    System.out.println(inJson);
                    System.out.println(dbTable);
                    return 0;
                }

                @Override
                public int delete(String inJson,JSONObject dbTable) {
                    System.out.println("delete");
                    System.out.println(inJson);
                    System.out.println(dbTable);
                    return 0;
                }

                @Override
                public int updata(String inJson,JSONObject dbTable) {
                    System.out.println("updata");
                    System.out.println(inJson);
                    System.out.println(dbTable);
                    return 0;
                }
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            binary.release(); //out
        }
    }
}
