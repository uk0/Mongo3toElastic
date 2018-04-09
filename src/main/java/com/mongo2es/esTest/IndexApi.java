package com.mongo2es.esTest;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by guozi on 2018/1/17.
 */
public class IndexApi {
    protected TransportClient client;

    @Before
    public void createClient() throws Exception{
        Settings esSettings = Settings.builder()
                .put("cluster.name", "my-application")
                .put("client.transport.sniff", true)
                .build();
        client = new PreBuiltTransportClient(esSettings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.1.80"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.1.81"), 9300))
                .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.1.82"), 9300));
    }

    @After
    public void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
    }

    @Test
    public void createIndex() throws IOException {
        //创建映射
        XContentBuilder  mapping = jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("mailNo").field("type","keyword").endObject()
                .startObject("weight").field("type","integer").endObject()
                .startObject("recDatetime").field("type","date").field("format","yyyy-mm-dd").field("index","false").endObject()
                .startObject("recProv").field("type","keyword").endObject()
                .startObject("recCity").field("type","keyword").endObject()
                .startObject("recAddress").field("type","text").endObject()
                .startObject("recName").field("type","text").field("analyzer","standard").endObject()
                .startObject("recPhone").field("type","keyword").endObject()
                .startObject("senProv").field("type","keyword").endObject()
                .startObject("senCity").field("type","keyword").endObject()
                .startObject("senAddress").field("type","text").endObject()
                .startObject("senName").field("type","text").field("analyzer","standard").endObject()
                .startObject("senPhone").field("type","keyword").endObject()
                .endObject()
                .endObject();
        System.out.println(mapping.string());
        PutMappingRequest putmap = Requests.putMappingRequest("orders").type("ordersData").source(mapping);//orders：索引名   ordersdata：类型名
        //创建索引
        client.admin().indices().prepareCreate("orders").execute().actionGet();
        //为索引添加映射
        client.admin().indices().putMapping(putmap).actionGet();
    }

    @Test
    public void insertDataByMap(){
        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        //HashMap<String, Object> hashMap是迭代器变量
        hashMap.put("mailNo", "667748612922");
        hashMap.put("weight", 5);
        hashMap.put("recDatetime", "2018-01-01");
        hashMap.put("recProv", "湖北省");
        hashMap.put("recCity", "黄冈市");
        hashMap.put("recAddress", "新成街道建北街63号，英大财险保险股份有限公司");
        hashMap.put("recName", "张三");
        hashMap.put("recPhone", "18512345678");
        hashMap.put("senProv", "广东省");
        hashMap.put("senCity", "广州市");
        hashMap.put("senAddress", "新站街道中安新胜小区");
        hashMap.put("senName", "李四");
        hashMap.put("senPhone", "17112345678");
        //添加一个id=1的数据
        IndexResponse indexResponse = client.prepareIndex("orders", "ordersData")
                .setSource(hashMap)//设值
                .get();
        //.execute().actionGet();get()就是对.execute().actionGet() 进行了封装
        System.out.println(indexResponse.getVersion());
    }

    @Test
    public void insertDataByBuilder() throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("mailNo", "667748612966")
                .field("weight", 10)
                .field("recDatetime", new Date())
                .field("recProv", "湖南省")
                .field("recCity", "岳阳市")
                .field("recAddress", "东城街道劳动局斜对面永江门窗")
                .field("recName", "王五")
                .field("recPhone", "18312345678")
                .field("senProv", "广西省")
                .field("senCity", "南宁市")
                .field("senAddress", "福永街道悦昌路超喜B栋")
                .field("senName", "赵六")
                .field("senPhone", "15512345678")
                .endObject();
        String json = builder.string();
        System.out.println(json);
        IndexResponse response = client.prepareIndex("orders", "ordersData")
                .setSource(builder)
                .get();
    }
    @Test
    public void insertBulk() throws IOException {
        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        hashMap.put("mailNo", "667748612928");
        hashMap.put("weight", 5);
        hashMap.put("recDatetime", "2016-07-16");
        hashMap.put("recProv", "湖北省");
        hashMap.put("recCity", "武汉市");
        hashMap.put("recAddress", "新成街道建北街63号，英大财险保险股份有限公司");
        hashMap.put("recName", "张三");
        hashMap.put("recPhone", "18512345678");
        hashMap.put("senProv", "广东省");
        hashMap.put("senCity", "广州市");
        hashMap.put("senAddress", "新站街道中安新胜小区");
        hashMap.put("senName", "赵六");
        hashMap.put("senPhone", "17112345678");
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("mailNo", "667748612929")
                .field("weight", 10)
                .field("recDatetime", "2018-02-06")
                .field("recProv", "湖南省")
                .field("recCity", "长沙市")
                .field("recAddress", "东城街道劳动局斜对面永江门窗")
                .field("recName", "王五")
                .field("recPhone", "18312345678")
                .field("senProv", "广西省")
                .field("senCity", "南宁市")
                .field("senAddress", "福永街道悦昌路超喜B栋")
                .field("senName", "赵六")
                .field("senPhone", "15512345678")
                .endObject();
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.add(client.prepareIndex("orders", "ordersData", "7")
                .setSource(hashMap)
        );
        bulkRequest.add(client.prepareIndex("orders", "ordersData", "8")
                .setSource(builder)
        );
        BulkResponse bulkResponse = bulkRequest.get();
    }
     @Test
    public void deleteDataById(){
        DeleteResponse response = client.prepareDelete("orders", "ordersData", "4").get();
    }

    @Test
    public void deleteData(){
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.matchQuery("mailNo", "667748612925"))
                        .source("orders")
                        .get();
        long deleted = response.getDeleted();
        System.out.println(deleted);
    }

    @Test
    public void updateData() throws Exception{
        UpdateResponse updateResponse = client.prepareUpdate("orders", "ordersData", "3")
                .setDoc(jsonBuilder()
                        .startObject()
                        .field("recName", "张四")
                        .endObject())
                .get();
        System.out.println(updateResponse.status());
    }
}
