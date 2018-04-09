package com.mongo2es.esTest;




import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Date;
import java.util.HashMap;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by guozi on 2018/1/15.
 */
public class EsTest {
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
    //设置ES集群的名称
    //自动嗅探整个集群的状态，把集群中其他ES节点的ip添加到本地的客户端列表中
     @Test
    public void test1() throws Exception{
         IndexResponse response = client.prepareIndex("guozi", "guozi", "1")
                 .setSource(jsonBuilder()
                         .startObject()
                         .field("user", "kimchy")
                         .field("postDate", new Date())
                         .field("message", "trying out Elasticsearch")
                         .endObject()
                 )
                 .get();

//         GetResponse response = client.prepareGet("test", "emp", "2")
//                 .setOperationThreaded(false)
//                 .get();
//         System.out.println(response.getSource());

    }


    String index = "test";//设置索引库
    String type = "emp";//设置类型

    //索引index（四种格式：json,map,bean,es helper）

    /**
     * index-1 json
     * 实际工作中使用
     * @throws Exception
     */
    @Test
    public void test4() throws Exception {
//创建映射
        XContentBuilder  mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                //      .startObject("m_id").field("type","keyword").endObject()
                .startObject("poi_index").field("type","integer").endObject()
                .startObject("poi_title").field("type","text").field("analyzer","english").endObject()
                .startObject("poi_address").field("type","text").field("analyzer","english").endObject()
                .startObject("poi_tags").field("type","text").field("analyzer","english").endObject()
                .startObject("poi_phone").field("type","text").field("analyzer","english").endObject()
                .startObject("poi_lng").field("type","text").endObject()
                .startObject("poi_lat").field("type","text").endObject()
                .endObject()
                .endObject();
        System.out.println(mapping.string());

        //pois：索引名   cxyword：类型名（可以自己定义）
        PutMappingRequest putmap = Requests.putMappingRequest("pois").type("cxyword").source(mapping);
        //创建索引
        client.admin().indices().prepareCreate("pois").execute().actionGet();
        //为索引添加映射
        client.admin().indices().putMapping(putmap).actionGet();
    }

    /**
     * index-2 hashmap
     * 实际工作中使用
     * @throws Exception
     */
    @Test
    public void test5() throws Exception {//把hashmap类型的数据放入index库
        HashMap<String, Object> hashMap = new HashMap<String, Object>();
        //HashMap<String, Object> hashMap是迭代器变量
        hashMap.put("poi_index", 555);
        hashMap.put("poi_title", "中国");
        IndexResponse indexResponse = client.prepareIndex("pois", "cxyword", "1")//添加一个id=2的数据
                .setSource(hashMap)//设值
                .get();
        //.execute().actionGet();   这个和上面的get()方法是一样的,get()就是对.execute().actionGet() 进行了封装
        System.out.println(indexResponse.getVersion());
    }

}


