package com.mongo2es.esTest;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.sum.InternalSum;
import org.elasticsearch.search.aggregations.metrics.sum.SumAggregationBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

/**
 * Created by guozi on 2018/1/18.
 */
public class QueryApi {
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
    public void matchAllTest() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData")//指定类型
                .setQuery(QueryBuilders.matchAllQuery())//指定查询条件
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    /**
     * search查询详解
     * @throws Exception
     */
    @Test
    public void matchQueryTest() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData")//指定类型
                .setQuery(QueryBuilders.matchQuery("senCity", "武汉"))//指定查询条件
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    @Test
    public void multiMatchQueryTest() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData")//指定类型
                .setQuery(QueryBuilders.multiMatchQuery("武汉","senCity", "recCity"))//指定查询条件
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    @Test
    public void matchPhraseQueryTest() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData")//指定类型
                .setQuery(QueryBuilders.matchPhraseQuery("senName","欧阳"))//指定查询条件
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
            System.out.println(searchHit.getScore());

        }
    }

    @Test
    public void termQueryTest() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData")//指定类型
                .setQuery(termQuery("mailNo","667748612925"))//指定查询条件
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    @Test
    public void rangeQueryTest() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData")//指定类型
                .setQuery(QueryBuilders.rangeQuery("weight")
                        .from(5)
                        .to(10)
                        .includeLower(true)
                        .includeUpper(false)   )//指定查询条件
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);

        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
        }
    }

    @Test
    public void boolQueryTest() throws Exception {
        SearchResponse searchResponse = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData")//指定类型
                .setQuery(QueryBuilders.boolQuery()
                        .must(termQuery("recProv", "湖北省"))
                        .mustNot(termQuery("mailNo", "667748612924"))
                        .should(termQuery("senName", "李四"))
                        .filter(termQuery("recCity", "武汉市"))
                )
                .setExplain(true)//按照查询数据的匹配度返回数据
                .get();

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits();
        System.out.println("总数："+totalHits);
        //获取满足条件数据的详细内容
        SearchHit[] hits2 = hits.getHits();
        for (SearchHit searchHit : hits2) {
            System.out.println(searchHit.getSourceAsString());
            System.out.println(searchHit.getScore());
        }
    }

    @Test
    public void aggOneFileTest() throws Exception{
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData");//指定类型
        TermsAggregationBuilder recProvAgg= AggregationBuilders.terms("recProv_count").field("recProv");
        SearchResponse searchResponse = searchRequestBuilder.addAggregation(recProvAgg).get();
        Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
        StringTerms recProvTerms= (StringTerms) aggMap.get("recProv_count");
        System.out.println(recProvTerms);
        Iterator<StringTerms.Bucket> teamBucketIt = recProvTerms.getBuckets().iterator();
        while (teamBucketIt.hasNext()){
            StringTerms.Bucket buck = teamBucketIt .next();
            //省名
            String team = buck.getKeyAsString();
            //记录数
            long count = buck.getDocCount();
            System.out.println(team+":"+count);
        }
    }

    @Test
    public void aggMultipleFileTest() throws Exception{
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData");//指定类型
        TermsAggregationBuilder recProvAgg= AggregationBuilders.terms("recProv_count").field("recProv");
        TermsAggregationBuilder recCityAgg= AggregationBuilders.terms("recCity_count").field("recCity");
        recProvAgg.subAggregation(recCityAgg);
        SearchResponse searchResponse = searchRequestBuilder.addAggregation(recProvAgg).get();
        Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
        StringTerms recProvTerms= (StringTerms) aggMap.get("recProv_count");
        Iterator<StringTerms.Bucket> teamBucketIt = recProvTerms.getBuckets().iterator();
        while (teamBucketIt.hasNext()){
            StringTerms.Bucket buck = teamBucketIt .next();
            //省名
            String team = buck.getKeyAsString();
            //记录数
            long count = buck.getDocCount();
            System.out.println(team+":"+count);

            Map<String, Aggregation> subaggmap = buck.getAggregations().asMap();
            StringTerms recCityTerms= (StringTerms) subaggmap.get("recCity_count");
            Iterator<StringTerms.Bucket> recCityBucketIt = recCityTerms.getBuckets().iterator();
            while (recCityBucketIt.hasNext()){
                StringTerms.Bucket recCityBuck = recCityBucketIt .next();
                //市名
                String recCityteam = recCityBuck.getKeyAsString();
                //记录数
                long recCitycount = recCityBuck.getDocCount();
                System.out.println(recCityteam +":"+recCitycount);
            }
        }
    }

    @Test
    public void aggSumTest() throws Exception{
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch("orders")//指定索引库
                .setTypes("ordersData");//指定类型
        TermsAggregationBuilder recProvAgg= AggregationBuilders.terms("recProv_count").field("recProv");
        TermsAggregationBuilder recCityAgg= AggregationBuilders.terms("recCity_count").field("recCity");
        SumAggregationBuilder sumAggregationBuilder =AggregationBuilders.sum("weight_count").field("weight");
        recCityAgg.subAggregation(sumAggregationBuilder);
        recProvAgg.subAggregation(recCityAgg);
        SearchResponse searchResponse = searchRequestBuilder.addAggregation(recProvAgg).get();
        Map<String, Aggregation> aggMap = searchResponse.getAggregations().asMap();
        StringTerms recProvTerms= (StringTerms) aggMap.get("recProv_count");
        Iterator<StringTerms.Bucket> teamBucketIt = recProvTerms.getBuckets().iterator();
        while (teamBucketIt.hasNext()){
            StringTerms.Bucket buck = teamBucketIt .next();
            //省名
            String team = buck.getKeyAsString();
            //记录数
            long count = buck.getDocCount();
            System.out.println(team+":"+count);

            Map<String, Aggregation> subaggmap = buck.getAggregations().asMap();
            StringTerms recCityTerms= (StringTerms) subaggmap.get("recCity_count");
            Iterator<StringTerms.Bucket> recCityBucketIt = recCityTerms.getBuckets().iterator();
            while (recCityBucketIt.hasNext()){
                StringTerms.Bucket recCityBuck = recCityBucketIt .next();
                //市名
                String recCityteam = recCityBuck.getKeyAsString();
                //记录数
                long recCitycount = recCityBuck.getDocCount();
                System.out.println(recCityteam +":"+recCitycount);

                recCityBuck.getAggregations().asMap().get("weight_count");
                //sum值获取方法
                double total_salary = ((InternalSum)recCityBuck.getAggregations().asMap().get("weight_count")).getValue();
                System.out.println("总重量："+total_salary);

            }
        }
    }
}
