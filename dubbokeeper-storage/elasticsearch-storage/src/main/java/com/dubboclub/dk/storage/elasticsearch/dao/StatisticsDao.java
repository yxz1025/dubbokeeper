package com.dubboclub.dk.storage.elasticsearch.dao;

import com.dubboclub.dk.storage.elasticsearch.ElasticTemplate;
import com.dubboclub.dk.storage.model.ServiceInfo;
import com.dubboclub.dk.storage.model.Statistics;
import com.dubboclub.dk.storage.elasticsearch.dto.TempMethodOveride;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount;
import org.elasticsearch.search.sort.SortOrder;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by yuanxiaozhong on 2018/5/6.
 */
public class StatisticsDao extends AbstractEsDao {

    private static final String STATISTICS_INDEX_PREFIX = "statistics_";
    private static final Integer PAGE_SIZE = 1000;

    private ElasticTemplate elasticTemplate;

    public ElasticTemplate getElasticTemplate() {
        return elasticTemplate;
    }

    public StatisticsDao setElasticTemplate(ElasticTemplate elasticTemplate) {
        this.elasticTemplate = elasticTemplate;
        return this;
    }

    public void addOne(String application,Statistics statistics){
        boolean result = elasticTemplate.indexExists(STATISTICS_INDEX_PREFIX + application);
        if(!result){
            elasticTemplate.createIndex(STATISTICS_INDEX_PREFIX + application, application, getMapping());
        }
        elasticTemplate.save(STATISTICS_INDEX_PREFIX + application, application, createObjToJson(statistics));
    }

    private HashMap<String, Object> createObjToJson(Statistics statistics){
        HashMap<String, Object> map = new HashMap<>();
        map.put("timestamp", statistics.getTimestamp());
        map.put("serviceInterface", statistics.getServiceInterface());
        map.put("method", statistics.getMethod());
        map.put("type", statistics.getType()== Statistics.ApplicationType.CONSUMER?0:1);
        map.put("tps", statistics.getTps());
        map.put("kbps", statistics.getKbps());
        map.put("host", statistics.getHost());
        map.put("elapsed", statistics.getElapsed());
        map.put("concurrent", statistics.getConcurrent());
        map.put("input", statistics.getInput());
        map.put("output", statistics.getOutput());
        map.put("successCount", statistics.getSuccessCount());
        map.put("failureCount", statistics.getFailureCount());
        map.put("remoteAddress", statistics.getRemoteAddress());
        map.put("remoteType", statistics.getRemoteType() == Statistics.ApplicationType.CONSUMER ? 0: 1);
        return map;
    }

    private void processResultHitsItem(final List<Statistics> list, SearchHit item, String application) {
        Statistics info = new Statistics();
        info.setApplication(application);
        info.setConcurrent(Long.valueOf(String.valueOf(item.getSource().get("concurrent"))));
        info.setElapsed(Long.valueOf(String.valueOf(item.getSource().get("elapsed"))));
        info.setFailureCount(Integer.valueOf(String.valueOf(item.getSource().get("failureCount"))));
        info.setHost(String.valueOf(item.getSource().get("failureCount")));
        info.setInput(Long.valueOf(String.valueOf(item.getSource().get("input"))));
        info.setKbps(Double.valueOf(String.valueOf(item.getSource().get("kbps"))));
        info.setMethod(String.valueOf(item.getSource().get("method")));
        info.setOutput(Long.valueOf(String.valueOf(item.getSource().get("output"))));
        info.setRemoteAddress(String.valueOf(item.getSource().get("remoteAddress")));
        info.setSuccessCount(Integer.valueOf(String.valueOf(item.getSource().get("successCount"))));
        info.setTimestamp(Long.valueOf(String.valueOf(item.getSource().get("timestamp"))));
        info.setServiceInterface(String.valueOf(item.getSource().get("serviceInterface")));
        info.setTps(Double.valueOf(String.valueOf(item.getSource().get("tps"))));
        info.setType((item.getSource().get("type").equals(0) ? Statistics.ApplicationType.CONSUMER : Statistics.ApplicationType.PROVIDER));
        info.setRemoteType((item.getSource().get("remoteType").equals(0) ? Statistics.ApplicationType.CONSUMER : Statistics.ApplicationType.PROVIDER));
        list.add(info);
    }

    @Override
    public XContentBuilder getMapping() {
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject("properties")
                        .startObject("timestamp")
                            .field("type", "date")
                            .field("format", "strict_date_optional_time||epoch_millis")
                        .endObject()
                        .startObject("serviceInterface")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("method")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("type")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("tps")
                            .field("type", "float")
                        .endObject()
                        .startObject("kbps")
                            .field("type", "float")
                        .endObject()
                        .startObject("host")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("elapsed")
                            .field("type", "long")
                        .endObject()
                        .startObject("concurrent")
                            .field("type", "long")
                        .endObject()
                        .startObject("input")
                            .field("type", "long")
                        .endObject()
                        .startObject("output")
                            .field("type", "long")
                        .endObject()
                        .startObject("successCount")
                            .field("type", "integer")
                        .endObject()
                        .startObject("failureCount")
                            .field("type", "integer")
                        .endObject()
                        .startObject("remoteAddress")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("remoteType")
                            .field("type", "keyword")
                        .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }


    public void batchInsert(String application,List<Statistics> statisticsList){
        boolean result = elasticTemplate.indexExists(STATISTICS_INDEX_PREFIX + application);
        if(!result){
            elasticTemplate.createIndex(STATISTICS_INDEX_PREFIX + application, application, getMapping());
        }

        List<HashMap<String, Object>> listData = new ArrayList<>();
        statisticsList.stream().forEach(item -> listData.add(createObjToJson(item)));
        elasticTemplate.bulkSave(STATISTICS_INDEX_PREFIX + application, application, listData);
    }

    public List<Statistics> queryStatisticsForMethod(String application, String serviceInterface, String method, long startTime, long endTime){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("serviceInterface", serviceInterface));
        boolQueryBuilder.must(QueryBuilders.termQuery("method", method));
        boolQueryBuilder.must(QueryBuilders.rangeQuery("timestamp").format("strict_date_optional_time||epoch_millis").gte(startTime).lte(endTime));

        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(STATISTICS_INDEX_PREFIX + application);
        srb.setTypes(application);
        srb.setQuery(boolQueryBuilder);
        srb.setFrom(0).setSize(PAGE_SIZE);
        SearchResponse sr = srb.execute().actionGet();
        //获取结果集
        SearchHit[] hits = sr.getHits().getHits();
        List<Statistics>  statisticses = new ArrayList<>();
        Arrays.stream(hits).forEach(item -> processResultHitsItem(statisticses, item, application));
        return statisticses;
    }

    /**
     * 查询接口下的方法
     * @param application
     * @param serviceInterface
     * @return
     */
    public List<TempMethodOveride> findMethodForService(String application, String serviceInterface){
        List<TempMethodOveride>  tempMethodOverides = new ArrayList<>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("serviceInterface", serviceInterface));
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(STATISTICS_INDEX_PREFIX + application);
        srb.setTypes(application);
        srb.setQuery(boolQueryBuilder).setSize(0);
        srb.addAggregation(AggregationBuilders.terms("m").field("method").size(0).order(Terms.Order.count(false)));
        SearchResponse sr = srb.execute().actionGet();
        Terms terms = sr.getAggregations().get("m");
        List<Terms.Bucket> buckets = terms.getBuckets();
        buckets.forEach(item -> {
            TempMethodOveride tempMethodOveride = new TempMethodOveride();
            tempMethodOveride.setM(item.getKeyAsString());
            tempMethodOveride.setTotal(Integer.valueOf(String.valueOf(item.getDocCount())));
            tempMethodOverides.add(tempMethodOveride);
        });

        return  tempMethodOverides;
    }


    /**
     *  查询应用接口区间倒排数据
     * @param column
     * @param application
     * @param serviceInterface
     * @param method
     * @param startTime
     * @param endTime
     */
    public Statistics findMethodMaxItemByService(String column,String application, String serviceInterface, String method, long startTime, long endTime){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("serviceInterface", serviceInterface));
        boolQueryBuilder.must(QueryBuilders.termQuery("method", method));
        boolQueryBuilder.must(QueryBuilders.rangeQuery("timestamp").format("strict_date_optional_time||epoch_millis").gte(startTime).lte(endTime));
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(STATISTICS_INDEX_PREFIX + application);
        srb.setTypes(application);
        srb.setQuery(boolQueryBuilder);
        srb.setFrom(0).setSize(1);
        srb.addSort(column, SortOrder.DESC);

        SearchResponse sr = srb.execute().actionGet();
        //获取结果集
        SearchHit[] hits = sr.getHits().getHits();
        List<Statistics> statisticses = new ArrayList<>();
        Arrays.stream(hits).forEach(source -> processResultHitsItem(statisticses, source, application));
        return statisticses.get(0);
    }



    /**
     *  查询应用概要信息
     * @param application
     * @param startTime
     * @param item
     * @param endTime
     * @return
     */
    public List<Statistics> findApplicationOverview(String application,String item,long startTime, long endTime){
        List<Statistics> statisticsList = new ArrayList<>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.rangeQuery("timestamp").format("strict_date_optional_time||epoch_millis").gte(startTime).lte(endTime));
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(STATISTICS_INDEX_PREFIX + application);
        srb.setTypes(application);
        srb.setQuery(boolQueryBuilder);
        srb.setFrom(0).setSize(200);
        srb.addSort(item, SortOrder.DESC);

        SearchResponse sr = srb.execute().actionGet();
        //获取结果集
        SearchHit[] hits = sr.getHits().getHits();
        List<Statistics>  statisticses = new ArrayList<>();
        Arrays.stream(hits).forEach(source -> processResultHitsItem(statisticses, source, application));
        return statisticses;
    }


    /**
     *  查询接口概要信息
     * @param application
     * @param startTime
     * @param item
     * @param service
     * @param endTime
     * @return
     */
    public List<Statistics> findServiceOverview(String application,String service,String item,long startTime, long endTime){
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must(QueryBuilders.termQuery("serviceInterface", service));
        boolQueryBuilder.must(QueryBuilders.rangeQuery("timestamp").format("strict_date_optional_time||epoch_millis").gte(startTime).lte(endTime));
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(STATISTICS_INDEX_PREFIX + application);
        srb.setTypes(application);
        srb.setQuery(boolQueryBuilder);
        srb.setFrom(0).setSize(200);
        srb.addSort(item, SortOrder.DESC);

        SearchResponse sr = srb.execute().actionGet();
        //获取结果集
        SearchHit[] hits = sr.getHits().getHits();
        List<Statistics> statisticses = new ArrayList<>();
        Arrays.stream(hits).forEach(source -> processResultHitsItem(statisticses, source, application));
        return statisticses;
    }


    /**
     * 查询服务接口
     * @param application
     * @return
     */
    public List<ServiceInfo> findServiceByApp(String application){
        List<ServiceInfo> serviceInfos = new ArrayList<>();
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(STATISTICS_INDEX_PREFIX + application);
        srb.setTypes(application);
        srb.setSize(0);
        srb.addAggregation(AggregationBuilders.terms("name").field("serviceInterface").size(0).subAggregation(AggregationBuilders.terms("remoteType").field("remoteType")));
        SearchResponse sr = srb.execute().actionGet();
        Terms termsInterface = sr.getAggregations().get("name");
        List<Terms.Bucket> bucketsInterface = termsInterface.getBuckets();

        bucketsInterface.stream().forEach(bucket -> {
            Terms termsType = bucket.getAggregations().get("remoteType");
            termsType.getBuckets().stream().forEach(item -> {
                ServiceInfo serviceInfo = new ServiceInfo();
                serviceInfo.setName(bucket.getKeyAsString());
                serviceInfo.setRemoteType(item.getKeyAsString());
                serviceInfos.add(serviceInfo);
            });
        });
        return  serviceInfos;
    }



    /**
     * 查询时间区间内的item最大值
     * @param application
     * @param service
     * @param startTime
     * @param endTime
     * @return
     */
    public Statistics queryMaxItemByService(String application,String service,String item,long startTime,long endTime){
        List<Statistics> statisticsList = new ArrayList<>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        if(service != null){
            boolQueryBuilder.must(QueryBuilders.termQuery("serviceInterface", service));
        }
        boolQueryBuilder.must(QueryBuilders.rangeQuery("timestamp").format("strict_date_optional_time||epoch_millis").gte(startTime).lte(endTime));
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(STATISTICS_INDEX_PREFIX + application);
        srb.setTypes(application);
        srb.setQuery(boolQueryBuilder);
        srb.addSort(item, SortOrder.DESC);

        SearchResponse sr = srb.execute().actionGet();
        SearchHit[] hits = sr.getHits().getHits();
        if (hits.length > 0){
            processResultHitsItem(statisticsList, hits[0], application);
        }

        return statisticsList.get(0);
    }
}
