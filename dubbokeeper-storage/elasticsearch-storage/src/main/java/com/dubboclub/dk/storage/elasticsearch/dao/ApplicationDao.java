package com.dubboclub.dk.storage.elasticsearch.dao;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.dubboclub.dk.storage.elasticsearch.ElasticTemplate;
import com.dubboclub.dk.storage.model.ApplicationInfo;

import java.io.IOException;
import java.util.*;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by yuanxiaozhong on 2018/5/6.
 */
public class ApplicationDao extends AbstractEsDao {

    private static final Integer PAGE_SIZE = 1000;
    private static final String INDEX_APPLICATION = "index_application";
    private static final String TYPE = "application";

    private ElasticTemplate elasticTemplate;

    public ApplicationDao(ElasticTemplate elasticTemplate) {
        this.elasticTemplate = elasticTemplate;
    }

    public void updateAppType(String application, int type){
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(INDEX_APPLICATION);
        srb.setTypes(TYPE);
        srb.setQuery(QueryBuilders.termQuery("applicationName",application));
        SearchResponse sr = srb.execute().actionGet();

        //获取结果集
        SearchHit[] hits = sr.getHits().getHits();
        if (hits.length > 0){
            String id = hits[0].getId();
            try {
                //更新
                elasticTemplate.getClient().prepareUpdate(INDEX_APPLICATION, TYPE, id)
                        .setDoc(jsonBuilder().startObject().field("applicationType", type).endObject()).get();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public List<ApplicationInfo> findAll(){
        List<ApplicationInfo> list = new ArrayList<ApplicationInfo>();
        SearchRequestBuilder srb = elasticTemplate.getClient().prepareSearch(INDEX_APPLICATION);
        srb.setTypes(TYPE);
        srb.setFrom(0).setSize(PAGE_SIZE);
        SearchResponse sr = srb.execute().actionGet();
        //获取结果集
        SearchHit[] hits = sr.getHits().getHits();

        Arrays.stream(hits).forEach(item -> processResultHitsItem(list, item));
        return list;
    }

    private void processResultHitsItem(final List<ApplicationInfo> list, SearchHit item) {
        ApplicationInfo info = new ApplicationInfo();
        info.setApplicationName(String.valueOf(item.getSource().get("applicationName")));
        info.setApplicationType(Integer.valueOf(String.valueOf(item.getSource().get("applicationType"))));
        list.add(info);
    }

    public void addApplication(ApplicationInfo applicationInfo){
        //插入数据
        HashMap<String, Object> map = new HashMap<String, Object>();
        map.put("applicationName", applicationInfo.getApplicationName());
        map.put("applicationType", applicationInfo.getApplicationType());
        elasticTemplate.save(INDEX_APPLICATION, TYPE, map);
    }

    public void createApplicationMapping(){
        boolean result = elasticTemplate.indexExists(INDEX_APPLICATION);
        if(!result){
            elasticTemplate.createIndex(INDEX_APPLICATION, TYPE, getMapping());
        }
    }

    @Override
    public XContentBuilder getMapping() {
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("applicationName")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("applicationType")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return mapping;
    }
}
