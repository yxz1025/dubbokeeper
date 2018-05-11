package com.dubboclub.dk.storage.elasticsearch;

import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.client.PreBuiltXPackTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

/**
 * Created by yuanxiaozhong on 2018/3/16.
 */
public class ElasticTemplate {

    private static final Logger logger = LoggerFactory.getLogger(ElasticTemplate.class);
    private Client client = null;

    private String ipAddress;

    private String cluster;

    private String username;

    private String password;

    public Client getClient() {
        return client;
    }

    public ElasticTemplate setClient(Client client) {
        this.client = client;
        return this;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public ElasticTemplate setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
        return this;
    }

    public String getCluster() {
        return cluster;
    }

    public ElasticTemplate setCluster(String cluster) {
        this.cluster = cluster;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ElasticTemplate setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ElasticTemplate setPassword(String password) {
        this.password = password;
        return this;
    }

    public void init(){
        Settings.Builder builder = Settings.builder();
        builder.put("cluster.name", cluster);
        builder.put("client.transport.sniff", false);

        builder.put("xpack.security.user", username + ":" + password);
        Settings settings = builder.build();
        try {
            client = new PreBuiltXPackTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(ipAddress), 9300));
        } catch (UnknownHostException e) {
            logger.error("UnknowHost host=[{}], cluster=[{}]", ipAddress, cluster);
        }

    }

    /**
     * 判断索引是否存在
     * @param index
     * @return
     */
    public boolean indexExists(String index){
        IndicesExistsRequest request = new IndicesExistsRequest(index);
        IndicesExistsResponse response = client.admin().indices().exists(request).actionGet();
        if (response.isExists()) {
            return true;
        }
        return false;
    }

    /**
     * 创建索引
     * @param index
     * @param type
     * @param mapping
     */
    public void createIndex(String index, String type, XContentBuilder mapping){
        //构建一个Index（索引）
        CreateIndexRequest request = new CreateIndexRequest(index);
        ActionFuture<CreateIndexResponse> responseActionFuture = client.admin().indices().create(request);
        if (responseActionFuture.actionGet().isAcknowledged()){
            createMapping(index, type, mapping);
        }
    }

    private void createMapping(String index, String type, XContentBuilder mapping){
        //创建mapping
        PutMappingRequest putMappingRequest = Requests.putMappingRequest(index).type(type).source(mapping);
        client.admin().indices().putMapping(putMappingRequest).actionGet();
    }

    /**
     * 保存单行数据
     * @param index
     * @param type
     * @param map
     * @return
     */
    public String save(String index, String type, HashMap<String, Object> map){
        IndexResponse response = client.prepareIndex(index, type)
                .setSource(map, XContentType.JSON)
                .get();
        return response.getId();
    }

    public void bulkSave(String index, String type, List<HashMap<String, Object>> list){
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        list.stream().forEach(item -> {
            IndexRequestBuilder lrb = client
                    .prepareIndex(index, type)
                    .setSource(item);
            bulkRequest.add(lrb);
        });
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()){
            System.out.println(bulkResponse.getItems().toString());
        }
    }

    public void close(){
        if (this.client != null){
            client.close();
        }
    }
}
