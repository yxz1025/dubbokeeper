package com.dubboclub.dk.storage.elasticsearch.dao;

import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Created by yuanxiaozhong on 2018/5/7.
 */
public abstract class AbstractEsDao {

    public abstract XContentBuilder getMapping();
}
