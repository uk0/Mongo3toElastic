package com.mongo2es.inter;

import com.alibaba.fastjson.JSONObject;

/**
 * Created by zhangjianxin on 2018/3/19.
 * Github Breakeval13
 * blog firsh.me
 */
public interface watcherInterface {

    int insert(String inData, JSONObject dbTable);
    int delete(String inData, JSONObject dbTable);
    int updata(String inData, JSONObject dbTable);

}
