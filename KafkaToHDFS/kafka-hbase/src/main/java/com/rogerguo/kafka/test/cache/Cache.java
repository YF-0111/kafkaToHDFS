package com.rogerguo.kafka.test.cache;

import java.util.*;

public class Cache {
    private static HashMap<String, List<Item>> streamMap = new HashMap<String, List<Item>>();
//    static int static_key = 0; // 表示当前的key
    public static void setStreamMap(HashMap<String, List<Item>> streamMap) {
        Cache.streamMap = streamMap;
    }

    public static HashMap<String, List<Item>> getStreamMap() {
        return streamMap;
    }

    public static void add_one_map(String v_id, long date, String longitude, String latitude) throws Exception {
//        int id = static_key % trajectory_num +1;
//        System.out.println("static_key:"+static_key);
//        System.out.println("streamMap.get(static_key):"+streamMap.get(id));
        Item item = new Item(v_id,date,longitude,latitude);
        List<Item> map_value_list = new ArrayList<>();
        if(getStreamMap().get(v_id) != null && !getStreamMap().get(v_id).isEmpty()){
            map_value_list.addAll(getStreamMap().get(v_id));
        }
        map_value_list.add(item);
//        System.out.println("new_static_key:"+id);
//        getStreamMap().put(id,map_value_list);
        HashMap<String, List<Item>> tmp = getStreamMap();
        tmp.put(v_id,map_value_list);
        setStreamMap(tmp);
//        static_key ++;
    }

    public static void clear_map() throws Exception {
        // static_key = 0;
        getStreamMap().clear();
    }
    public static void clear_map_list(String key) throws Exception {
        getStreamMap().remove(key);
    }
    public static void print_map() throws Exception {
        // 取出数据
        Set<String> keys = getStreamMap().keySet();		// 得到全部的key变成Set集合
        Iterator<String> iter_k = keys.iterator();		// 实例化Iterator对象

        while (iter_k.hasNext()){

            String str_k = iter_k.next();	// 取出key
            System.out.println("k:"+str_k);	// 输出key
            System.out.println("v:"+getStreamMap().get(str_k));	// 输出value
        }

    }
}


