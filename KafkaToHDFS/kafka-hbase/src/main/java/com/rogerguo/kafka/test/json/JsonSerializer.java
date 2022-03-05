package com.rogerguo.kafka.test.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @Description: 序列化类
 * @author: willzhao E-mail: zq2599@gmail.com
 * @date: 2020/5/2 15:05
 */
public class JsonSerializer<T> {

    private final ObjectMapper jsonMapper = new ObjectMapper();

    public String toJSONString(T r) {
        try {
            return jsonMapper.writeValueAsString(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + r, e);
        }
    }

    public byte[] toJSONBytes(T r) {
        try {
            return jsonMapper.writeValueAsBytes(r);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not serialize record: " + r, e);
        }
    }
}
