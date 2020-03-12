package com.nier.redis.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author gin
 */
@Controller
@RequestMapping("/redis")
public class RedisTestController {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @RequestMapping("/page")
    public String showDetail(Model model){
        //跳转到指定页面 /pages/itemDetail.jsp
        return "itemDetail";
    }


    @RequestMapping(value = "index/redis", method = RequestMethod.GET)
    @ResponseBody
    //注入对象 @RequestBody xxxVO xxx
    public String testRedis() {
        Long beginTime = System.currentTimeMillis();
        try {
            System.out.println("--------indexSync-------- allocateIndexTask begin time=" + new Date() + " ----------------");

            List<String> ids = new LinkedList<>();
            for (int i = 0; i < 5; i++) {
                ids.add("" + (360702199210311610L + i));
            }
            batchCacheMarketInfo(ids);

            System.out.println("--------indexSync-------- allocateIndexTask end,time=" + (System.currentTimeMillis() - beginTime) + " ms");
            return "redis success";
        } catch (Exception e) {
            return "redis error: " + e;
        }
    }

    private void batchCacheMarketInfo(List<String> dataList) {
        //使用pipeline方式
        redisTemplate.executePipelined(new RedisCallback<List<Object>>() {
            @Override
            public List<Object> doInRedis(RedisConnection connection) throws DataAccessException {
                for (String key : dataList) {
                    byte[] rawKey = key.getBytes();
                    connection.set(rawKey, rawKey);
                }
                return null;
            }
        });

    }

}
