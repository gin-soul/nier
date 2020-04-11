package com.naruto.controller;

import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * @author gin
 * @date 2020/02/29
 *
 * 请求路径: http://主机名:端口号/context-path(如果配置了)/Controller的URI/方法的URI
 * http://localhost:8080/api/ts/login
 */
@Controller
@RequestMapping("ts")
public class TestController {

    /**
     * 普通Controller,不是ResponseBody注解修饰的
     * String返回值,会找模板文件
     *
     * 需要引入依赖 spring-boot-starter-thymeleaf
     * @return login.html
     */
    @RequestMapping("/login")
    public String getLoginHtml(){
        return "login";
    }


    /**
     * @param map ModelMap 类似context上下文数据,在 thymeleaf 中可以获取对应属性值
     * @return html
     */
    @RequestMapping("/ok")
    public String testOk(ModelMap map){
        map.addAttribute("name", "Hello Gin JRebel");
        return "login";
    }

    @RequestMapping(value={"obj"})
    @ResponseBody
    public Map entryList(@RequestBody Map map) {
        return map;
    }

    @RequestMapping("/{id}")
    @ResponseBody
    public String getId(@PathVariable("id")Integer id){
        return String.valueOf(id);
    }

}
