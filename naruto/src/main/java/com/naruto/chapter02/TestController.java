package com.naruto.chapter02;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

@Controller
public class TestController {

    @RequestMapping("/ok")
    @ResponseBody
    public String testOk(){
        return "ok";
    }

    @RequestMapping("/param")
    @ResponseBody
    public String testOk(String param){
        return param;
    }

    @RequestMapping(value={"obj"})
    public Map entryList(@RequestBody Map map) {
        return map;
    }

    @RequestMapping("/{id}")
    @ResponseBody
    public String getId(@PathVariable("id")Integer id){
        return String.valueOf(id);
    }



}
