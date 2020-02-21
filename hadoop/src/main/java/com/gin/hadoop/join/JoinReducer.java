package com.gin.hadoop.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class JoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        List<String> orderInfoList = new LinkedList<>();
        String productInfo = "";
        for (Text value : values) {
            // p 开头的是商品信息
            if(value.toString().startsWith("P")) {
                productInfo = value.toString();
            } else {
                //将订单数据收集成list,后续将商品信息放在前面进行拼接
                orderInfoList.add(value.toString());
            }
        }

        for (String order : orderInfoList) {
            if ("".equals(productInfo)){
                context.write(key, new Text("NULL" + "\t" + order));
            } else {
                context.write(key, new Text(productInfo + "\t" + order));
            }
        }

    }
}
