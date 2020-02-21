package com.gin.hadoop.flow.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortFlowCountMapper extends Mapper<LongWritable, Text, SortFlowCountVO, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //因目前需要对上行流量统计结果进行排序
        // 一般排序字段需要作为 k2
        //1:拆分手机号,手机号作为v2,
        String[] split = value.toString().split("\t");
        //第一列为时间戳, 第二例为手机号
        String phoneNum = split[0];
        //2:获取四个流量字段
        SortFlowCountVO sortFlowCountVO = new SortFlowCountVO();
        sortFlowCountVO.setUpFlow(Integer.parseInt(split[1]));
        sortFlowCountVO.setDownFlow(Integer.parseInt(split[2]));
        sortFlowCountVO.setUpCountFlow(Integer.parseInt(split[3]));
        sortFlowCountVO.setDownCountFlow(Integer.parseInt(split[4]));

        //3:将k2和v2写入上下文中
        context.write(sortFlowCountVO, new Text(phoneNum));
    }
}
