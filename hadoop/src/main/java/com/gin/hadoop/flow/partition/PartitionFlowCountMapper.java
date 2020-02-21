package com.gin.hadoop.flow.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionFlowCountMapper extends Mapper<LongWritable, Text, Text, PartitionFlowCountVO> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1:拆分手机号,手机号作为k2,
        String[] split = value.toString().split("\t");
        //第一列为时间戳, 第二例为手机号
        String phoneNum = split[1];
        //2:获取四个流量字段
        PartitionFlowCountVO combinerFlowCountVO = new PartitionFlowCountVO();
        combinerFlowCountVO.setUpFlow(Integer.parseInt(split[6]));
        combinerFlowCountVO.setDownFlow(Integer.parseInt(split[7]));
        combinerFlowCountVO.setUpCountFlow(Integer.parseInt(split[8]));
        combinerFlowCountVO.setDownCountFlow(Integer.parseInt(split[9]));

        //3:将k2和v2写入上下文中
        context.write(new Text(phoneNum), combinerFlowCountVO);
    }
}
