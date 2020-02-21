package com.gin.hadoop.flow.combiner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowCountVO, Text, FlowCountVO> {
    @Override
    protected void reduce(Text key, Iterable<FlowCountVO> values, Context context) throws IOException, InterruptedException {
        //封装新的FlowBean
        FlowCountVO flowCountVO = new FlowCountVO();
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        //将每个手机号(新的k2)的新的v2集合遍历,所有值累加
        //例:    13560439658  <18	15	1116	954 ,15	9	918	4938>
        //合并为 13560439658  <33	24	2034	5892>
        for (FlowCountVO value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        flowCountVO.setUpFlow(upFlow);
        flowCountVO.setDownFlow(downFlow);
        flowCountVO.setUpCountFlow(upCountFlow);
        flowCountVO.setDownCountFlow(downCountFlow);
        //将K3和V3写入上下文中
        context.write(key, flowCountVO);
    }
}
