package com.gin.hadoop.flow.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitionFlowCountReducer extends Reducer<Text, PartitionFlowCountVO, Text, PartitionFlowCountVO> {
    @Override
    protected void reduce(Text key, Iterable<PartitionFlowCountVO> values, Context context) throws IOException, InterruptedException {
        //封装新的FlowBean
        PartitionFlowCountVO partitionFlowCountVO = new PartitionFlowCountVO();
        Integer upFlow = 0;
        Integer downFlow = 0;
        Integer upCountFlow = 0;
        Integer downCountFlow = 0;
        //将每个手机号(新的k2)的新的v2集合遍历,所有值累加
        //例:    13560439658  <18	15	1116	954 ,15	9	918	4938>
        //合并为 13560439658  <33	24	2034	5892>
        for (PartitionFlowCountVO value : values) {
            upFlow += value.getUpFlow();
            downFlow += value.getDownFlow();
            upCountFlow += value.getUpCountFlow();
            downCountFlow += value.getDownCountFlow();
        }
        partitionFlowCountVO.setUpFlow(upFlow);
        partitionFlowCountVO.setDownFlow(downFlow);
        partitionFlowCountVO.setUpCountFlow(upCountFlow);
        partitionFlowCountVO.setDownCountFlow(downCountFlow);
        //将K3和V3写入上下文中
        context.write(key, partitionFlowCountVO);
    }
}
