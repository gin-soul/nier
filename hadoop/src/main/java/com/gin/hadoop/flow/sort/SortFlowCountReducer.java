package com.gin.hadoop.flow.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortFlowCountReducer extends Reducer<SortFlowCountVO, Text, Text, SortFlowCountVO> {
    @Override
    protected void reduce(SortFlowCountVO key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            //为了将手机号放在前面,所有调换了顺序
            context.write(value, key);
        }
    }
}
