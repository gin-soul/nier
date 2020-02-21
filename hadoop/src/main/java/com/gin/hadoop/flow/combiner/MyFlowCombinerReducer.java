package com.gin.hadoop.flow.combiner;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gin
 * @date 2020/2/18 17:16
 */
public class MyFlowCombinerReducer extends Reducer<Text, LongWritable, Text, LongWritable> {



    /**
     * 自定义combiner 逻辑 其实和reduce需要做的事情一样,只是位置不同
     *
     * combiner 是 MapReduce 程序中 Mapper 和 Reducer 之外的一种组件
     * 	combiner 组件的父类就是Reducer(combiner 和 reducer 的区别仅仅在于运行的位置,逻辑一致)
     * 	Combiner 是在每一个 maptask 所在的节点运行
     * Reducer 是接收全局所有 Mapper 的输出结果
     * combiner 的意义就是对每一个 maptask 的输出进行局部汇总，以减小网络传输量
     *
     * 所有的key都是单词，所有的values都是单词出现的次数
     * @param key 单词
     * @param values 单词出现的次数集合
     * @param context 上下文对象(连接map和reduce的桥梁对象)
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //将k2,v2 转换为 k3,v3
        long count = 0;
        //遍历集合,将集合中的值相加
        for (LongWritable value : values) {
            count += value.get();
        }
        //然后将统计的值赋给k3
        context.write(key,new LongWritable(count));
    }
}