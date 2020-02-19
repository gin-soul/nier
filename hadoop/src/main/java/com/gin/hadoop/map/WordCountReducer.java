package com.gin.hadoop.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author gin
 * @date 2020/2/18 17:16
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

    /**
     * 自定义reduce逻辑
     * 完成 新的k2,新的v2 向 k3,v3 的转换

     hello -> 1  来源第一行数据中的hello (hello,world,hadoop)
     hello -> 1  来源第二行数据中的hello (hive,sqoop,flume,hello)

     经过hadoop的shuffle(分区,排序,规约,分组)阶段后
     会由hadoop形成 新的k2,v2
     hello -> <1,1>

     reduce的操作是将 新的k2,v2 转换成业务需要的(目前是单词统计业务) k3,v3
     hello -> <1,1>

     转换为

     hello 2

     这里的泛型也是 新的k2, 新的v2, k3, v3 所对应的泛型
     extends Reducer<Text, LongWritable, Text, LongWritable>

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