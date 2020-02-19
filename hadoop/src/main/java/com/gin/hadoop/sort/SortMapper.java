package com.gin.hadoop.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 *
 * 数据准备
 *
 * vim word_sort.txt

a 1
a 9
b 3
a 7
b 8
b 10
a 5

 * 上传至HDFS
 * hdfs dfs -mkdir /word_sort/
 * hdfs dfs -put word_sort.txt /word_sort/
 *
 *
 * k1,v1
0     ->     a 1
5     ->     a 9
9     ->     b 3
13    ->     a 7
 *
 * k2(对应v1值,即原文本数据),
 * v2(拆分开v1的值,两个数据通过逗号分隔为SortPairWritable对象的两个field值
 *
 <a, 1>     ->     a 1
 <a, 9>     ->     a 9
 <b, 3>     ->     b 3
 <a, 7>     ->     a 7
 *
 * k1, v1, k2, v2
 * 所以对应的泛型为 Long, String, SortPairWritable, String
 *     即:        LongWritable, Text, SortPairWritable, Text
 *
 * @author gin
 * @date 2020/2/19 16:42
 */
public class SortMapper extends Mapper<LongWritable, Text, SortPairWritable, Text> {

    /**
     *
     * @param key k1
     * @param value v1
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //自定义计数器,使用普通方式
        //第一个参数为计数器的分类的名字(可自定义)
        //第二个参数为计数器的名称(可自定义)
        Counter counter = context.getCounter("MS_CNT", "MAP_SORT_COUNTER");
        //表示每执行一次map方法,计数器就会+1,最终计数器的类型,名称,次数均会在执行结果中显示
        counter.increment(1L);

        //将 v1 转换为 k2
        String[] partSplit = value.toString().split(" ");
        SortPairWritable sortPairWritable = new SortPairWritable();
        sortPairWritable.setFirst(partSplit[0]);
        sortPairWritable.setSecond(Integer.parseInt(partSplit[1]));
        context.write(sortPairWritable, value);
    }

}
