package com.gin.hadoop.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * k2(对应v1值,即原文本数据),
 * v2(拆分开v1的值,两个数据通过逗号分隔为SortPairWritable对象的两个field值
 *
 <a, 1>     ->     a 1
 <a, 9>     ->     a 9
 <b, 3>     ->     b 3
 <a, 7>     ->     a 7
 *
 * 对应的 k3(SortPairWritable对象)
 *       v3 NullWritable
 *
 <a, 1>     ->     NullWritable
 <a, 9>     ->     NullWritable
 <b, 3>     ->     NullWritable
 <a, 7>     ->     NullWritable
 *
 *
 * @author gin
 * @date 2020/2/19 16:42
 */
public class SortReducer extends Reducer<SortPairWritable, Text, SortPairWritable, NullWritable> {

    /**
     *
     * @param key 新的 k2
     * @param values 新的 v2
     * @param context 上下文
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(SortPairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //注意,当对象如果相同,  <a, 1> 那么hadoop会将他们合并为
        //  <a, 1> -> < a 1, a 1>
        //所以为了将重复对象,重复几次则输出几次,那么就需要对合并后的values进行遍历
        for (Text value : values) {
            //实际上k3就是原来的k2, 而v3为空
            context.write(key, NullWritable.get());
        }
    }

}
