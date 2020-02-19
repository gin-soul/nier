package com.gin.hadoop.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * 数据准备
 *
 * vim word_count.txt

hello,world,hadoop
hive,sqoop,flume,hello
kitty,tom,jerry,world
hadoop

 假设换行符偏移量是只 +1
 0 -> hello,world,hadoop
 19 -> hive,sqoop,flume,hello
 (18个字符 + 1个换行符)
 42 -> kitty,tom,jerry,world
 (19 + 22字符 + 1个换行符)
 64 -> hadoop
 (42 + 21个字符 + 1个换行符)


 * 上传至HDFS
 * hdfs dfs -mkdir /word_count/
 * hdfs dfs -put word_count.txt /word_count/
 *

 可以将第一次的: key1(行偏移量) -> value1(第一行数据)
 转换成         key2(每个单词( -> value2(固定值1)
 通过Mapper接口的mapper方法完成

 例:将 0 -> hello,world,hadoop 转换为
 hello -> 1
 world -> 1
 hadoop -> 1

 Mapper的泛型:
 Long String (key1, value1的泛型) String Long (key2, value2的泛型)
 对应Hadoop自己定义的泛型:(hadoop设计时认为java类型序列化方式过于臃肿,故自行定义类型及序列化方式
 LongWritable Text(key1, value1的泛型) Text LongWritable(key2, value2的泛型)

 *
 * @author gin
 * @date 2020/2/18 17:16
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    /**
     * map方法是将 k1和v1 转换成 k2和v2
     * @param key 行偏移量
     * @param value 第一行数据
     * @param context 上下文对象(连接map和reduce的桥梁对象)
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //避免for循环中频繁创建新对象
        Text text = new Text();
        LongWritable longWritable = new LongWritable();

        String line = value.toString();
        //hello,world,hadoop 做个字符串切割
        String[] split = line.split(",");
        for (String word : split) {
            //然后再给每个切开的字符串(k2) 拼接上对应的 v2的值
            //即完成对k1对应的v1 至 k2,v2的转换了(write 相当于向后传递数据)
            text.set(word);
            longWritable.set(1L);
            context.write(text, longWritable);
        }
    }

}
