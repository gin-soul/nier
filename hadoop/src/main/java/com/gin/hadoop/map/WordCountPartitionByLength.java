package com.gin.hadoop.map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 分区编号默认从 0 开始
 * 业务需求:
 * 将 单词长度>=5 的数据统计在一个分区(文件)中
 * 将 单词长度<5  的数据统计在一个分区(文件)中
 *
 * 分区1:
 *  hello  -> 1
 *  world  -> 1
 *  hadoop -> 1
 *
 * 分区2:
 *  java   -> 1
 *
 * 第一个泛型为: k2的泛型  String -> Text
 * 第二个泛型为: v2的泛型  Long   -> LongWritable
 *
 * @author gin
 * @date 2020/2/19 15:39
 */
public class WordCountPartitionByLength extends Partitioner<Text, LongWritable> {

    /**
     *
     * @param text 表示 k2
     * @param longWritable  表示 v2
     * @param i reduce(分区)的个数
     * @return 分区编号
     */
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        //如果单词长度 >= 5, 则进入第一个分区 -> 第一个reduceTask -> reduce的编号是0
        if (text.toString().length() >= 5) {
            return 0;
        } else {
            //如果单词长度 < 5, 则进入第二个分区 -> 第二个reduceTask -> reduce的编号是1
            return 1;
        }

    }

}
