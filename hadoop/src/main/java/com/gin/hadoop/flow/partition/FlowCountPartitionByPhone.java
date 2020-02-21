package com.gin.hadoop.flow.partition;

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
public class FlowCountPartitionByPhone extends Partitioner<Text, PartitionFlowCountVO> {

    /**
     *
     * @param text 表示 k2
     * @param partitionFlowCountVO  表示 v2
     * @param i reduce(分区)的个数
     * @return 分区编号
     */
    @Override
    public int getPartition(Text text, PartitionFlowCountVO partitionFlowCountVO, int i) {
        //如果手机号以135开头, 则进入第一个分区 -> 第一个reduceTask -> reduce的编号是0
        if (text.toString().startsWith("135")) {
            return 0;
        } else if(text.toString().startsWith("136")) {
            //如果手机号以136开头, 则进入第二个分区 -> 第二个reduceTask -> reduce的编号是1
            return 1;
        }  else if(text.toString().startsWith("137")) {
            //如果手机号以137开头, 则进入第三个分区 -> 第三个reduceTask -> reduce的编号是2
            return 2;
        }  else {
            //其他, 则进入第四个分区 -> 第四个reduceTask -> reduce的编号是3
            return 3;
        }

    }

}
