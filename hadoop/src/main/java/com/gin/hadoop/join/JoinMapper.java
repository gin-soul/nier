package com.gin.hadoop.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 假如数据量巨大，两表的数据是以文件的形式存储在 HDFS 中, 需要用 MapReduce 程 序来实现以下 SQL 查询运算
 * select a.id,a.date,b.name,b.category_id,b.price from t_order a left join t_product b on a.pid = b.id
 *
 * vim orders.txt
1001,20150711,P0001,2
1002,20150712,P0001,3
1002,20150712,P0002,3
1003,20150713,P0003,3

 * vim products.txt
P0001,小米10,8888,4999
P0002,苹果11,9999,5599
 *
 * hdfs dfs -mkdir /join/
 * hdfs dfs -put orders.txt /join/
 * hdfs dfs -put products.txt /join/
 *
 订单数据表:
id      date        pid     amount
1001    20150711    P0001   2
1002    20150712    P0001   3
1002    20150712    P0002   3
1003    20150713    P0003   3

 商品信息表
id      pname       category_id         price
P0001   小米10        8888               4999
P0002   苹果11        9999               5500

 通过将关联的条件作为map输出的key，
 将两表满足join条件的数据并携带数据所来源的文件信息，
 发往同一个reduce task，在reduce中进行数据的串联

 即:
 k1,v1  ->  k2,v2

 订单数据表
 0,1001 20150710 P0001 2  ->  P0001, 1001 20150710 P0001 2
 商品信息表
 0,P0001 小米5 1000 2000   ->  P0001, P0001 小米5 1000 2000

 在reduce阶段就能获取到
 新的k2,新的v2: P0001, <1001 20150710 P0001 2, 1002 20150710 P0001 3>

 *
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //首先判断数据来自哪个文件
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        if("orders.txt".equals(fileName)){
            //获取pid 订单表
            String[] split = value.toString().split(",");
            context.write(new Text(split[2]), value);
        }else{
            //获取pid 商品表
            String[] split = value.toString().split(",");
            context.write(new Text(split[0]), value);
        }
    }
}
