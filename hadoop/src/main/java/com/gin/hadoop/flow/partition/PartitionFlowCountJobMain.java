package com.gin.hadoop.flow.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionFlowCountJobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //创建一个任务对象
        Job job = Job.getInstance(super.getConf(), PartitionFlowCountJobMain.class.getSimpleName());

        //打包放在集群运行时，需要做一个配置
        job.setJarByClass(PartitionFlowCountJobMain.class);
        //第一步:设置读取文件的类: K1 和V1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/flow_count"));

        //第二步：设置Mapper类
        job.setMapperClass(PartitionFlowCountMapper.class);
        //设置Map阶段的输出类型: k2 和V2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionFlowCountVO.class);

        //shuffled (分区，排序，规约，分组)
        //第三 设置分区类
        job.setPartitionerClass(FlowCountPartitionByPhone.class);

        // 第四，五，六步采用默认方式


        //第七步 ：设置文的Reducer类
        job.setReducerClass(PartitionFlowCountReducer.class);
        //设置Reduce阶段的输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionFlowCountVO.class);

        //设置Reduce的个数(目前有四个分区: 0,1,2,3)
        job.setNumReduceTasks(4);

        //第八步:设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置输出的路径
        TextOutputFormat.setOutputPath(job, new Path("hdfs://node01:8020/flow_count_partition_out"));


        boolean outFlag = job.waitForCompletion(true);
        System.out.println("mainJob wordCount outFlag" + outFlag);
        return outFlag ? 0 : 1;

    }

    //程序main函数的入口类
    //测试方式: 需要打包成jar包 <packaging>jar</packaging>
    //上传到集群服务器后,通过 hadoop 命令启动
    //hadoop jar hadoop-0.0.1-SNAPSHOT.jar com.gin.hadoop.flow.partition.PartitionFlowCountJobMain
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //启动一个任务
        int run = ToolRunner.run(configuration, new PartitionFlowCountJobMain(), args);
        //系统输出执行结果,0表示系统正常执行完成,其他表示异常
        System.exit(run);
    }

}
