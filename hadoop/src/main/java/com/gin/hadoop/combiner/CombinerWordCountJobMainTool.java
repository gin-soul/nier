package com.gin.hadoop.combiner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author gin
 * @date 2020/2/18 20:32
 */
public class CombinerWordCountJobMainTool extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        //第一个参数直接获取即可,在调用main方法时, new Configuration() 传递给了当前类的 conf 变量
        //故可以直接super通过getConf()方法获取成员变量值
        //第二个参数是任务名称,可以随便定义
        //Job job = Job.getInstance(super.getConf(), "mapReduceWordCount");
        Job job = Job.getInstance(super.getConf(), CombinerWordCountJobMainTool.class.getSimpleName());

        //打包到集群上面运行时候，必须要添加以下配置，指定程序的main函数
        //否则会找不到对应的 Mapper 和 Reduce 类
        job.setJarByClass(CombinerWordCountJobMainTool.class);

        // 第一步：读取输入文件解析成key，value对(设置使用哪个类读取文件)
        job.setInputFormatClass(TextInputFormat.class);
        //设置读取路径(设置目录名,不需要设置文件名)
        TextInputFormat.addInputPath(job, new Path("hdfs://node01:8020/word_count"));

        // 第二步：设置我们的mapper类(使用哪个Mapper来执行第一次的 k1,v1 -> k2,v2 转换)
        job.setMapperClass(CombinerWordCountMapper.class);
        // 设置我们map阶段完成之后的输出类型(k2,v2的类型)
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //shuffle(分区,排序,规约,分组)
        // 第三步 分区 默认1个分区

        // 第四步 排序 默认
        // 第五步 规约 提前在maptask阶段进行reduce,降低网络传输
        job.setCombinerClass(MyCombinerReducer.class);

        // 第六步 分组 默认
        // 先使用默认方式处理

        // 第七步：设置我们的reduce类(使用哪个reduce类来执行第二次的 新k2,v2 -> k3,v3 转换)
        job.setReducerClass(CombinerWordCountReducer.class);
        // 设置我们reduce阶段完成之后的输出类型(k3,v3的类型)
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // 第八步：设置输出类以及输出路径(结果文件输出到哪)
        job.setOutputFormatClass(TextOutputFormat.class);
        //注意: wordCountOut 该路径不允许存在,程序会自动创建
        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/word_count_out"));
        boolean outFlag = job.waitForCompletion(true);
        System.out.println("mainJob wordCount outFlag" + outFlag);
        return outFlag ? 0 : 1;
    }


    //程序main函数的入口类
    //测试方式: 需要打包成jar包 <packaging>jar</packaging>
    //上传到集群服务器后,通过 hadoop 命令启动
    //hadoop jar hadoop-0.0.1-SNAPSHOT.jar com.gin.hadoop.combiner.CombinerWordCountJobMainTool
    public static void main(String[] args) throws Exception {
        //hadoop相关的一些配置可以在这里设置
        Configuration configuration = new Configuration();
        Tool tool = new CombinerWordCountJobMainTool();
        //第一个参数为hadoop相关配置类(会传递给当前类的 conf 变量)
        //第二个为实现了 Tool 接口的类名,所以可以使用这个类本身
        //第三个为主程序的执行参数,可以直接将main方法参数提供给run方法
        int run = ToolRunner.run(configuration, tool, args);
        //系统输出执行结果,0表示系统正常执行完成,其他表示异常
        System.exit(run);
    }

}
