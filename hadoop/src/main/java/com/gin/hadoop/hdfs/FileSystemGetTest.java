package com.gin.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import java.io.IOException;
import java.net.URI;

/**
 * @author gin
 * @date 2020/2/14 11:58
 *
 * Configuration
 *  该类的对象封转了客户端或者服务器的配置
 *  configuration.set("fs.defaultFS", "hdfs://node01:8020/", "hadoop"); 第三个参数表示使用哪个用户操作(权限控制)
 * FileSystem
 *  该类的对象是一个文件系统对象, 可以用该对象的一些方法来对文件进行操作, 通过 FileSystem 的静态方法 get 获得该对象
 *  FileSystem fs = FileSystem.get(conf)
 * - get` 方法从 `conf` 中的一个参数 `fs.defaultFS` 的配置值判断具体是什么类型的文件系统
 * - 如果我们的代码中没有指定 `fs.defaultFS`, 并且工程 ClassPath 下也没有给定相应的配置, `conf` 中的默认值就来自于 Hadoop 的 Jar 包中的 `core-default.xml`
 * - 默认值为 `file:/// `, 则获取的不是一个 DistributedFileSystem 的实例, 而是一个本地文件系统的客户端对象
 *
 * 注意导入的是 apache.hadoop 的配置,系统操作包
 * import org.apache.hadoop.conf.Configuration;
 * import org.apache.hadoop.fs.FileSystem;
 *
 */

public class FileSystemGetTest {

    /*public static void main(String[] args) {
        try {
            getFileSystem1();
            getFileSystem2();
            getFileSystem3();
            getFileSystem4();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    /**
     * 方式一:
     */
    public static void getFileSystem1() throws IOException {
        //获取Configuration对象
        Configuration configuration = new Configuration();
        //指定我们使用的文件系统类型:(使用 hostname 需要在本机的 C:\Windows\System32\drivers\etc\hosts 中配置
        //configuration.set("fs.defaultFS", "hdfs://192.168.25.110:8020/");
        //设置Configuration对象,设置的目的是来指定我们要操作的文件系统(不指定设置则会获取当前操作系统的文件系统)
        configuration.set("fs.defaultFS", "hdfs://node01:8020/");



        // 注意!!!!!!
        // 这里的 "hadoop" 表示以谁的身份去访问hdfs集群(主要用在权限控制中)
        //configuration.set("fs.defaultFS", "hdfs://node01:8020/", "hadoop");



        //获取我们指定的文件系统,获取FileSystem就相当于获取了主节点中所有的元数据信息
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println("fileSystem:" + fileSystem.toString());

    }

    /**
     * 方式二:
     */
    public static void getFileSystem2() throws Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        System.out.println("fileSystem:" + fileSystem);
    }

    /**
     * 方式三:
     */
    public static void getFileSystem3() throws Exception{
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println("fileSystem:" + fileSystem.toString());
    }

    /**
     * 方式四:
     */
    public static void getFileSystem4() throws Exception{
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020") ,new Configuration());
        System.out.println("fileSystem:" + fileSystem.toString());
    }

}
