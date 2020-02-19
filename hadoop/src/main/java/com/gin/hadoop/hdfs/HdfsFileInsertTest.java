package com.gin.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.net.URI;

/**
 * @author gin
 * @date 2020/2/14 12:23
 */
public class HdfsFileInsertTest {

    /*public static void main(String[] args) {
        try {
            getFileSystem();
            mkdirs();
            mkFile();
            closeFileSystem();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    private static FileSystem fileSystem;

    public static void getFileSystem() throws Exception{
        //先获取主节点连接
        fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
    }

    public static void mkdirs() throws Exception{
        //创建文件夹
        boolean mkdirs = fileSystem.mkdirs(new Path("/hello/mydir/test"));
        System.out.println("mkdirs flag=" + mkdirs);
    }

    public static void mkFile() throws Exception{
        //创建文件夹
        String path = "/hello/mydir/test/a.txt";
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(path));
        fsDataOutputStream.close();
        System.out.println("mkFile path=" + path);
    }

    public static void closeFileSystem() throws Exception{
        //释放资源
        fileSystem.close();
    }

}
