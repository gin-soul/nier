package com.gin.hdfs.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;

/**
 * @author gin
 * @date 2020/2/14 12:23
 */
public class HdfsFileInsertTest {

    private static FileSystem fileSystem;

    @BeforeAll
    public static void getFileSystem() throws  Exception{
        //先获取主节点连接
        fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
    }

    @Test
    public void mkdirs() throws  Exception{
        //创建文件夹
        boolean mkdirs = fileSystem.mkdirs(new Path("/hello/mydir/test"));
        System.out.println("mkdirs flag=" + mkdirs);
    }

    @Test
    public void mkFile() throws  Exception{
        //创建文件夹
        String path = "/hello/mydir/test/a.txt";
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path(path));
        fsDataOutputStream.close();
        System.out.println("mkFile path=" + path);
    }

    @AfterAll
    public static void closeFileSystem() throws  Exception{
        //释放资源
        fileSystem.close();
    }

}
