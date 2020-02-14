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
public class HdfsFileGetTest {

    private static FileSystem fileSystem;

    @BeforeAll
    public static void getFileSystem() throws  Exception{
        //先获取主节点连接
        fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
    }

    @Test
    public void getFile() throws  Exception{
        System.out.println("fileSystem:" + fileSystem);
        //获取RemoteIterator 得到所有的文件或者文件夹
        // 第一个参数指定遍历的路径(起始路径, / 表示从根路径开始)
        // 第二个参数表示是否要递归遍历(true表示除了获取当前目录信息,还需要获取子目录信息)
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            //获取每一个文件详细信息
            LocatedFileStatus fileStatus = locatedFileStatusRemoteIterator.next();
            //获取每一个文件的存储路径信息
            String fileName = fileStatus.getPath().getName();
            System.out.println(fileName + " absolute path= " + fileStatus.getPath());
            //获取文件的block存储信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            //打印每个文件的block数(大文件会切割成几块,一般小文件就是1块)
            System.out.println(fileName + " block count= " + blockLocations.length);
            //打印每个block副本的存储位置
            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();
                System.out.println(fileName + " hosts count= " + hosts.length);
                for (String host : hosts) {
                    System.out.println(fileName + " host= " + host);
                }
            }

        }

    }

    @AfterAll
    public static void closeFileSystem() throws  Exception{
        //释放资源
        fileSystem.close();
    }

}
