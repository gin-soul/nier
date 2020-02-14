package com.gin.hdfs.file;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author gin
 * @date 2020/2/14 12:23
 */
public class HdfsFileDownloadUploadTest {

    private static FileSystem fileSystem;

    @BeforeAll
    public static void getFileSystem() throws  Exception{
        //先获取主节点连接
        fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
    }

    @Test
    public void download() throws  Exception{
        //获取输入流
        FSDataInputStream inputStream = fileSystem.open(new Path("/tmp/abc.txt"));
        //获取输出流
        FileOutputStream  outputStream = new FileOutputStream(new File("D:/data01/abc.txt"));
        //apache.commons.io 进行文件拷贝
        IOUtils.copy(inputStream,outputStream );
        //释放资源
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }

    @Test
    public void download2() throws  Exception{

        //第一个path是远程路径(下载来源); 第二个path是本地路径(下载到哪)
        fileSystem.copyToLocalFile(new Path("/tmp/abc.txt"), new Path("D:/data01/abc2.txt"));

    }

    @Test
    public void upload() throws  Exception{
        //第一个为本地的路径(上传来源);第二个为远程路径(上传目的地)
        fileSystem.copyFromLocalFile(new Path("file:///D:/data01/sftp/local/big.txt"),new Path("/tmp/hello.txt"));
    }

    @AfterAll
    public static void closeFileSystem() throws  Exception{
        //释放资源
        fileSystem.close();
    }

}
