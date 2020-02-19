package com.gin.hadoop.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author gin
 * @date 2020/2/14 12:23
 */
public class HdfsFileDownloadUploadTest {

    private static FileSystem fileSystem;

    /*public static void main(String[] args) {
        try {
            getFileSystem();
            download();
            download2();
            upload();
            closeFileSystem();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    public static void getFileSystem() throws Exception{
        //先获取主节点连接
        fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
    }

    public static void download() throws Exception{
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

    public static void download2() throws Exception{

        //第一个path是远程路径(下载来源); 第二个path是本地路径(下载到哪)
        fileSystem.copyToLocalFile(new Path("/tmp/abc.txt"), new Path("D:/data01/abc2.txt"));

    }

    public static void upload() throws Exception{
        //第一个为本地的路径(上传来源);第二个为远程路径(上传目的地)
        fileSystem.copyFromLocalFile(new Path("file:///D:/data01/sftp/local/big.txt"),new Path("/tmp/hello.txt"));
    }

    public void mergeFile() throws Exception{
        //获取分布式文件系统(获取远程文件系统,并以 root 用户名作为权限校验用户)
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(),"root");
        //合并后的文件存放路径(包含文件名)
        FSDataOutputStream outputStream = fileSystem.create(new Path("/bigfile.txt"));
        //获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //通过本地文件系统获取文件列表，为一个集合(不包含文件名)
        FileStatus[] fileStatuses = local.listStatus(new Path("file:///D:/data01/sftp/local"));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            //将小文件合并至远程流中
            IOUtils.copy(inputStream,outputStream);
            //关闭当前小文件流
            IOUtils.closeQuietly(inputStream);
        }
        //资源释放
        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();
    }

    public static void closeFileSystem() throws Exception{
        //释放资源
        fileSystem.close();
    }

}
