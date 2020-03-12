package com.naruto.test;

import com.naruto.file.FileUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class AerospikeTest {

    public static void main(String[] args) {
        testBigData(100000, 1000);
    }

    public static boolean testBigData(int line, int splitRow){
        boolean flag = false;
        try {
            String remotePath = "/data01/sftp/remote/asTest.sh";
            //先删除历史数据
            FileUtils.deleteFile(remotePath);
            long beginTime = System.currentTimeMillis();

            //todo
            appendFile("#!/bin/bash\n" +
                    "cur_date1=`date +%Y-%m-%d` \n" +
                    "aql <<'EOF' 2>&1 > /dev/null \n", new File(remotePath));

            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < line; i++) {
                long id = 360702199210311600L + i;
                stringBuilder.append("insert into ns1.bigDataIndex (PK, indexInfo) values ('").append(id).append("', '").append(id).append("')\";").append("\r\n");
                if (i % splitRow == 0){
                    File file = new File(remotePath);
                    appendFile(stringBuilder.toString(), file);
                    //清空文本
                    stringBuilder.delete(0, stringBuilder.length());
                }
            }
            // line - 1 对应i的最大值
            if ((line - 1) % splitRow != 0) {
                File file = new File(remotePath);
                appendFile(stringBuilder.toString(), file);
                //清空文本
                stringBuilder.delete(0, stringBuilder.length());
            }

            //todo
            appendFile("EOF\r\n", new File(remotePath));
            appendFile("cur_date2=`date +%Y-%m-%d` \n" +
                    "echo ${cur_date1}__${cur_date2} ", new File(remotePath));

            long endTime = System.currentTimeMillis();
            System.out.println("create file costs time=" + (endTime - beginTime)/1000 + "(s)");
            flag = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return flag;
    }

    private static void appendFile(String text, File file) {
        try (
                //打开一个写文件器，构造函数中的第二个参数true表示以追加形式写文件
                FileWriter writer = new FileWriter(file, true)
        ){
            writer.write(text);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
