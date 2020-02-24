package com.gin.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author gin
 * @date 2020/2/24 19:21
 */
public class MyUDF extends UDF {

    public Text evaluate(final Text str){
        //将输入字符串的第一个字母大写
        if(str != null && !"".equals(str.toString())){
            String tmpStr = str.toString();
            String retStr = tmpStr.substring(0, 1).toUpperCase() + tmpStr.substring(1);
            return new Text(retStr);
        }
        return new Text("");
    }
}
