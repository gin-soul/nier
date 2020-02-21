package com.gin.hadoop.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 要想实现数据的排序,需要实现 WritableComparable 接口
 * 接口泛型为当前类名
 *
 * 文件数据格式如下
a 1
a 9
b 3
a 7
b 8
b 10
a 5
 *
 * 根据偏移量计算
 * k1 ->     v1
 *
0     ->     a 1
5     ->     a 9
9     ->     b 3
13    ->     a 7
 *
 * 对应的
 * k2(拆分开v1的值,两个数据通过逗号分隔为SortPairWritable对象的两个field值,
 * v2(对应v1值,即原文本数据)
 *
<a, 1>     ->     a 1
<a, 9>     ->     a 9
<b, 3>     ->     b 3
<a, 7>     ->     a 7
 *
 *
 * @author gin
 * @date 2020/2/19 16:43
 */
public class SortPairWritable implements WritableComparable<SortPairWritable> {

    /**
     * v2 对象的第一个值,排序第一规则
     */
    private String first;

    /**
     * v2 对象的第二个值,排序的第二规则
     */
    private int second;


    //实现排序规则
    @Override
    public int compareTo(SortPairWritable other) {
        //先比较first,如果first相同,再比较second
        int result = this.first.compareTo(other.first);
        if (result == 0){
            //升序排序,当前对象更大则返回正数
            return this.second - other.second;
        }
        return result;
    }

    //实现序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        //String在hadoop中的序列化方法 writeUTF
        dataOutput.writeUTF(first);
        //int在hadoop中的序列化方法 writeInt
        dataOutput.writeInt(second);
    }

    //实现反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        //String 反序列化方法
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public String toString() {
        //最终hadoop生成的文件数据会按照 toString 方法来处理,这里可以设置成需要的数据格式
        return first + " " + second;
    }

}
