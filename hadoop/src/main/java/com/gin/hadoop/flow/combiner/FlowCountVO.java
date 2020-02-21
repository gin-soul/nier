package com.gin.hadoop.flow.combiner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *  上行流量 下行流量 流量统计后的结果进行分区操作
 *
 *  只需要在指定 addInputPath 的读取路径为上次统计后的输出路径即可
 *
 *

 13480253104 3    3   180   180
 13502468823 57   102    7335   110349
 13560439658 33   24 2034    5892
 13600217502 18   138    1080   186852
 13602846565 15   12 1938    2910
 13660577991 24   9  6960 690
 13719199419 4    0   240   0
 13726230503 24   27 2481    24681
 13760778710 2    2   120   120
 13823070001 6    3   360   180
 13826544101 4    0   264   0
 13922314466 12   12 3008    3720
 13925057413 69   63 11058   48243
 13926251106 4    0   240   0
 13926435656 2    4   132   1512
 15013685858 28   27 3659    3538
 15920133257 20   20 3156    2936
 15989002119 3    3   1938  180
 18211575961 15   12 1527    2106
 18320173382 21   18 9531    2412
 84138413    20   16 4116    1432

 *
 * 这里不需要排序,直接实现 Writable,需要排序情况下实现 WritableComparable
 *
 */
public class FlowCountVO implements Writable {

    //上行包总数,下行包总数
    //上行流量,下行流量
    private Integer upFlow;
    private Integer downFlow;
    private Integer upCountFlow;
    private Integer downCountFlow;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(upFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upCountFlow);
        dataOutput.writeInt(downCountFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readInt();
        this.downFlow = dataInput.readInt();
        this.upCountFlow = dataInput.readInt();
        this.downCountFlow = dataInput.readInt();
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getUpCountFlow() {
        return upCountFlow;
    }

    public void setUpCountFlow(Integer upCountFlow) {
        this.upCountFlow = upCountFlow;
    }

    public Integer getDownCountFlow() {
        return downCountFlow;
    }

    public void setDownCountFlow(Integer downCountFlow) {
        this.downCountFlow = downCountFlow;
    }

    @Override
    public String toString() {
        //最终hadoop生成的文件数据会按照 toString 方法来处理,这里可以设置成需要的数据格式
        return upFlow +
                "\t" + downFlow +
                "\t" + upCountFlow +
                "\t" + downCountFlow;
    }

}
