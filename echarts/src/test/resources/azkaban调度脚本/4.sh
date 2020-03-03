#!/bin/bash

#set java env
export JAVA_HOME=/export/servers/jdk1.8.0_141
export JRE_HOME=${JAVA_HOME}/jre
export CLASSPATH=.:${JAVA_HOME}/lib:${JRE_HOME}/lib
export PATH=${JAVA_HOME}/bin:$PATH

#set hadoop env
export HADOOP_HOME=/export/servers/hadoop-2.6.0-cdh5.14.0
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH

#获取时间信息
day_01=`date -d'-1 day' +%Y%m%d`
syear=`date --date=$day_01 +%Y`
smonth=`date --date=$day_01 +%m`
sday=`date --date=$day_01 +%d`

#预处理输出结果(raw)目录
log_pre_output="/weblog/'$day_01'/weblogPreOut"
#点击流clickStream模型预处理程序输出目录
pageView="/weblog/'$day_01'/pageViewOut"
#点击流visit模型预处理程序输出目录
visit="/weblog/'$day_01'/clickStreamVisit"

#目标hive表
ods_weblog_origin="weblog.ods_weblog_origin"
ods_click_pageviews="weblog.ods_click_pageviews"
ods_click_stream_visit="weblog.ods_click_stream_visit"

#导入数据到weblog.ods_weblog_origin
HQL_origin="load data inpath '$log_pre_output' overwrite into table $ods_weblog_origin partition(datestr='$day_01')"
echo $HQL_origin 
/export/servers/hive-1.1.0-cdh5.14.0/bin/hive -e "$HQL_origin"




#导入点击流模型pageviews数据到
HQL_pvs="load data inpath '$pageView' overwrite into table ods_click_pageviews partition(datestr='$day_01')"
echo $HQL_pvs 
/export/servers/hive-1.1.0-cdh5.14.0/bin/hive -e "$HQL_pvs"

#导入点击流模型visit数据到
HQL_vst="load data inpath '$visit' overwrite into table $ods_click_stream_visit partition(datestr='$day_01')"
echo $HQL_vst 
/export/servers/hive-1.1.0-cdh5.14.0/bin/hive -e "$HQL_vst"

