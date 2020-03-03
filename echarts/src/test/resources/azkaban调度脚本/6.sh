#!/bin/bash

#数据导出
/export/servers/sqoop-1.4.6-cdh5.14.0/bin/sqoop export --connect jdbc:mysql://192.168.25.25:3306/weblog --username root --password admin --m 1  --export-dir /user/hive/warehouse/weblog.db/dw_pvs_everyday  --table dw_pvs_everyday  --input-fields-terminated-by '\001'

#数据导出
/export/servers/sqoop-1.4.6-cdh5.14.0/bin/sqoop export --connect jdbc:mysql://192.168.25.25:3306/weblog --username root --password admin --m 1  --export-dir /user/hive/warehouse/weblog.db/dw_pvs_everyhour_oneday/datestr=20130918  --table dw_pvs_everyhour_oneday  --input-fields-terminated-by '\001' 

#数据导出
/export/servers/sqoop-1.4.6-cdh5.14.0/bin/sqoop export --connect jdbc:mysql://192.168.25.25:3306/weblog --username root --password admin --m 1  --export-dir /user/hive/warehouse/weblog.db/dw_pvs_referer_everyhour/datestr=20130918  --table dw_pvs_referer_everyhour  --input-fields-terminated-by '\001' 

