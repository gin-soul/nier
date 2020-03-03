#!/bin/bash    
#设置日志文件存放目录  
logs_path="/usr/local/nginx/logs/"  
#设置pid文件  
pid_path="/usr/local/nginx/nginx-1.7.3/logs/nginx.pid"  
#日志文件    
filepath=${logs_path}"access.log"  
# Source function library.    
#重命名日志文件  
mv ${logs_path}access.log ${logs_path}access_$(date -d '-1 day' '+%Y-%m-%d').log  
#向nginx主进程发信号重新打开日志  
kill -USR1 `cat ${pid_path}`  