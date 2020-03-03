create database if not exist weblog;
use weblog;



-- 1.1.1 ����ô������Σ�һ�죩�еĸ�Сʱpvs
drop table if exists dw_pvs_everyhour_oneday;
create table if not exists dw_pvs_everyhour_oneday(month string,day string,hour string,pvs bigint) partitioned by(datestr string);

insert  into table dw_pvs_everyhour_oneday partition(datestr='20130918')
select a.month as month,a.day as day,a.hour as hour,count(*) as pvs from ods_weblog_detail a
where  a.datestr='20130918' group by a.month,a.day,a.hour;

-- ����ÿ���pvs
drop table if exists dw_pvs_everyday;
create table if not  exists dw_pvs_everyday(pvs bigint,month string,day string);

insert into table dw_pvs_everyday select count(*) as pvs,a.month as month,a.day as day from ods_weblog_detail a group by a.month,a.day;

-- ͳ��ÿСʱ������url������pv������ѯ������룺( "dw_pvs_referer_everyhour" )

drop table if exists dw_pvs_referer_everyhour;
create table if not exists dw_pvs_referer_everyhour (referer_url string,referer_host string,month string,day string,hour string,pv_referer_cnt bigint) partitioned by(datestr string);

insert into table dw_pvs_referer_everyhour partition(datestr='20130918') select http_referer,ref_host,month,day,hour,count(1) as pv_referer_cnt from ods_weblog_detail  group by http_referer,ref_host,month,day,hour  having ref_host is not null order by hour asc,day asc,month asc,pv_referer_cnt desc;



-- ͳ��ÿСʱ������host�Ĳ�����pv��������
drop table if exists dw_pvs_refererhost_everyhour;
create table if not exists dw_pvs_refererhost_everyhour(ref_host string,month string,day string,hour string,ref_host_cnts bigint) partitioned by(datestr string);

insert into table dw_pvs_refererhost_everyhour partition(datestr='20130918') select ref_host,month,day,hour,count(1) as ref_host_cnts from ods_weblog_detail  group by ref_host,month,day,hour  having ref_host is not null order by hour asc,day asc,month asc,ref_host_cnts desc;




