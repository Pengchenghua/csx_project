--客户近12月内销售数据，其中逾期金额、逾期金额占比两个指标为历史所有数据
--指标及权重：得分_最近销售距今天数(0~15),得分_销售天数(0~15),得分_销售金额(0~35),得分_逾期金额(-10~0),得分_逾期金额占比(-10~0),得分_毛利率(-15~15)
--因逾期金额、逾期金额占比是负向分，因此所有客户最后得分+20，使得客户得分范围0-100
--客户价值分类（此项写了计算方式最后未使用）：按分值高低排名，前20%为高价值，后20%为低价值，其他为中价值

-- 切换tez计算引擎
set mapred.job.name=sale_r_d_customer_score;
set hive.execution.engine=tez;
set tez.queue.name=caishixian;

-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions =1000;
set hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set mapred.output.compression.type=BLOCK;
set parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 昨日、12月前1日
--select ${hiveconf:current_day},${hiveconf:current_before_12mon};
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set current_before_12mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-12),'-','');



 -- 临时表1：大客户12个月内最小、最大成交日期 ，不含城市服务商业绩 客户价值用
drop table csx_tmp.tmp_cust_value_score_1;
create temporary table csx_tmp.tmp_cust_value_score_1
as 
select
  a.customer_no,
  a.min_sdt,
  a.max_sdt,
  a.last_sales_days,
  a.sum_sales_value,
  a.profit_rate,
  a.count_day,
  coalesce(b.receivable_amount,0) receivable_amount,
  coalesce(b.overdue_amount,0) overdue_amount,
  coalesce(coalesce(b.overdue_amount,0)/if(d.sales_value<=b.overdue_amount,b.overdue_amount,d.sales_value),0)as overdue_amount_rate  -- 逾期金额占比  
from 
  (
  select
    customer_no,
    min(sdt)as min_sdt,
    max(sdt)as max_sdt,
    datediff(to_date(current_date),to_date(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd'))))-1 as last_sales_days,
    sum(sales_value)as sum_sales_value,
    sum(profit)/abs(sum(sales_value))as profit_rate,
    count(distinct sdt)as count_day
  from csx_dw.dws_sale_r_d_detail
  where sdt>=${hiveconf:current_before_12mon}
  and channel_code in ('1','7','9')
  and business_type_code<>'4'
  and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046',
               'OC20111000000021','OC20111000000022','OC20111000000023','OC20111000000024','OC20111000000025') or order_no is null)
  group by customer_no
  )a
left join  -- 客户逾期金额 
  (
  select customer_no,
    sum(if(receivable_amount>=0,receivable_amount,0))as receivable_amount,
    sum(if(receivable_amount>=0 and overdue_amount>=0,overdue_amount,0))as overdue_amount
  from csx_dw.dws_sss_r_a_customer_accounts
  where sdt=${hiveconf:current_day}
  group by customer_no
  )b on b.customer_no=a.customer_no  
left join  -- 客户历史总销售额 
  (
    select 
      customer_no,
      sum(sales_value)sales_value
    from 
      (
      select customer_no,sdt,sales_value 
      from csx_dw.sale_item_m
      where sdt>='20180101' and sdt<'20190101' 
      and sales_type in ('qyg','sapqyg','sapgc','sc','bbc','gc','anhui') 
      and (channel like '大客户%' or channel like '企业购%' or length(channel)=0)
      union all
      select customer_no,sdt,sales_value
      from csx_dw.dws_sale_r_d_detail
      where sdt>='20190101' 
      and channel_code in('1','7','9')
	  and business_type_code<>'4'
      and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046','OM200724005043','OM200724005068','OC200724000008','OC200724000007') or order_no is null)
      )a
    group by customer_no
  )d on d.customer_no=a.customer_no;  

  

-- 临时表2：客户价值标签 12个月内价值
drop table csx_tmp.tmp_cust_value_score_2;
create temporary table csx_tmp.tmp_cust_value_score_2
as
select
  a.customer_no,a.counts,a.score,a.ranks,
  case when a.ranks/a.counts<=0.2 then '高价值'
     when a.ranks/a.counts>0.2 and a.ranks/a.counts<=0.8 then '中价值'
     when a.ranks/a.counts>0.8 then '低价值'
     end label_value,
  a.score_last_sales_days,a.score_count_day,a.score_sales_value,a.score_overdue_amount,a.score_overdue_amount_rate,a.score_profit_rate,
  c.last_sales_days,c.count_day,c.sum_sales_value,c.receivable_amount,c.overdue_amount,c.overdue_amount_rate,c.profit_rate
from 
  (select
    a.customer_no,
    a.counts,
    a.score,
    rank() over(order by a.score desc)as ranks,
    a.score_last_sales_days,
    a.score_count_day,
    a.score_sales_value,
    a.score_overdue_amount,
    a.score_overdue_amount_rate,
    a.score_profit_rate
  from 
    (select
      a.customer_no,
      a.counts,
      20+(1-bz_last_sales_days)*15+bz_count_day*15+bz_sales_value*35-bz_overdue_amount*10-overdue_amount_rate*10+profit_rate*15 as score,
      (1-bz_last_sales_days)*15 as score_last_sales_days,
      bz_count_day*15 as score_count_day,
      bz_sales_value*35 as score_sales_value,
      -bz_overdue_amount*10 as score_overdue_amount,
      -overdue_amount_rate*10 as score_overdue_amount_rate,
      profit_rate*15 as score_profit_rate
    from 
      (select
        a.customer_no,
        b.counts,
        (lg_last_sales_days-min_lg_last_sales_days)/(max_lg_last_sales_days-min_lg_last_sales_days)as bz_last_sales_days,
        (lg_count_day-min_lg_count_day)/(max_lg_count_day-min_lg_count_day)as bz_count_day,
        (lg_sales_value-min_lg_sales_value)/(max_lg_sales_value-min_lg_sales_value)as bz_sales_value,
        (lg_overdue_amount-min_lg_overdue_amount)/(max_lg_overdue_amount-min_lg_overdue_amount)as bz_overdue_amount,
        overdue_amount_rate,
        coalesce(profit_rate,0) profit_rate
      from 
        (select
          1 as id,
          a.customer_no,
          log10(a.last_sales_days+2)as lg_last_sales_days,--最近销售距今天数
          log10(a.count_day+2)as lg_count_day, -- 销售天数
          log10(if(a.sum_sales_value<=0,0,a.sum_sales_value)+2)as lg_sales_value, --总销售金额
          log10(a.overdue_amount+2)as lg_overdue_amount,  -- 总逾期金额
          a.overdue_amount_rate,  -- 逾期金额占比
          --coalesce(a.profit_rate,0) profit_rate -- 毛利率
          coalesce(case when a.profit_rate>=0.5 then 0.5 
          when a.profit_rate<=-0.5 then -0.5
          else a.profit_rate end,0) *2 profit_rate --毛利率
        from csx_tmp.tmp_cust_value_score_1 a
        where a.min_sdt<>''
        )a
      left join
        (select
          1 as id,
          count(customer_no) counts,
          min(a.lg_last_sales_days) min_lg_last_sales_days,
          max(a.lg_last_sales_days) max_lg_last_sales_days,
          min(a.lg_count_day) min_lg_count_day,
          max(a.lg_count_day) max_lg_count_day,
          min(a.lg_sales_value) min_lg_sales_value,
          max(a.lg_sales_value) max_lg_sales_value,
          min(a.lg_overdue_amount) min_lg_overdue_amount,
          max(a.lg_overdue_amount) max_lg_overdue_amount,
          min(a.overdue_amount_rate) min_overdue_amount_rate,
          max(a.overdue_amount_rate) max_overdue_amount_rate
        from
          (select
            a.customer_no,
            log10(a.last_sales_days+2)as lg_last_sales_days,--最近销售距今天数
            log10(a.count_day+2)as lg_count_day, -- 销售天数
            log10(if(a.sum_sales_value<=0,0,a.sum_sales_value)+2)as lg_sales_value, --总销售金额
            log10(a.overdue_amount+2)as lg_overdue_amount,  -- 总逾期金额
            a.overdue_amount_rate,  -- 逾期金额占比
            --coalesce(a.profit_rate,0) profit_rate -- 毛利率
            coalesce(case when a.profit_rate>=0.5 then 0.5 
            when a.profit_rate<=-0.5 then -0.5
            else a.profit_rate end,0) *2 profit_rate --毛利率            
          from csx_tmp.tmp_cust_value_score_1 a
          where a.min_sdt<>''
          )a
        )b on b.id=a.id
      )a
    )a
  )a  
left join csx_tmp.tmp_cust_value_score_1 c on c.customer_no=a.customer_no;


 --结果表1：客户价值得分
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
set hive.exec.parallel=TRUE;
set hive.exec.dynamic.partition=TRUE;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.sale_r_d_customer_score partition (sdt)
select 
  a.customer_no,b.customer_name,b.region_code,b.region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
  b.sales_name,b.work_no,b.first_category_name,b.second_category_name,b.third_category_name,b.first_sign_time,
  a.counts,a.score,a.ranks,a.label_value,
  score_last_sales_days,score_count_day,score_sales_value,score_overdue_amount,score_overdue_amount_rate,score_profit_rate,
  last_sales_days,count_day,sum_sales_value,receivable_amount,overdue_amount,overdue_amount_rate,profit_rate,
  ${hiveconf:current_day} sdt
from csx_tmp.tmp_cust_value_score_2 a
--客户信息
left join 
  (
    select 
	a.customer_no,a.customer_name,b.region_code,b.region_name,b.province_code,b.province_name,b.city_group_code,b.city_group_name,
	--dev_source_name,sales_province_name,sales_city_name,channel_name,
    a.sales_name,a.work_no,a.first_category_name,a.second_category_name,a.third_category_name,
	regexp_replace(split(a.first_sign_time, ' ')[0], '-', '') first_sign_time
	from
    (
      select *
      from csx_dw.dws_crm_w_a_customer
      where sdt='current' 
    ) a left join
    ( -- 获取省区与城市组信息
      select
        city_code,area_province_name,city_group_code,city_group_name, province_code,province_name,region_code,region_name
      from csx_dw.dws_sale_w_a_area_belong
    )b on a.sales_province_name = b.area_province_name and a.sales_city_code = b.city_code
  )b on a.customer_no=b.customer_no;


/*
--------------------------------- hive建表语句 两个总表 -------------------------------
-- csx_dw.sale_r_d_customer_score  客户价值得分

drop table if exists csx_dw.sale_r_d_customer_score;
create table csx_dw.sale_r_d_customer_score(
  `customer_no` string COMMENT '客户编号',
  `customer_name` string COMMENT '客户名称',
  `region_code` string COMMENT '大区编码',
  `region_name` string COMMENT '大区名称',
  `province_code` string COMMENT '省区编码',
  `province_name` string COMMENT '省区名称',
  `city_group_code` string COMMENT '城市组编码',
  `city_group_name` string COMMENT '城市组名称',
  `sales_name` string COMMENT '销售员',
  `work_no` string COMMENT '销售员工号',
  `first_category_name` string COMMENT '一级客户分类名称',
  `second_category_name` string COMMENT '二级客户分类名称',
  `third_category_name` string COMMENT '三级客户分类名称',
  `first_sign_time` string COMMENT '签约日期',
  `counts` decimal(26,2) COMMENT '客户数',
  `score` decimal(26,6) COMMENT '价值得分',
  `ranks` decimal(26,6) COMMENT '价值排名',  
  `label_value` string COMMENT '价值标签',
  `score_last_sales_days` decimal(26,6)  COMMENT '得分_最近销售距今天数',
  `score_count_day` decimal(26,6)  COMMENT '得分_销售天数',
  `score_sales_value` decimal(26,6)  COMMENT '得分_销售金额',
  `score_overdue_amount` decimal(26,6)  COMMENT '得分_逾期金额',
  `score_overdue_amount_rate` decimal(26,6)  COMMENT '得分_逾期金额占比',
  `score_profit_rate` decimal(26,6)  COMMENT '得分_毛利率',
  `last_sales_days` decimal(26,6)  COMMENT '最近销售距今天数',
  `count_day` decimal(26,6)  COMMENT '销售天数',
  `sum_sales_value` decimal(26,6)  COMMENT '销售金额',
  `receivable_amount` decimal(26,6)  COMMENT '应收金额',
  `overdue_amount` decimal(26,6)  COMMENT '逾期金额',
  `overdue_amount_rate` decimal(26,6)  COMMENT '逾期金额占比',
  `profit_rate` decimal(26,6)  COMMENT '毛利率'
) COMMENT '客户价值得分'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/
