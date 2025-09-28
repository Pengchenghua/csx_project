-- 财务客户损益模型表
-- 核心逻辑： 统计当月客户销售情况、应收金额、资金占用费（客户应收）  （按照账龄表时间段资金占用费=（应收金额*应收天数*6%/365））
--20210825 因运费取不到BBC，所以销售与应收都只要大客户的（期初订单都作为大客户）；单日资金占用费=（（当日应收+前一日应收）/2）*0.06/365

-- 切换tez计算引擎
set mapred.job.name=report_sss_r_d_cust_capital_takes_up;
SET hive.execution.engine=tez;
SET tez.queue.name=caishixian;

-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions =1000;
SET hive.exec.max.dynamic.partitions.pernode =1000;

-- 中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;

-- 启用引号识别
set hive.support.quoted.identifiers=none;

-- 昨日、昨日、昨日月1日、上月最后一天、上月最后一天、上月1日、上月1日、上上月最后一天
--select ${hiveconf:current_day},${hiveconf:current_start_mon},${hiveconf:before1_last_mon},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};
set current_day1 =date_sub(current_date,1);
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set current_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
set before1_last_mon1 =last_day(add_months(date_sub(current_date,1),-1));
set before1_last_mon =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');
set before1_start_mon1 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set before1_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set before2_last_mon =regexp_replace(last_day(add_months(date_sub(current_date,1),-2)),'-','');
set created_time =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间
set created_by='raoyanhua';


		  

--临时表1：客户上月至今每日的应收金额
drop table csx_tmp.tmp_cust_order_receivable_days_dtl_1;
create temporary table csx_tmp.tmp_cust_order_receivable_days_dtl_1
as
select
  a.sdt,
  a.customer_no,
  --a.company_code,	-- 签约公司编码
  sum(a.unpaid_amount) as receivable_amount	-- 应收金额
from
  (
    --非期初对账单
    select 
    	source_bill_no as order_no,	-- 来源单号
    	customer_code as customer_no,	-- 客户编码
    	company_code,	-- 签约公司编码
    	happen_date,	-- 发生时间		
    	overdue_date,	-- 逾期时间	
    	source_statement_amount,	-- 源单据对账金额
    	money_back_status,	-- 回款状态
    	unpaid_amount,	-- 未回款金额
    	account_period_code,	--账期编码 
    	account_period_name,	--账期名称 
    	account_period_val,	--账期值
    	'否' as beginning_mark,	--是否期初
    	bad_debt_amount,
    	sdt
    from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
    where sdt>=${hiveconf:before2_last_mon}
    and regexp_replace(date(happen_date),'-','')<=sdt
    --and substr(source_bill_no,1,2) not in ('19','20','21','R1','R2')  --剔除BBC订单
    --期初对账单
    union all
    select 
    	id as order_no,	-- 来源单号
    	customer_code as customer_no,	-- 客户编码
    	company_code,	-- 签约公司编码		
    	date_sub(from_unixtime(unix_timestamp(overdue_date,'yyyy-MM-dd hh:mm:ss')),coalesce(account_period_val,0)) as happen_date,	-- 发生时间		
    	overdue_date,	-- 逾期时间	
    	beginning_amount source_statement_amount,	-- 源单据对账金额
    	money_back_status,	-- 回款状态
    	unpaid_amount,	-- 未回款金额
    	account_period_code,	--账期编码 
    	account_period_name,	--账期名称 
    	account_period_val,	--账期值
    	'是' as beginning_mark,	--是否期初	
    	bad_debt_amount,
           sdt		
    from csx_dw.dwd_sss_r_a_beginning_receivable_20201116  
    where sdt>=${hiveconf:before2_last_mon}
  )a
left join
  (
    select distinct order_no,channel_code from csx_dw.dws_sale_r_d_detail
  )b on b.order_no=a.order_no
where b.channel_code<>'7' or a.beginning_mark='是'  --剔除BBC订单 
group by a.sdt,a.customer_no;	


--临时表2：客户上月至今每日的当日应收金额、昨日应收金额、资金占用费
drop table csx_tmp.tmp_cust_order_receivable_days_dtl_2;
create temporary table csx_tmp.tmp_cust_order_receivable_days_dtl_2
as
select sdt,customer_no,
  coalesce(sum(receivable_amount),0) as receivable_amount,  --当日应收
  coalesce(sum(receivable_amount_last),0) as receivable_amount_last,   --前1日应收
  ((coalesce(sum(receivable_amount),0)+coalesce(sum(receivable_amount_last),0))/2)*0.06/365 capital_takes_up      -- 资金占用费
from
(
  select sdt,customer_no,receivable_amount,0 receivable_amount_last
  from csx_tmp.tmp_cust_order_receivable_days_dtl_1 
  --where sdt>=${hiveconf:before1_start_mon}
  union all
  select regexp_replace(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-1),'-','') as sdt,
    customer_no,0 receivable_amount,receivable_amount as receivable_amount_last
  from csx_tmp.tmp_cust_order_receivable_days_dtl_1
  --where sdt<${hiveconf:current_day}
)a
where sdt>=${hiveconf:before1_start_mon} and sdt<=${hiveconf:current_day}
group by sdt,customer_no;


--临时表3：客户上月至今每日的销售  不含税销售额 大客户不含BBC
drop table csx_tmp.tmp_cust_order_sales_value;
create temporary table csx_tmp.tmp_cust_order_sales_value
as
select 
  coalesce(b.region_code,e.region_code,'99') as region_code,
  coalesce(b.region_name,e.region_name,'其他') as region_name,
  coalesce(b.province_code,e.province_code,'99') as province_code,
  coalesce(b.province_name,e.province_name,'其他') as province_name,
  coalesce(b.city_group_code,e.city_group_code,'99') as city_group_code,
  coalesce(b.city_group_name,e.city_group_name,'其他') as city_group_name,  
  coalesce(b.channel_code,case when f.shop_name is not null then '2' end) as channel_code,
  coalesce(b.channel_name,case when f.shop_name is not null then '商超' end) as channel_name, 
  a.sdt,a.customer_no,
  coalesce(b.customer_name,f.shop_name) as customer_name,
  b.sales_id,
  b.work_no,
  b.sales_name, 
  sum(excluding_tax_sales) sales_value,
  sum(excluding_tax_profit) profit  
from 
  (
   select 
     --region_code,region_name,
     --province_code,province_name,
     --city_group_code,city_group_name,
     sdt,customer_no,
     case when channel_code='2' then channel_code end as channel_code,
     case when channel_code='2' then dc_code end as dc_code,
     sum(excluding_tax_sales) excluding_tax_sales,
     sum(excluding_tax_profit) excluding_tax_profit
   from csx_dw.dws_sale_r_d_detail
   --where sdt>=${hiveconf:current_2020} and sdt<=${hiveconf:current_day} 	--销售数据起始日期20200101
   where sdt>=${hiveconf:before1_start_mon} and sdt<=${hiveconf:current_day} 
   and channel_code<>'7'   --剔除BBC订单 
   group by
     sdt,customer_no,
     case when channel_code='2' then channel_code end,
  case when channel_code='2' then dc_code end
  )a
left join
  ( -- 获取客户信息
    select
      sales_region_code as region_code,
  	sales_region_name as region_name,
  	province_code,
      province_name,
  	city_group_code,
  	city_group_name,
      channel_code,
      channel_name,
      customer_no,
      customer_name,
      sales_id,
      work_no,
      sales_name
    from csx_dw.dws_crm_w_a_customer
    where sdt = ${hiveconf:current_day}
  ) b on a.customer_no = b.customer_no
left join
  ( -- 获取商超客户信息:省区、城市
    select 
      sales_region_code as region_code,
  	sales_region_name as region_name,
  	sales_province_code as province_code,
      sales_province_name as province_name,
  	city_group_code,
  	city_group_name,
  	shop_id,
  	shop_name 
    from csx_dw.dws_basic_w_a_csx_shop_m a 
    where sdt='current'
  )e on a.dc_code= e.shop_id
left join
  ( -- 获取商超客户信息:客户名称、门店名称
    select 
      sales_region_code as region_code,
  	sales_region_name as region_name,
  	sales_province_code as province_code,
      sales_province_name as province_name,
  	city_group_code,
  	city_group_name,
  	shop_id,
  	shop_name 
    from csx_dw.dws_basic_w_a_csx_shop_m a 
    where sdt='current'
  )f on substr(a.customer_no,2,4)= f.shop_id
group by 
  coalesce(b.region_code,e.region_code,'99'),
  coalesce(b.region_name,e.region_name,'其他'),
  coalesce(b.province_code,e.province_code,'99'),
  coalesce(b.province_name,e.province_name,'其他'),
  coalesce(b.city_group_code,e.city_group_code,'99'),
  coalesce(b.city_group_name,e.city_group_name,'其他'),  
  coalesce(b.channel_code,case when f.shop_name is not null then '2' end),
  coalesce(b.channel_name,case when f.shop_name is not null then '商超' end), 
  a.sdt,a.customer_no,
  coalesce(b.customer_name,f.shop_name),
  b.sales_id,
  b.work_no,
  b.sales_name  
;


--结果表
insert overwrite table csx_dw.report_sss_r_d_cust_capital_takes_up partition(sdt)
--drop table csx_tmp.report_sss_r_d_cust_capital_takes_up;
--create temporary table csx_tmp.report_sss_r_d_cust_capital_takes_up
--as
select
  concat_ws('-',coalesce(a.sdt,b.sdt),coalesce(a.customer_no,b.customer_no),coalesce(a.city_group_code,b.city_group_code)) as biz_id, 
  coalesce(a.region_code,b.region_code,'99') as region_code,
  coalesce(a.region_name,b.region_name,'其他') as region_name,
  coalesce(a.province_code,b.province_code,'99') as province_code,
  coalesce(a.province_name,b.province_name,'其他') as province_name,
  coalesce(a.city_group_code,b.city_group_code,'99') as city_group_code,
  coalesce(a.city_group_name,b.city_group_name,'其他') as city_group_name,  
  coalesce(a.channel_code,b.channel_code) as channel_code,
  coalesce(a.channel_name,b.channel_name) as channel_name,
  coalesce(a.customer_no,b.customer_no) customer_no,
  coalesce(a.customer_name,b.customer_name) as customer_name,
  coalesce(a.sales_id,b.sales_id) sales_id,
  coalesce(a.work_no,b.work_no) work_no,
  coalesce(a.sales_name,b.sales_name) sales_name,
  a.sales_value,
  a.profit,   
  coalesce(b.receivable_amount,0) receivable_amount,		 -- 应收金额
  coalesce(b.receivable_amount_last,0) receivable_amount_last,		 -- 前1日应收
  coalesce(b.capital_takes_up,0) capital_takes_up,      -- 资金占用费
  coalesce(b.transport_amount,0) transport_amount,      -- 运费
  ${hiveconf:created_by} create_by,
  ${hiveconf:created_time} create_time,
  ${hiveconf:created_time} update_time, 
  coalesce(a.sdt,b.sdt) sdt -- 统计日期 
from csx_tmp.tmp_cust_order_sales_value a
full join
(
  select a1.sdt,
	  coalesce(b.region_code,'99') as region_code,
	  coalesce(b.region_name,'其他') as region_name,
	  coalesce(b.province_code,'99') as province_code,
	  coalesce(b.province_name,'其他') as province_name,
	  coalesce(b.city_group_code,'99') as city_group_code,
	  coalesce(b.city_group_name,'其他') as city_group_name,  
	  b.channel_code,
	  b.channel_name, 	
	  a1.customer_no,
	  b.customer_name,
	  b.sales_id,
	  b.work_no,
	  b.sales_name,
	  a1.receivable_amount,		 -- 应收金额
	  a1.receivable_amount_last,		 -- 前1日应收
	  a1.capital_takes_up,      -- 资金占用费
	  a1.transport_amount       -- 运费
  from 	
  (
    select 
	    sdt,customer_no,
	    sum(receivable_amount) receivable_amount,		 -- 应收金额
	    sum(receivable_amount_last) receivable_amount_last,		 -- 前1日应收
	    sum(capital_takes_up) capital_takes_up,      -- 资金占用费
	    sum(transport_amount) transport_amount       -- 运费	
    from 
    (	  
        --客户上月至今每日的当日应收金额、昨日应收金额、资金占用费
      select
        sdt,customer_no,
        receivable_amount,		 -- 应收金额
        receivable_amount_last,   --前1日应收
		  capital_takes_up,      -- 资金占用费
		  0 transport_amount      -- 运费
      from csx_tmp.tmp_cust_order_receivable_days_dtl_2
      --客户上月至今每日的运费
      union all
      select 
        a2.sdt,a2.customer_no,
	      0 as receivable_amount,		 -- 应收金额
	      0 receivable_amount_last,   --前1日应收
	      0 as capital_takes_up,      -- 资金占用费
	      a2.transport_amount      -- 运费	  
      from
      (
        select regexp_replace(send_date,'-','') as sdt,
          customer_code as customer_no, --dc_code,
          sum(transport_amount)/1.09 as transport_amount      -- 运费
        from csx_dw.dws_tms_r_d_entrucking_order_detail a
        left semi
        join 
        (
          select * 
          from csx_dw.dwd_tms_r_d_entrucking_order
          where status_code != 100
        )b on a.entrucking_code = b.entrucking_code             
        where sdt>=${hiveconf:before1_start_mon}
        and regexp_replace(send_date,'-','')>=${hiveconf:before1_start_mon} and regexp_replace(send_date,'-','')<=${hiveconf:current_day}
        group by regexp_replace(send_date,'-',''),customer_code
      )a2 
	  )a 
	  group by sdt,customer_no
   )a1 
  left join
  ( -- 获取客户信息
    select
      sales_region_code as region_code,
  	  sales_region_name as region_name,
  	  province_code,
      province_name,
  	  city_group_code,
  	  city_group_name,
      channel_code,
      channel_name,
      customer_no,
      customer_name,
      sales_id,
      work_no,
      sales_name
    from csx_dw.dws_crm_w_a_customer
    where sdt = ${hiveconf:current_day}
  )b on a1.customer_no = b.customer_no
)b on a.customer_no=b.customer_no and a.sdt=b.sdt
;

