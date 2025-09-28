-- 财务客户损益模型表
-- 核心逻辑： 统计当月客户销售情况、应收金额、资金占用费（客户应收）  （按照账龄表时间段资金占用费=（应收金额*应收天数*6%/365））

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

-- 昨日、昨日、昨日月1日
--select ${hiveconf:current_day},${hiveconf:current_start_mon},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};
set current_day1 =date_sub(current_date,1);
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set current_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
set current_2020 ='20200101';
set created_time =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间
set created_by='raoyanhua';


--临时表：订单应收金额、逾期日期、应收天数
drop table csx_tmp.tmp_cust_order_receivable_days_dtl;
create temporary table csx_tmp.tmp_cust_order_receivable_days_dtl
as
select
  a.order_no,	-- 来源单号
  a.customer_no,	-- 客户编码
  a.company_code,	-- 签约公司编码
  a.happen_date,	-- 发生时间		
  a.overdue_date,	-- 逾期时间	
  a.source_statement_amount,	-- 源单据对账金额
  a.money_back_status,	-- 回款状态
  a.unpaid_amount receivable_amount,	-- 应收金额
  a.account_period_code,	--账期编码 
  a.account_period_name,	--账期名称 
  a.account_period_val,	--账期值
  a.beginning_mark,	--是否期初
  a.bad_debt_amount,	
  a.over_days,	-- 逾期天数
  a.receivable_days,	-- 应收天数
  if(a.unpaid_amount>0,a.unpaid_amount*a.receivable_days*0.06/365,0) capital_takes_up,  --资金占用费
  --if(a.account_period_code like 'Y%', if(a.account_period_val = 31, 45, a.account_period_val + 15), a.account_period_val) as acc_val_calculation_factor,	-- 标准账期
  ${hiveconf:current_day} sdt
from
	(
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
		if((datediff(${hiveconf:current_day1}, happen_date)+1)>=1,datediff(${hiveconf:current_day1}, happen_date),0) as receivable_days,	-- 应收天数
		if((money_back_status<>'ALL' or (datediff(${hiveconf:current_day1}, overdue_date)+1)>=1),datediff(${hiveconf:current_day1}, overdue_date)+1,0) as over_days	-- 逾期天数
	from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	where sdt=${hiveconf:current_day}
	and date(happen_date)<=${hiveconf:current_day1}
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
		if((datediff(${hiveconf:current_day1}, date_sub(from_unixtime(unix_timestamp(overdue_date,'yyyy-MM-dd hh:mm:ss')),coalesce(account_period_val,0)))+1)>=1,datediff(${hiveconf:current_day1}, 
				date_sub(from_unixtime(unix_timestamp(overdue_date,'yyyy-MM-dd hh:mm:ss')),coalesce(account_period_val,0))),0) as receivable_days,	-- 应收天数
		if((money_back_status<>'ALL' or (datediff(${hiveconf:current_day1}, overdue_date)+1)>=1),datediff(${hiveconf:current_day1}, overdue_date)+1,0) as over_days	-- 逾期天数		
	from csx_dw.dwd_sss_r_a_beginning_receivable_20201116 
	where sdt=${hiveconf:current_day}
	)a	
;	

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
  a.customer_no,
  coalesce(b.customer_name,f.shop_name) as customer_name,
  b.sales_id,
  b.work_no,
  b.sales_name,
  a.company_code,	-- 公司代码
  d.company_name,	-- 公司名称  
  -- 客户账期类型会发生变化，因此在子查询中不加入账期类型作为分组值，获取当前最新的账期类型作为展示值
  c.payment_terms as account_period_code, -- 账期编码
  c.payment_name as account_period_name, -- 账期名称
  if(c.payment_terms like 'Y%', if(c.payment_days = 31, 45, c.payment_days + 15), c.payment_days) as account_period_val, -- 帐期天数
  c.payment_short_name as account_period_short_name, -- 账期简称  
  sum(sales_value)sales_value,
  sum(profit) profit  
from 
  (
   select 
     --region_code,region_name,
     --province_code,province_name,
     --city_group_code,city_group_name,
     customer_no,sign_company_code as company_code,
     case when channel_code='2' then channel_code end as channel_code,
  case when channel_code='2' then dc_code end as dc_code,
     sum(sales_value)sales_value,
     sum(profit) profit
   from csx_dw.dws_sale_r_d_detail
   where sdt>=${hiveconf:current_2020} and sdt<=${hiveconf:current_day} 	--销售数据起始日期20200101
   --where sdt>=${hiveconf:current_start_mon} and sdt<=${hiveconf:current_day}    
   group by
     customer_no,sign_company_code,
     case when channel_code='2' then channel_code end,
  case when channel_code='2' then dc_code end
  )a
left join
( -- 获取客户信息
  select
    sales_region_code as region_code,
	sales_region_name as region_name,
	sales_province_code as province_code,
    sales_province_name as province_name,
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
( -- 获取客户+签约公司的详细信息
  select * from csx_dw.dws_crm_w_a_customer_company
  where sdt = 'current'
) c on a.customer_no = c.customer_no and a.company_code = c.company_code
left join
( -- 获取公司编码
  select
    code, name as company_name
  from csx_dw.dws_basic_w_a_company_code
  where sdt = 'current'
) d on a.company_code = d.code
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
  a.customer_no,
  coalesce(b.customer_name,f.shop_name),
  b.sales_id,
  b.work_no,
  b.sales_name,
  a.company_code,	-- 公司代码
  d.company_name,	-- 公司名称   
  c.payment_terms, -- 账期编码
  c.payment_name, -- 账期名称
  if(c.payment_terms like 'Y%', if(c.payment_days = 31, 45, c.payment_days + 15), c.payment_days), -- 帐期天数
  c.payment_short_name -- 账期简称  
;



	
-- 计算不同时间段的资金占用费，类似客户账款统计表
insert overwrite table csx_dw.report_sss_r_d_cust_capital_takes_up partition(sdt)
--drop table csx_tmp.report_sss_r_d_cust_capital_takes_up;
--create temporary table csx_tmp.report_sss_r_d_cust_capital_takes_up
--as
select
  concat_ws('-',${hiveconf:current_day},coalesce(a.customer_no,b.customer_no),coalesce(a.company_code,b.company_code),coalesce(a.city_group_code,b.city_group_code)) as biz_id, 
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
  coalesce(a.company_code,b.company_code) company_code,	-- 公司代码
  coalesce(a.company_name,b.company_name) company_name,	-- 公司名称
  coalesce(a.account_period_code,b.account_period_code) account_period_code, -- 账期编码
  coalesce(a.account_period_name,b.account_period_name) account_period_name, -- 账期名称
  coalesce(a.account_period_val,b.account_period_val) account_period_val, -- 帐期天数
  coalesce(a.account_period_short_name,b.account_period_short_name) account_period_short_name, -- 账期简称
  a.sales_value,
  a.profit,   
  coalesce(b.receivable_amount,0) receivable_amount,		 -- 应收金额
  coalesce(b.capital_takes_up,0) capital_takes_up,      -- 资金占用费
  b.fifteen_capital_takes_up, -- 0-15天资金占用费
  b.thirty_capital_takes_up, -- 16-30天资金占用费
  b.sixty_capital_takes_up, -- 31-60天资金占用费
  b.ninety_capital_takes_up, -- 61-90天资金占用费
  b.one_hundred_twenty_capital_takes_up, -- 91-120天资金占用费
  b.half_a_year_capital_takes_up, -- 121-180天资金占用费，半年内资金占用费
  b.one_year_capital_takes_up, -- 181-365天资金占用费，1年内资金占用费
  b.two_year_capital_takes_up, -- 366-730天资金占用费，2年内资金占用费
  b.three_year_capital_takes_up, -- 731-1095天资金占用费，3年内资金占用费
  b.three_year_over_capital_takes_up, -- 3年以上资金占用费
  ${hiveconf:created_by} create_by,
  ${hiveconf:created_time} create_time,
  ${hiveconf:created_time} update_time,
  ${hiveconf:current_day} as sdt -- 统计日期
from csx_tmp.tmp_cust_order_sales_value a
full join
  (
    select
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
	a1.company_code,	-- 公司代码
	d.company_name,	-- 公司名称	
	-- 客户账期类型会发生变化，因此在子查询中不加入账期类型作为分组值，获取当前最新的账期类型作为展示值
	c.payment_terms as account_period_code, -- 账期编码
	c.payment_name as account_period_name, -- 账期名称
	if(c.payment_terms like 'Y%', if(c.payment_days = 31, 45, c.payment_days + 15), c.payment_days) as account_period_val, -- 帐期天数
	c.payment_short_name as account_period_short_name, -- 账期简称	
	a1.receivable_amount,		 -- 应收金额
	a1.capital_takes_up,      -- 资金占用费
	a1.fifteen_capital_takes_up,      -- 0-15天资金占用费
	a1.thirty_capital_takes_up,      -- 16-30天资金占用费
	a1.sixty_capital_takes_up,      -- 31-60天资金占用费
	a1.ninety_capital_takes_up,      -- 61-90天资金占用费
	a1.one_hundred_twenty_capital_takes_up,      -- 91-120天资金占用费
	a1.half_a_year_capital_takes_up,      -- 121-180天资金占用费，半年内资金占用费
	a1.one_year_capital_takes_up,      -- 181-365天资金占用费，1年内资金占用费
	a1.two_year_capital_takes_up,      -- 366-730天资金占用费，2年内资金占用费
	a1.three_year_capital_takes_up,      -- 731-1095天资金占用费，3年内资金占用费
	a1.three_year_over_capital_takes_up      -- 3年以上资金占用费
from 	
    (
	
    select
      customer_no,
      company_code,
      sum(receivable_amount) as receivable_amount,		 -- 应收金额
      sum(if(capital_takes_up>0,capital_takes_up,0)) as capital_takes_up,      -- 资金占用费
      -- 0-15天资金占用费
      sum( if(receivable_days >= 0 and receivable_days <= 15, if(capital_takes_up>0,capital_takes_up,0),0)) as fifteen_capital_takes_up,
      -- 16-30天资金占用费
      sum( if(receivable_days >= 16 and receivable_days <= 30, if(capital_takes_up>0,capital_takes_up,0),0)) as thirty_capital_takes_up,
      -- 31-60天资金占用费
      sum( if(receivable_days >= 31 and receivable_days <= 60, if(capital_takes_up>0,capital_takes_up,0),0)) as sixty_capital_takes_up,
      -- 61-90天资金占用费
      sum( if(receivable_days >= 61 and receivable_days <= 90, if(capital_takes_up>0,capital_takes_up,0),0)) as ninety_capital_takes_up,
      -- 91-120天资金占用费
      sum( if(receivable_days >= 91 and receivable_days <= 120, if(capital_takes_up>0,capital_takes_up,0),0)) as one_hundred_twenty_capital_takes_up,
      -- 121-180天资金占用费，半年内资金占用费
      sum( if(receivable_days >= 121 and receivable_days <= 180, if(capital_takes_up>0,capital_takes_up,0),0)) as half_a_year_capital_takes_up,
      -- 181-365天资金占用费，1年内资金占用费
      sum( if(receivable_days >= 181 and receivable_days <= 365, if(capital_takes_up>0,capital_takes_up,0),0)) as one_year_capital_takes_up,
      -- 366-730天资金占用费，2年内资金占用费
      sum( if(receivable_days >= 366 and receivable_days <= 730, if(capital_takes_up>0,capital_takes_up,0),0)) as two_year_capital_takes_up,
      -- 731-1095天资金占用费，3年内资金占用费
      sum( if(receivable_days >= 731 and receivable_days <= 1095, if(capital_takes_up>0,capital_takes_up,0),0)) as three_year_capital_takes_up,
      -- 3年以上资金占用费
      sum( if(receivable_days >= 1096, if(capital_takes_up>0,capital_takes_up,0),0)) as three_year_over_capital_takes_up
    from csx_tmp.tmp_cust_order_receivable_days_dtl
    group by customer_no, company_code
    )a1 
left join
( -- 获取客户信息
  select
    sales_region_code as region_code,
	sales_region_name as region_name,
	sales_province_code as province_code,
    sales_province_name as province_name,
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
) b on a1.customer_no = b.customer_no
left join
( -- 获取客户+签约公司的详细信息
  select * from csx_dw.dws_crm_w_a_customer_company
  where sdt = 'current'
) c on a1.customer_no = c.customer_no and a1.company_code = c.company_code
left join
( -- 获取公司编码
  select
    code, name as company_name
  from csx_dw.dws_basic_w_a_company_code
  where sdt = 'current'
) d on a1.company_code = d.code
)b on a.customer_no=b.customer_no and a.company_code=b.company_code
;



--INVALIDATE METADATA csx_dw.report_sss_r_d_cust_capital_takes_up;

/*

---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------

--财务客户损益模型表 csx_dw.report_sss_r_d_cust_capital_takes_up

drop table if exists csx_dw.report_sss_r_d_cust_capital_takes_up;
create table csx_dw.report_sss_r_d_cust_capital_takes_up(
  `biz_id` string COMMENT  '唯一值',
  `region_code` string COMMENT  '大区编码',
  `region_name` string COMMENT  '大区',
  `province_code` string COMMENT  '省区编码',
  `province_name` string COMMENT  '省区',
  `city_group_code` string COMMENT  '城市组编码',
  `city_group_name` string COMMENT  '城市组',
  `channel_code` string COMMENT  '渠道编码',
  `channel_name` string COMMENT  '渠道编码',
  `customer_no` string COMMENT  '客户编号',
  `customer_name` string COMMENT  '客户名称',
  `sales_id` string COMMENT  '销售员编码',
  `work_no` string COMMENT  '销售员工号',
  `sales_name` string COMMENT  '销售员',
  `company_code` string COMMENT  '公司代码',
  `company_name` string COMMENT  '公司名称',
  `account_period_code` string COMMENT  '账期编码',
  `account_period_name` string COMMENT  '账期名称',
  `account_period_val` decimal(12,0)  COMMENT '标准账期天数',
  `account_period_short_name` string COMMENT  '账期简称',
  `sales_value` decimal(26,6)  COMMENT '月至今销售额',
  `profit` decimal(26,6)  COMMENT '月至今毛利额',
  `receivable_amount` decimal(26,6)  COMMENT '应收金额',
  `capital_takes_up` decimal(26,6)  COMMENT '资金占用费',
  `fifteen_capital_takes_up` decimal(26,6)  COMMENT '0-15天资金占用费',
  `thirty_capital_takes_up` decimal(26,6)  COMMENT '16-30天资金占用费',
  `sixty_capital_takes_up` decimal(26,6)  COMMENT '31-60天资金占用费',
  `ninety_capital_takes_up` decimal(26,6)  COMMENT '61-90天资金占用费',
  `one_hundred_twenty_capital_takes_up` decimal(26,6)  COMMENT '91-120天资金占用费',
  `half_a_year_capital_takes_up` decimal(26,6)  COMMENT '121-180天资金占用费，半年内资金占用费',
  `one_year_capital_takes_up` decimal(26,6)  COMMENT '181-365天资金占用费，1年内资金占用费',
  `two_year_capital_takes_up` decimal(26,6)  COMMENT '366-730天资金占用费，2年内资金占用费',
  `three_year_capital_takes_up` decimal(26,6)  COMMENT '731-1095天资金占用费，3年内资金占用费',
  `three_year_over_capital_takes_up` decimal(26,6)  COMMENT '3年以上资金占用费', 
  `create_by` string COMMENT  '创建人',
  `create_time` timestamp comment '创建时间',
  `update_time` timestamp comment '更新时间'
) COMMENT '财务客户损益模型表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;


select province_name,
  sum(sales_value)sales_value,
  sum(profit) profit,
  sum(receivable_amount) as receivable_amount,		 -- 应收金额
  sum(if(capital_takes_up>0,capital_takes_up,0)) as capital_takes_up      -- 资金占用费     	
from csx_dw.report_sss_r_d_cust_capital_takes_up
where sdt='20210224'
group by province_name
;


