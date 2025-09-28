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

-- 昨日、昨日、昨日月1日、上月最后一天
--select ${hiveconf:current_day},${hiveconf:current_start_mon},${hiveconf:before1_last_mon},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};
set current_day1 =date_sub(current_date,1);
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set current_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
set before1_last_mon =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');
set current_2020 ='20200101';
set created_time =from_utc_timestamp(current_timestamp(),'GMT');	------当前时间
set created_by='raoyanhua';



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
   --where sdt>=${hiveconf:current_2020} and sdt<=${hiveconf:current_day} 	--销售数据起始日期20200101
   where sdt>=${hiveconf:current_start_mon} and sdt<=${hiveconf:current_day}    
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
  coalesce(b.receivable_amount_last_mon,0) receivable_amount_last_mon,		 -- 上月末应收金额
  coalesce(b.capital_takes_up,0) capital_takes_up,      -- 资金占用费
  b.transport_amount,      -- 运费
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
	a1.receivable_amount_last_mon,		 -- 上月末应收金额
	a1.capital_takes_up,      -- 资金占用费
	a1.transport_amount       -- 运费
  from 	
   (
      select 
	    customer_no,company_code,
	    sum(receivable_amount) receivable_amount,		 -- 应收金额
	    sum(receivable_amount_last_mon) receivable_amount_last_mon,		 -- 上月末应收金额
	    sum(capital_takes_up) capital_takes_up,      -- 资金占用费
	    sum(transport_amount) transport_amount       -- 运费	
      from 
       (	  
	    select customer_no,company_code,
	      sum(coalesce(case when sdt=${hiveconf:current_day} then if(receivable_amount>=0,receivable_amount,0) end,0)) as receivable_amount,		 -- 应收金额
	      sum(coalesce(case when sdt=${hiveconf:before1_last_mon} then if(receivable_amount>=0,receivable_amount,0) end,0)) as receivable_amount_last_mon,		 -- 上月末应收金额
	      sum(coalesce(case when sdt=${hiveconf:current_day} then if(receivable_amount>=0,receivable_amount,0) end,0))-
          sum(coalesce(case when sdt=${hiveconf:before1_last_mon} then if(receivable_amount>=0,receivable_amount,0) end,0)) as capital_takes_up,      -- 资金占用费
	      0 transport_amount      -- 运费
	    from csx_dw.dws_sss_r_a_customer_accounts
	    where sdt=${hiveconf:before1_last_mon} or sdt=${hiveconf:current_day}
        group by customer_no,company_code
        --运费
        union all
        select 
          a2.customer_no,b2.company_code,
	      0 as receivable_amount,		 -- 应收金额
	      0 as receivable_amount_last_mon,		 -- 上月末应收金额
	      0 as capital_takes_up,      -- 资金占用费
	      a2.transport_amount      -- 运费	  
        from
         (
           select 
             customer_code as customer_no,dc_code,
             sum(transport_amount) transport_amount      -- 运费
           from csx_dw.dws_tms_r_d_entrucking_order_detail
           where sdt>=${hiveconf:current_start_mon}
           and regexp_replace(send_date,'-','')>=${hiveconf:current_start_mon} and regexp_replace(send_date,'-','')<=${hiveconf:current_day}
           group by customer_code,dc_code
           )a2 
         left join
         ( -- 获取公司编码、公司名称
           select
             shop_id,shop_name,company_code,company_name
           from csx_dw.dws_basic_w_a_csx_shop_m
           where sdt = 'current'
         )b2 on a2.dc_code = b2.shop_id 
	   )a 
	   group by customer_no,company_code
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
  )b on a1.customer_no = b.customer_no
  left join
  ( -- 获取客户+签约公司的详细信息
    select * from csx_dw.dws_crm_w_a_customer_company
    where sdt = 'current'
  )c on a1.customer_no = c.customer_no and a1.company_code = c.company_code
  left join
  ( -- 获取公司名称
    select
      code, name as company_name
    from csx_dw.dws_basic_w_a_company_code
    where sdt = 'current'
  )d on a1.company_code = d.code 
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
  `receivable_amount_last_mon` decimal(26,6)  COMMENT '上月末应收金额',
  `capital_takes_up` decimal(26,6)  COMMENT '资金占用费',
  `transport_amount` decimal(26,6)  COMMENT '运费',
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


