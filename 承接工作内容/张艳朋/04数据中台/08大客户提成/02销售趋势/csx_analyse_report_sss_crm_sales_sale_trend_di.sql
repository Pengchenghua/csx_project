-- 动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;


with tmp_report_sss_r_d_crm_sales_sale_trend_1 as
(
select
	a.*,
	a2.customer_name,
	a2.sales_user_number,
	a2.sales_user_name,
	a2.sales_user_id
from
	(
	select 
		sdt,substr(sdt,1,6) smonth,
		customer_code,
		-- 各类型销售额
		sum(sale_amt) as sale_amt, 
		sum(case when business_type_code in('1') then sale_amt else 0 end) as ripei_sale_amt,
		sum(case when business_type_code in('6') then sale_amt else 0 end) as bbc_sale_amt,
		sum(case when business_type_code in('1','6') then sale_amt else 0 end) as ripei_bbc_sale_amt,
		sum(case when business_type_code in('2') then sale_amt else 0 end) as fuli_sale_amt,
		-- 各类型定价毛利额
		sum(profit) as profit,
		sum(case when business_type_code in('1') then profit else 0 end) as ripei_profit,
		sum(case when business_type_code in('6') then profit else 0 end) as bbc_profit,		
		sum(case when business_type_code in('1','6') then profit else 0 end) as ripei_bbc_profit,
		sum(case when business_type_code in('2') then profit else 0 end) as fuli_profit,
		-- 各类型退货金额
		sum(case when refund_order_flag='X' then sale_amt else 0 end) as refund_sale_amt,
		sum(case when business_type_code in('1') and refund_order_flag='X' then sale_amt else 0 end) as refund_ripei_sale_amt,
		sum(case when business_type_code in('6') and refund_order_flag='X' then sale_amt else 0 end) as refund_bbc_sale_amt,
		sum(case when business_type_code in('1','6') and refund_order_flag='X' then sale_amt else 0 end) as refund_ripei_bbc_sale_amt,
		sum(case when business_type_code in('2') and refund_order_flag='X' then sale_amt else 0 end) as refund_fuli_sale_amt	
	from 
		-- csx_dw.dws_sale_r_d_detail
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>=regexp_replace(trunc('${ytd_date}', 'YEAR'),'-','') and sdt<='${ytd}'
		and channel_code in('1','7','9')
		and goods_code not in ('8718','8708','8649') --202112月签呈，剔除飞天茅台酒销售额及定价毛利额，每月,'8718','8708','8649'
		and business_type_code in('1','2','6')
	group by 
		sdt,substr(sdt,1,6),customer_code
	)a
	join
		(
		select 
			substr(sdt,1,6) as smonth,customer_code,customer_name,sales_user_number,sales_user_name,sales_user_id
		from 
			-- csx_dw.dws_crm_w_a_customer 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt>=regexp_replace(trunc('${ytd_date}', 'YEAR'),'-','')
			and sdt<='${ytd}'
			and sdt=if(substr(sdt,1,6)=substr('${ytd}',1,6),'${ytd}',
					regexp_replace(last_day(to_date(from_unixtime(unix_timestamp(if(sdt='current',null,sdt),'yyyyMMdd')))),'-','')
					)
		)a2 on a.customer_code=a2.customer_code and a.smonth=a2.smonth
)


insert overwrite table csx_analyse.csx_analyse_report_sss_crm_sales_sale_trend_di partition(sdt)

select
	concat_ws('&',cast(sales_user_id as string),sdt) as biz_id,
	sales_user_id,
	sales_user_number,
	sales_user_name,
	sdt as sale_date,
	sum(sale_amt) as sale_amt,
	sum(ripei_sale_amt) as ripei_sale_amt,
	sum(bbc_sale_amt)as bbc_sale_amt,
	sum(ripei_bbc_sale_amt) as ripei_bbc_sale_amt,
	sum(fuli_sale_amt) as fuli_sale_amt,
	sum(profit) as profit,
	sum(ripei_profit) as ripei_profit,
	sum(bbc_profit) as bbc_profit,
	sum(ripei_bbc_profit) as ripei_bbc_profit,
	sum(fuli_profit) as fuli_profit,
	sum(refund_sale_amt) as refund_sale_amt,
	sum(refund_ripei_sale_amt) as refund_ripei_sale_amt,
	sum(refund_bbc_sale_amt) as refund_bbc_sale_amt,
	sum(refund_ripei_bbc_sale_amt) as refund_ripei_bbc_sale_amt,
	sum(refund_fuli_sale_amt) as refund_fuli_sale_amt,
	substr(sdt,1,6) as smonth,
	from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') as updated_time,
	sdt
from
	tmp_report_sss_r_d_crm_sales_sale_trend_1
group by 
	sales_user_id,
	sales_user_number,
	sales_user_name,
	sdt	
;

/*
create table csx_analyse.csx_analyse_report_sss_crm_sales_sale_trend_di(
`biz_id`                         string              COMMENT    '业务主键',
`sales_user_id`                  string              COMMENT    '销售员id',
`sales_user_number`              string              COMMENT    '销售员工号',
`sales_user_name`                string              COMMENT    '销售员名称',
`sale_date`                      string              COMMENT    '销售日期',
`sale_amt`                       decimal(20,6)       COMMENT    '销售额',
`ripei_sale_amt`                 decimal(20,6)       COMMENT    '日配销售额',
`bbc_sale_amt`                   decimal(20,6)       COMMENT    'BBC销售额',
`ripei_bbc_sale_amt`             decimal(20,6)       COMMENT    '日配&BBC销售额',
`fuli_sale_amt`                  decimal(20,6)       COMMENT    '福利销售额',
`profit`                         decimal(20,6)       COMMENT    '定价毛利额',
`ripei_profit`                   decimal(20,6)       COMMENT    '日配定价毛利额',
`bbc_profit`                     decimal(20,6)       COMMENT    'BBC定价毛利额',
`ripei_bbc_profit`               decimal(20,6)       COMMENT    '日配&BBC定价毛利额',
`fuli_profit`                    decimal(20,6)       COMMENT    '福利定价毛利额',
`refund_sale_amt`                decimal(20,6)       COMMENT    '退货金额',
`refund_ripei_sale_amt`          decimal(20,6)       COMMENT    '日配退货金额',
`refund_bbc_sale_amt`            decimal(20,6)       COMMENT    'BBC退货金额',
`refund_ripei_bbc_sale_amt`      decimal(20,6)       COMMENT    '日配&BBC退货金额',
`refund_fuli_sale_amt`           decimal(20,6)       COMMENT    '福利退货金额',
`smonth`                         string              COMMENT    '年月',
`updated_time`                   string              COMMENT    '更新时间'

) COMMENT '业务员销售额趋势表'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

*/	