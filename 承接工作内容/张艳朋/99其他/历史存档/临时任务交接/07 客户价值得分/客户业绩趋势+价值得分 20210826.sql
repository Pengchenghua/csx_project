--昨日、昨日月1日、上月1日、上月昨日
--select ${hiveconf:current_day},${hiveconf:current_start_mon},${hiveconf:before1_start_mon},${hiveconf:before1_current_mon};
set current_day =regexp_replace(date_sub(current_date,1),'-','');
set current_start_mon =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');
set before1_start_mon=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set before1_current_mon=concat(substr(regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-',''),1,6),
					if(date_sub(current_date,1)=last_day(date_sub(current_date,1))
					,substr(regexp_replace(last_day(add_months(trunc(date_sub(current_date,1),'MM'),-1)),'-',''),7,2)
					,substr(regexp_replace(date_sub(current_date,1),'-',''),7,2)));	

-------临时 明细表1√√√√√√√√√ 本月+近3月，不含合伙人
drop table csx_tmp.tmp_sale_1;
create temporary table csx_tmp.tmp_sale_1
as
select
a.channel_name,
a.province_code,
a.province_name,
a.customer_no,
b.customer_name,
c.label_value,
b.first_category_name,
b.second_category_name,
b.third_category_name,
b.sales_name,
b.sign_date,
d.min_sdt, -- 首次销售日期 例'20190101'
d.max_sdt, -- 最近销售日期
c.score,c.score_last_sales_days,c.score_count_day,c.score_sales_value,c.score_overdue_amount,c.score_overdue_amount_rate,c.score_profit_rate,
c.last_sales_days,c.count_day,c.sum_sales_value,c.overdue_amount,c.overdue_amount_rate,c.profit_rate,
a.H_sales_day,
a.H_sales_value,
if(d.min_sdt<${hiveconf:before1_start_mon}
	and a.M0_sales_day>0 and a.M1_sales_day>0,a.M0_sales_value,0) M0_old_sales_value,
if(d.min_sdt<${hiveconf:before1_start_mon}
	and a.M0_sales_day>0 and a.M1_sales_day>0,a.H_sales_value,0) H_old_sales_value,
a.M0_sales_day,
a.M1_sales_day,
a.M2_sales_day,
a.M3_sales_day,
a.M0_sales_value,
a.M1_sales_value,
a.M2_sales_value,
a.M3_sales_value,
a.M0_profit,
a.M1_profit,
a.M2_profit,
a.M3_profit,
a.M0_profit_rate,
a.M1_profit_rate,
a.M2_profit_rate,
a.M3_profit_rate,
a.M0_new_sale,
a.M1_new_sale,
a.M2_new_sale,
a.M3_new_sale
from (
	select
	a.channel_name,
	--a.channel,
	a.province_code,
	a.province_name,
	a.customer_no,
	--a.customer_name,
	sum(a.H_sales_day) H_sales_day,
	sum(a.H_sales_value) H_sales_value,
	max(if(month_no = 0,a.sales_day,'')) as M0_sales_day,
	max(if(month_no = 0,a.sales_value,'')) as M0_sales_value,
	max(if(month_no = 0,a.profit,'')) as M0_profit,
	max(if(month_no = 0,a.profit_rate,'')) as M0_profit_rate,
	max(if(month_no = 0,a.new_sale,'')) as M0_new_sale,

	max(if(month_no = 1,a.sales_day,'')) as M1_sales_day,
	max(if(month_no = 1,a.sales_value,'')) as M1_sales_value,
	max(if(month_no = 1,a.profit,'')) as M1_profit,
	max(if(month_no = 1,a.profit_rate,'')) as M1_profit_rate,
	max(if(month_no = 1,a.new_sale,'')) as M1_new_sale,

	max(if(month_no = 2,a.sales_day,'')) as M2_sales_day,
	max(if(month_no = 2,a.sales_value,'')) as M2_sales_value,
	max(if(month_no = 2,a.profit,'')) as M2_profit,
	max(if(month_no = 2,a.profit_rate,'')) as M2_profit_rate,
	max(if(month_no = 2,a.new_sale,'')) as M2_new_sale,

	max(if(month_no = 3,a.sales_day,'')) as M3_sales_day,
	max(if(month_no = 3,a.sales_value,'')) as M3_sales_value,
	max(if(month_no = 3,a.profit,'')) as M3_profit,
	max(if(month_no = 3,a.profit_rate,'')) as M3_profit_rate,
	max(if(month_no = 3,a.new_sale,'')) as M3_new_sale
	from(
		select
		a.channel_name,
		a.province_code,
		a.province_name,
		a.customer_no,
		a.s_month,
		months_between(trunc(date_sub(current_date,1),'MM'),from_unixtime(unix_timestamp(a.s_month,'yyyymm'),'yyyy-mm-dd')) as month_no,
		a.sales_day,a.H_sales_day,
		a.sales_value,a.H_sales_value,
		a.profit,
		a.profit_rate,
		if(substr(d.min_sdt,1,6)=a.s_month,1, 0) as new_sale, -- 当月新客
		if(substr(c.sign_date,1,6)=a.s_month,1,0) count_sign -- 成交客户中当月签约
		from
			(
			select
			'大客户' channel_name,
			a.province_code,a.province_name,a.customer_no,
			substr(a.sdt,1,6) s_month,
			count(distinct a.sdt) sales_day,
			count(distinct case when a.sdt>= ${hiveconf:before1_start_mon} and a.sdt<= ${hiveconf:before1_current_mon} then a.sdt end) H_sales_day,
			sum(a.sales_value) sales_value,
			sum(case when a.sdt>= ${hiveconf:before1_start_mon} and a.sdt<= ${hiveconf:before1_current_mon} then a.sales_value end) H_sales_value,
			sum(a.profit) as profit,
			sum(a.profit)/abs(sum(a.sales_value)) as profit_rate
			from csx_dw.dws_sale_r_d_detail a
			where a.sdt>= regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-3),'-','')
			and a.sdt<= ${hiveconf:current_day}
			and a.channel_code in ('1','7','9')--大客户 包含企业购
			and business_type_code<>'4' --不含合伙人
			group by a.province_code,a.province_name,a.customer_no,substr(a.sdt,1,6)
			) a 
		left join 
			(select regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,* 
			from csx_dw.dws_crm_w_a_customer where sdt='current'
			)c on c.customer_no=a.customer_no
		left join (select customer_no,first_order_date as min_sdt from csx_dw.dws_crm_w_a_customer_active where sdt='current'
		           )d on d.customer_no=a.customer_no
	) a
	group by a.channel_name,a.province_code,a.province_name,a.customer_no
) a
left join 
	(select * from csx_dw.sale_r_d_customer_score where sdt=${hiveconf:current_day} )c on c.customer_no=a.customer_no -- 客户价值
left join --客户信息
	(select regexp_replace(split(first_sign_time, ' ')[0], '-', '') as sign_date,* 
	from csx_dw.dws_crm_w_a_customer where sdt='current'
	)b on b.customer_no=a.customer_no
left join 
	(select customer_no,first_order_date as min_sdt,last_order_date as max_sdt 
	from csx_dw.dws_crm_w_a_customer_active where sdt='current'
	)d on d.customer_no=a.customer_no
;


-- 结果表2-明细-all
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select
a.channel_name,
a.province_code,
a.province_name,
a.city_name,
a.customer_no,
a.customer_name,
d.first_category_name,
d.second_category_name,
d.third_category_name,
d.sales_name,
d.sign_date,
d.min_sdt, -- 首次销售日期 例'20190101'
d.max_sdt, --最近销售日期
--新客标识
case when datediff(date_sub(current_date,1),to_date(from_unixtime(UNIX_TIMESTAMP(d.min_sdt,'yyyyMMdd'))))<30 then '新客30天内'
	 when datediff(date_sub(current_date,1),to_date(from_unixtime(UNIX_TIMESTAMP(d.min_sdt,'yyyyMMdd'))))<60 then '新客60天内'
	 when datediff(date_sub(current_date,1),to_date(from_unixtime(UNIX_TIMESTAMP(d.min_sdt,'yyyyMMdd'))))<90 then '新客90天内'
	 else '正常' end is_new,
a.sales_day,
d.H_sales_day,
a.sales_value/a.sales_day as avg_sales_value,
a.sku,
a.sales_value,
d.H_sales_value,
a.profit,
a.profit_rate,
a.return_value,
d.score,
d.score_last_sales_days,d.score_count_day,d.score_sales_value,d.score_overdue_amount,d.score_overdue_amount_rate,d.score_profit_rate,
d.last_sales_days,d.count_day,d.sum_sales_value,d.overdue_amount,d.overdue_amount_rate,d.profit_rate profit_rate1,

if(M0_sales_day/int(substr(${hiveconf:current_day},7,2))* int(substr(regexp_replace(last_day(date_sub(current_date,1)),'-',''),7,2))
	<(if(d.M1_sales_day>=0,d.M1_sales_day,0)+if(d.M2_sales_day>0,d.M2_sales_day,0)+if(d.M3_sales_day>0,d.M3_sales_day,0))
	/(if(d.M1_sales_day>0,1,0)+if(d.M2_sales_day>0,1,0)+if(d.M3_sales_day>0,1,0)),0,1) M_sales_day,
	
if(M0_sales_value/int(substr(${hiveconf:current_day},7,2))* int(substr(regexp_replace(last_day(date_sub(current_date,1)),'-',''),7,2))
	<(if(length(d.M1_sales_value)>0,d.M1_sales_value,0)+if(length(d.M2_sales_value)>0,d.M2_sales_value,0)+if(length(d.M3_sales_value)>0,d.M3_sales_value,0))
	/(if(length(d.M1_sales_value)>0,1,0)+if(length(d.M2_sales_value)>0,1,0)+if(length(d.M3_sales_value)>0,1,0))
	,0,1) M_sales_value,
	
if((M0_profit/int(substr(${hiveconf:current_day},7,2))* int(substr(regexp_replace(last_day(date_sub(current_date,1)),'-',''),7,2)))
	<(if(length(d.M1_profit)>0,d.M1_profit,0)+if(length(d.M2_profit)>0,d.M2_profit,0)+if(length(d.M3_profit)>0,d.M3_profit,0))
	/(if(length(d.M1_profit)>0,1,0)+if(length(d.M2_profit)>0,1,0)+if(length(d.M3_profit)>0,1,0))
	,0,1) M_profit,
	
if(M0_profit_rate<
	(if(length(d.M1_profit_rate)>0,d.M1_profit_rate,0)+if(length(d.M2_profit_rate)>0,d.M2_profit_rate,0)+if(length(d.M3_profit_rate)>0,d.M3_profit_rate,0))
	/(if(length(d.M1_profit_rate)>0,1,0)+if(length(d.M2_profit_rate)>0,1,0)+if(length(d.M3_profit_rate)>0,1,0))
	,0,1) M_profit_rate,

d.M0_sales_day,
d.M1_sales_day,
d.M2_sales_day,
d.M3_sales_day,
d.M0_sales_value,
d.M1_sales_value,
d.M2_sales_value,
d.M3_sales_value,
d.M0_profit,
d.M1_profit,
d.M2_profit,
d.M3_profit,
d.M0_profit_rate,
d.M1_profit_rate,
d.M2_profit_rate,
d.M3_profit_rate,
${hiveconf:current_day} sdt
from
(
	select
		'大客户' channel_name,
		province_code,
		province_name,
		city_name,
		customer_no,
		customer_name,
		count(distinct sdt) sales_day,
		sum(sales_value) sales_value,
		count(distinct goods_code) sku,
		sum(profit) as profit,
		sum(profit)/sum(sales_value) as profit_rate,
		sum(if(return_flag='X',sales_value,0)) return_value
	from csx_dw.dws_sale_r_d_detail
	where sdt>= ${hiveconf:current_start_mon}
	and channel_code in ('1','7','9')
	group by province_code,province_name,city_name,customer_no,customer_name
) a 
inner join csx_tmp.tmp_sale_1 d on d.customer_no=a.customer_no
where d.M0_sales_day<>'';


