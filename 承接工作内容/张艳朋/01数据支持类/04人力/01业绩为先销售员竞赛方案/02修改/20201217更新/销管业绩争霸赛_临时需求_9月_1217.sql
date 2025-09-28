
-- 销售员 破冰王 新开客户

set i_sdate ='2020-09-30';

select
	concat_ws('-', from_unixtime(unix_timestamp(trunc(${hiveconf:i_sdate},'MM'), 'yyyy-MM-dd'),'MMdd'),from_unixtime(unix_timestamp(${hiveconf:i_sdate}, 'yyyy-MM-dd'),'MMdd')) as statistic_month,
	province_name,
	city_real,
	supervisor_work_no,
	supervisor_name,
	work_no,
	sales_name,
	sales_value,
	front_profit,
	front_profit_rate,
	cust_count,
	avg_sales_value
from
	(
	select
		province_name, 
		city_real, 
		supervisor_work_no, 
		supervisor_name, 
		work_no, 
		sales_name,
		sum(sales_value) as sales_value, 
		sum(front_profit) as front_profit,
		sum(front_profit) / sum(sales_value) as front_profit_rate,
		count(distinct customer_no) as cust_count,
		sum(avg_sales_value) as avg_sales_value
	from
		(
		select
			b.first_date,
			a.customer_no,
			a.province_name,
			a.city_real,
			a.supervisor_work_no,
			a.supervisor_name, 
			a.work_no, 
			a.sales_name,
			sum(sales_value) as sales_value, 
			sum(front_profit) as front_profit,
			sum(sales_value) / (datediff(${hiveconf:i_sdate}, b.first_date)+1) as avg_sales_value
		from
			(
			select 
				customer_no, 
				province_name, 
				city_real, 
				supervisor_work_no, 
				supervisor_name,
				work_no, 
				sales_name, 
				sales_value, 
				front_profit
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt between regexp_replace(trunc(${hiveconf:i_sdate}, 'MM'), '-', '') and regexp_replace(${hiveconf:i_sdate}, '-', '')
				and sales_type in ('qyg', 'bbc') 
				and channel in ('1', '7')
				and attribute_code in (1, 2) 
				and first_category_code in ('21', '23') 
				and work_no not in ('81022625','80795486','80548045','80796125','80930620','80857686','81020490','80203532','81004930','80927348','80141332','80663108','80796343','80142191','80949208','80980614','80972915','80960666','80816155','80012225','80764642','80952742','80973546','80928572','80895351','80255598','80691224','80680804')
			) a 
			join
				(
				select 
					customer_no, 
					from_unixtime(unix_timestamp(first_order_date,'yyyyMMdd'),'yyyy-MM-dd') as first_date
				from 
					csx_dw.dws_crm_r_a_customer_active_info
				where 
					sdt = regexp_replace(${hiveconf:i_sdate}, '-', '')
					and first_order_date between regexp_replace(trunc(${hiveconf:i_sdate}, 'MM'), '-', '') and regexp_replace(${hiveconf:i_sdate}, '-', '')
				) b on b.customer_no=a.customer_no
		group by
			b.first_date,
			a.customer_no,
			a.province_name,
			a.city_real,
			a.supervisor_work_no,
			a.supervisor_name, 
			a.work_no, 
			a.sales_name
		) tmp1
	group by 
		province_name,
		city_real, 
		supervisor_work_no, 
		supervisor_name, 
		work_no, 
		sales_name
	) tmp2
where 
	front_profit_rate > 0.03 
	and avg_sales_value >= 1000
order by 
	cust_count desc, 
	sales_value desc; 


-- 销售员 增长王 业绩增长

set i_sdate ='2020-09-30';

select
	concat_ws('-', from_unixtime(unix_timestamp(trunc(${hiveconf:i_sdate},'MM'), 'yyyy-MM-dd'),'MMdd'),from_unixtime(unix_timestamp(${hiveconf:i_sdate}, 'yyyy-MM-dd'),'MMdd')) as statistic_month,
	province_name,
	city_real,
	supervisor_work_no,
	supervisor_name,
	work_no,
	sales_name,
	sales_value,
	front_profit,
	front_profit_rate,
	value2,
	profit2 / value2 as profit_rate2,
	month_growth
from
	(
	select
		a.province_name, 
		a.city_real, 
		a.supervisor_work_no, 
		a.supervisor_name, 
		a.work_no, 
		a.sales_name,
		sum(a.sales_value) as sales_value, 
		sum(a.front_profit) as front_profit,
		sum(a.front_profit) / sum(a.sales_value) as front_profit_rate,
		coalesce(sum(b.sales_value), 0) as value2, 
		coalesce(sum(b.front_profit), 0) as profit2,
		sum(a.sales_value) - coalesce(sum(b.sales_value), 0) as month_growth
	from
		(
		select 
			customer_no, 
			province_name, 
			city_real, 
			supervisor_work_no, 
			supervisor_name, 
			work_no, 
			sales_name,
			sum(sales_value) as sales_value, 
			sum(front_profit) as front_profit
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt between regexp_replace(trunc(${hiveconf:i_sdate}, 'MM'), '-', '') and regexp_replace(${hiveconf:i_sdate}, '-', '')
			and sales_type in ('qyg', 'bbc') 
			and channel IN ('1', '7')
			and attribute_code in (1, 2) 
			and first_category_code IN ('21', '23')
			and work_no not in ('81022625','80795486','80548045','80796125','80930620','80857686','81020490','80203532','81004930','80927348','80141332','80663108','80796343','80142191','80949208','80980614','80972915','80960666','80816155','80012225','80764642','80952742','80973546','80928572','80895351','80255598','80691224','80680804')
		group by 
			customer_no, 
			province_name, 
			city_real, 
			supervisor_work_no, 
			supervisor_name, 
			work_no, 
			sales_name
		) a 
		left join
			(
			select 
				customer_no, 
				sum(sales_value) as sales_value, 
				sum(front_profit) as front_profit
			from 
				csx_dw.dws_sale_r_d_customer_sale
			where 
				sdt between regexp_replace(add_months(trunc(${hiveconf:i_sdate},'MM'),-1),'-','') and regexp_replace(add_months(last_day(${hiveconf:i_sdate}),-1),'-','')
				and sales_type in ('qyg', 'bbc') 
				and channel in ('1', '7')
			group by 
				customer_no
			) b on a.customer_no = b.customer_no
		join
			(
			select 
				customer_no
			from 
				csx_dw.dws_crm_w_a_customer_m_v1
			where
				sdt between regexp_replace(trunc(${hiveconf:i_sdate}, 'MM'), '-', '') and regexp_replace(${hiveconf:i_sdate}, '-', '')
				and position = 'SALES'
			group by 
				customer_no
			) c on a.customer_no = c.customer_no
	group by 
		a.province_name, 
		a.city_real, 
		a.supervisor_work_no, 
		a.supervisor_name, 
		a.work_no, 
		a.sales_name
	) tmp
where 
	month_growth >= 50000 
	and front_profit_rate > 0.05
order by 
	month_growth desc;


-- 销售员 BBC突破 BBC业绩突破

set i_sdate ='2020-09-30';

select
	concat_ws('-', from_unixtime(unix_timestamp(trunc(${hiveconf:i_sdate},'MM'), 'yyyy-MM-dd'),'MMdd'),from_unixtime(unix_timestamp(${hiveconf:i_sdate}, 'yyyy-MM-dd'),'MMdd')) as statistic_month,
	province_name, 
	city_real, 
	supervisor_work_no, 
	supervisor_name, 
	work_no, 
	sales_name,
	sales_value, front_profit, front_profit_rate, cust_count, avg_sales_value
from
	(
	select
		province_name, 
		city_real, 
		supervisor_work_no, 
		supervisor_name, 
		work_no, 
		sales_name,
		sum(sales_value) AS sales_value, 
		sum(front_profit) AS front_profit,
		sum(front_profit) / sum(sales_value) as front_profit_rate, 
		count(distinct customer_no) as cust_count,
		sum(avg_sales_value) as avg_sales_value
	from
		(
		select
			a.first_date, 
			b.customer_no, 
			b.province_name, 
			b.city_real, 
			b.supervisor_work_no,
			b.supervisor_name, 
			b.work_no, 
			b.sales_name,
			sum(sales_value) as sales_value, 
			sum(front_profit) as front_profit,
			sum(sales_value) / (datediff(${hiveconf:i_sdate}, a.first_date)+1) as avg_sales_value
		from
			(
			select 
				customer_no, 
				from_unixtime(unix_timestamp(first_order_date,'yyyyMMdd'),'yyyy-MM-dd') as first_date
			from 
				csx_dw.dws_crm_r_a_customer_active_info
			where 
				sdt = regexp_replace(${hiveconf:i_sdate}, '-', '')
				and first_order_date between regexp_replace(trunc(${hiveconf:i_sdate}, 'MM'), '-', '') and regexp_replace(${hiveconf:i_sdate}, '-', '')
			) a 
			join
				(
				select 
					customer_no, 
					province_name, 
					city_real, 
					supervisor_work_no, 
					supervisor_name,
					work_no, 
					sales_name, 
					sales_value, 
					front_profit
				from 
					csx_dw.dws_sale_r_d_customer_sale
				where 
					sdt between regexp_replace(trunc(${hiveconf:i_sdate}, 'MM'), '-', '') and regexp_replace(${hiveconf:i_sdate}, '-', '')
					and sales_type = 'bbc' 
					and channel = '7'
					and attribute_code in (1, 2) 
					and first_category_code in ('21', '23')
					and work_no not in ('81022625','80795486','80548045','80796125','80930620','80857686','81020490','80203532','81004930','80927348','80141332','80663108','80796343','80142191','80949208','80980614','80972915','80960666','80816155','80012225','80764642','80952742','80973546','80928572','80895351','80255598','80691224','80680804')
				) b on a.customer_no = b.customer_no
			join
				(
				select 
					customer_no
				from 
					csx_dw.dws_crm_w_a_customer_m_v1
				where
					sdt between regexp_replace(trunc(${hiveconf:i_sdate}, 'MM'), '-', '') and regexp_replace(${hiveconf:i_sdate}, '-', '')
					and position = 'SALES'
				group by 
					customer_no
				) c on a.customer_no = c.customer_no
		group by 
			a.first_date, 
			b.customer_no, 
			b.province_name, 
			b.city_real,
			b.supervisor_work_no, 
			b.supervisor_name, 
			b.work_no, 
			b.sales_name
		) tmp
	group by 
		province_name, 
		city_real, 
		supervisor_work_no, 
		supervisor_name, 
		work_no, 
		sales_name
	) tmp2
where 
	front_profit_rate > 0.05 
	and avg_sales_value >= 700
order by 
	cust_count desc, 
	sales_value desc;