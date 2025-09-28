--教育行业客户增长数，销售额增长绝对值占比

-- 昨日、昨日月1日， 上月同日，上月1日，上月最后一日
set i_sdate_11 =regexp_replace(date_sub(current_date,1),'-','');

set i_sdate_12 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),0),'-','');

set i_sdate_21 =concat(substr(regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-',''),1,6),
					if(date_sub(current_date,1)=last_day(date_sub(current_date,1))
					,substr(regexp_replace(last_day(add_months(trunc(date_sub(current_date,1),'MM'),-1)),'-',''),7,2)
					,substr(regexp_replace(date_sub(current_date,1),'-',''),7,2)));	
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
					
set i_sdate_23 =regexp_replace(last_day(add_months(date_sub(current_date,1),-1)),'-','');


-- 客户销售结果

--insert overwrite directory '/tmp/zhangyanpeng/linshi01' row format delimited fields terminated by '\t'
select 
	a.province_code as `省份代码`,
	a.province_name as `省份名称`,
	a.city_group_code as `城市组编号`,
	a.city_group_name as `城市组`,
	a.channel as `渠道编号`,
	a.channel_name as `渠道名称`,
	a.customer_no as `客户编号`,
	a.customer_name as `客户名称`,
	d.first_category as `一级分类`,
	d.second_category as `二级分类`,
	d.third_category as `三级分类`,
	b.min_sdt as `首单日期`,
	if(c.customer_no is not null,'是','否') as `是否单笔金额大于等于2万客户`,
	a.sales_value as `销售额`,
	a.profit as `毛利额`,
	a.front_profit as `前端毛利额`
	
from
	(
	select	
		province_code,
		province_name,
		city_group_code,
		city_group_name,
		channel,
		channel_name,
		customer_no,
		customer_name,
		sum(sales_value)sales_value,
		sum(profit) as profit,
		sum(front_profit) as front_profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		sdt between '20200701' and ${hiveconf:i_sdate_11}
		and sales_type in ('sapqyg','sapgc','qyg','sc','bbc') 
		and province_name not like '平台%'
		and channel in('1','7')
		and (city_group_name ='福州市,宁德市,三明市' or province_name='安徽省')
	group by 
		province_code,province_name,city_group_code,city_group_name,channel,channel_name,customer_no,customer_name
	) as a
	left join 
		(
		select
			customer_no,min(sdt) as min_sdt
		from 
			(
			select 
				customer_no,sdt,sales_value 
			from 
				csx_dw.sale_item_m 
			where 
				sdt>='20180101' 
				and sdt<'20190101' 
				and sales_type in('qyg','sapqyg','sapgc','sc','bbc','gc','anhui') 
			union all 
			select 
				customer_no,sdt,sales_value 
			from 
				csx_dw.dws_sale_r_d_customer_sale 
			where 
				sdt between '20190101' and ${hiveconf:i_sdate_11} 
				and sales_type in('qyg','sapqyg','sapgc','sc','bbc')
				and (order_no not in ('OC200529000043','OC200529000044','OC200529000045','OC200529000046') or order_no is null)
			) a1
		group by 
			customer_no 
		) as b on b.customer_no=a.customer_no
	left join 
		(
		select
			customer_no
		from 
			(
			select 
				customer_no,sum(sales_value) as sales_value
			from 
				csx_dw.dws_sale_r_d_customer_sale 
			where 
				sdt between '20200701' and ${hiveconf:i_sdate_11}
				and sales_type in ('sapqyg','sapgc','qyg','sc','bbc')
			group by
				customer_no
			having
				sum(sales_value)>=20000
			) a2
		group by 
			customer_no 
		) as c on c.customer_no=a.customer_no
	left join 
		(
		select 
			customer_no,
			customer_name,
			first_category,
			second_category,
			third_category
		from 
			csx_dw.dws_crm_w_a_customer_m_v1
		where 
			sdt = ${hiveconf:i_sdate_11} --昨日
			and customer_no<>''
		) as d on d.customer_no=a.customer_no
where
	b.min_sdt between '20200701' and ${hiveconf:i_sdate_11} --首次下单日期在q3季度
	--and c.customer_no is not null --单笔订单金额大于等于2万的客户
	--and d.second_category='教育' --教育行业
--group by 
--	a.province_name,a.city_group_name,a.channel_name;