-- ================================================================================================================
-- 销售员新签及断约
-- 近半年：202103-202107
-- 近三月：202105-202107

-- insert overwrite directory '/tmp/zhangyanpeng/20210726_linshi_5' row format delimited fields terminated by '\t' 

set current_day =regexp_replace(date_sub(current_date,1),'-','');

select
	a.prov_name, -- 省份
	a.city_name, -- 城市
	'' as sales_manger, -- 销售经理
	b.name, -- 销售主管
	a.name, -- 销售员
	c.customer_cnt, -- 客户数量
	d.sales_this_month_customer_cnt, -- 本月履约客户数
	d.sales_value, -- 本月销售额
	d.profit, -- 本月毛利额
	d.profit_rate, -- 本月定价毛利率
	e.overdue_amount, -- 逾期金额
	c.customer_cnt_2, -- 近半年新签客户数
	d.sign_sale_six_months, -- 近半年新签并履约客户
	d.sign_sales_value_six_months, -- 近半年新签销售金额
	d.sign_sale_three_months, -- 近三月新签并履约客户
	d.sign_sales_value_three_months, -- 近三月新签销售金额
	d.sign_sale_this_months, -- 本月新签并履约客户
	f.break_six_months, -- 近半年断约客户数
	f.break_three_months, -- 近三月断约客户数
	f.break_this_month -- 本月断约客户数
from	
	(
	select 
		id,leader_id,user_number,name,user_position,channel,user_source_busi,prov_name,city_name,del_flag,status
	from 
		csx_dw.dws_basic_w_a_user
	where 
		sdt ='current'
		and user_position ='SALES'
		and status='0' -- 0:启用  1：禁用
		and del_flag='0' -- 删除标记0正常-1删除
		and prov_name not like '%平台%'
	) a
	left join
		(
		select 
			id,user_number,name,user_position
		from 
			csx_dw.dws_basic_w_a_user
		where 
			sdt = 'current'
		) b on b.id = a.leader_id
	left join
		( -- 客户数量、近半年新签客户数
		select
			sales_id,
			count(distinct customer_no) as customer_cnt, -- 客户数量
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')>='202103' then customer_no else null end) as customer_cnt_2 -- 近半年新签客户数
		from
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
		group by 
			sales_id
		) c on c.sales_id=a.id
	left join
		( -- 本月销售额、定价毛利额、定价毛利率、近半年新签并履约客户、近半年新签销售金额、近三月新签并履约客户、近三月新签销售金额、本月新签并履约客户
		select 
			sales_id,
			sum(case when sdt between '20210701' and ${hiveconf:current_day} then sales_value else 0 end) as sales_value, -- 本月销售额
			sum(case when sdt between '20210701' and ${hiveconf:current_day} then profit else 0 end) as profit, -- 本月定价毛利额
			sum(case when sdt between '20210701' and ${hiveconf:current_day} then profit else 0 end)
				/abs(sum(case when sdt between '20210701' and ${hiveconf:current_day} then sales_value else 0 end)) as profit_rate, -- 本月定价毛利率
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')>='202103' then customer_no else null end) as sign_sale_six_months, -- 近半年新签并履约客户
			sum(case when regexp_replace(substr(sign_time,1,7),'-','')>='202103' then sales_value else 0 end) as sign_sales_value_six_months, -- 近半年新签销售金额
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')>='202105' then customer_no else null end) as sign_sale_three_months, -- 近三月新签并履约客户
			sum(case when regexp_replace(substr(sign_time,1,7),'-','')>='202105' then sales_value else 0 end) as sign_sales_value_three_months, -- 近三月新签销售金额		
			count(distinct case when regexp_replace(substr(sign_time,1,7),'-','')>='202107' then customer_no else null end) as sign_sale_this_months, -- 本月新签并履约客户
			count(distinct case when sdt between '20210701' and ${hiveconf:current_day} then customer_no else null end) as sales_this_month_customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and ${hiveconf:current_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		group by 
			sales_id
		) d on d.sales_id=a.id
	left join
		(
		select --应收逾期
			sales_id,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_day}
		group by 
			sales_id
		) e on e.sales_id=a.id
	left join
		(
		select
			sales_id,
			count(distinct case when after_month between '202103' and '202107' then customer_no else null end) as break_six_months,
			count(distinct case when after_month between '202105' and '202107' then customer_no else null end) as break_three_months,
			count(distinct case when after_month between '202107' and '202107' then customer_no else null end) as break_this_month
		from
			(
			select 
				sales_id,customer_no,
				substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as after_month
			from 
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt between '20190101' and ${hiveconf:current_day}
				and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				and sales_type !='fanli'
			group by 
				sales_id,customer_no
			) tmp1
		group by 
			sales_id
		) f on f.sales_id=a.id


-- ================================================================================================================
-- 销售员客户具体情况
-- 近半年：202103-202107
-- 近三月：202105-202107

set current_day =regexp_replace(date_sub(current_date,1),'-','');

insert overwrite directory '/tmp/zhangyanpeng/20210727_linshi_1' row format delimited fields terminated by '\t' 

select
	a.sales_province_name,
	a.sales_city_name,
	a.second_supervisor_name,
	a.first_supervisor_name,
	a.sales_name,
	a.customer_no,
	a.customer_name,
	b.business_type_name,
	a.second_category_name,
	b.days_cnt,
	b.sales_value,
	b.front_profit,
	b.profit,
	c.overdue_amount
from
	(
	select
		sales_province_name,sales_city_name,second_supervisor_name,first_supervisor_name,sales_id,sales_name,customer_no,customer_name,second_category_name
	from
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = 'current'	
		and sales_province_name not like '%平台%'
		and sales_province_name not like '%BBC%'		
	) a
	left join
		(
		select 
			customer_no,business_type_name,
			count(distinct sdt) as days_cnt,
			sum(sales_value) as sales_value,
			sum(front_profit) as front_profit,
			sum(profit) as profit
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210701' and ${hiveconf:current_day}
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)	
		group by 
			customer_no,business_type_name
		) b on b.customer_no=a.customer_no
	left join
		(
		select --应收逾期
			customer_no,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount	-- 逾期金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt=${hiveconf:current_day}
		group by 
			customer_no
		) c on c.customer_no=a.customer_no	
	join
		(
		select 
			id,leader_id,user_number,name,user_position,channel,user_source_busi,prov_name,city_name,del_flag,status
		from 
			csx_dw.dws_basic_w_a_user
		where 
			sdt ='current'
			and user_position ='SALES'
			and status='0' -- 0:启用  1：禁用
			and del_flag='0' -- 删除标记0正常-1删除
			and prov_name not like '%平台%'	
		) d on d.id=a.sales_id
		
		
		
