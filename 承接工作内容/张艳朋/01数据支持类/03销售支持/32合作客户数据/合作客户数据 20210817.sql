select
	a.sales_region_name,
	a.province_name,
	a.city_group_name,
	a.customer_no,
	a.customer_name,
	a.attribute_desc,
	a.sales_name,
	a.business_type_name,
	b.total_value
from	
	(
	select 
		customer_no,customer_name,attribute_desc,work_no,sales_name,sales_region_name,province_name,city_group_name,business_type_name
	from 
		csx_dw.dws_crm_w_a_customer
	where 
		sdt = '20210816'
		and customer_no<>''	
		and is_cooperative_customer='1' --是否合作客户(0.否，1.是)
	) a
	left join
		(	
		select
			customer_no,customer_name,total_value
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt = '20210816' 
		) b on b.customer_no=a.customer_no
		
		
		
	select 
		source_bill_no as order_no,	-- 来源单号
		customer_code as customer_no,	-- 客户编码
		--customer_name,	-- 客户名称
		appoint_place_code,  --履约地点编码
		company_code,	-- 签约公司编码
		--company_name,	-- 签约公司名称
		happen_date,	-- 发生时间		
		overdue_date,	-- 逾期时间	
		source_statement_amount,	-- 源单据对账金额
		money_back_status,	-- 回款状态
		unpaid_amount,	-- 未回款金额
		account_period_code,	--账期编码 
		account_period_name,	--账期名称 
		account_period_val,	--账期值
		'否' as beginning_mark,	--是否期初
		bad_debt_amount
		--if((money_back_status<>'ALL' or (datediff(${hiveconf:i_sdate_1}, overdue_date)+1)>=1),datediff(${hiveconf:i_sdate_1}, overdue_date)+1,0) as over_days	-- 逾期天数
	--from csx_ods.source_sss_r_d_source_bill
	from csx_dw.dwd_sss_r_d_sale_order_statement_detail_20201116  --销售单对账
	where sdt='20210731'
	and date(happen_date)<='20210731'
	and customer_code='117103'
	--and beginning_mark='1'  	-- 期初标识 0-是 1-否
	--and money_back_status<>'ALL'
	
	

	
新客  长期客户 --当月新客
老客  长期客户 --当月老客
首月  长期客户 --首月非一次性客户
其他  长期客户 --当月老客	
   
新客  一次性客户 --当月老客
老客  一次性客户 --当月老客
首月  一次性客户 --一次性客户
其他  一次性客户 --当月老客
	

select 
	a.smonth,
	sum(a.excluding_tax_sales)excluding_tax_sales
from
	(	
	select
		customer_no,substr(sdt,1,6) smonth,sum(excluding_tax_sales)excluding_tax_sales
	from
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt>='20190101' and sdt<='20210731' 
		and channel_code in('1','7','9')
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		--and province_name='北京市'
	group by 
		customer_no,substr(sdt,1,6)
	) a 
	join
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,cooperation_mode_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			--and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
			and sales_province_name in ('吉林省')
		)c on c.customer_no=a.customer_no
group by 
	a.smonth
	
;	
	
	
第一步，将日期向下偏移一位，即lag_date表示fdate对应的前一天。
select 
	fdate,fuser_id,fis_sign_in, 
	datediff(fdate,lag(fdate) over(partition by fuser_id order by fdate)) diff 
from 
	t_user_attendence 
where 
	fis_sign_in=1
	
第二步 计算结果
select
	a.fuser_id,
	sum(case when a.diff=1 then 1 else 0 end)+1 '连续次数'
from
	(
	select 
		fdate,fuser_id,lag(fdate) over(partition by fuser_id order by fdate) lag_date,
		fis_sign_in, datediff(fdate,lag(fdate) over(partition by fuser_id order by fdate)) diff 
	from 
		t_user_attendence 
	where 
		fis_sign_in=1
	)a
group by 
	a.fuser_id, a.diff


	
select
	t.fuser_id,
	max(t.连续次数) '最大连续次数'
from
	(
	select
		a.fuser_id,
		sum(case when a.diff=1 then 1 else 0 end)+1 '连续次数'
	from
		(
		select 
			fdate, 
			fuser_id, 
			lag(fdate) over(partition by fuser_id order by fdate) lag_date,
			fis_sign_in, datediff(fdate,lag(fdate) over(partition by fuser_id order by fdate)) diff 
		from 
			t_user_attendence 
		where 
			fis_sign_in=1
		)a
	group by 
		a.fuser_id, a.diff
	)t
group by 
	t.fuser_id
	
在这里插入代码片
	
	
	
	
	
	
	
#第一步，计算用户最初签到日期
select 
	fuser_id ,
	min(fdate) '最开始签到日期'
from 
	t_user_attendence 
where 
	fis_sign_in=1 
group by 1

#第二步，计算用户最大断签日期
select 
	fuser_id ,
	max(fdate) '最大断签日期'
from 
	t_user_attendence 
where 
	fis_sign_in=0 #(and fdate<='2021/6/4' 如果不是求当前（2021/6/6），而是求之前某一日期需在此处限制日期）
group by 1

#第三步，结果查询
select 
	fuser_id,
	fcontinuous_days 
from
	(
	select
		a.fuser_id,
		case when c.最大断签日期 IS NULL THEN
		  datediff( '2021/6/6', b.最开始签到日期 )+ 1 ELSE datediff( '2021/6/6', c.最大断签日期 ) 
		END AS fcontinuous_days 
	FROM
		t_user_attendence a
		LEFT JOIN ( SELECT fuser_id, min( fdate ) '最开始签到日期' FROM t_user_attendence WHERE fis_sign_in = 1 GROUP BY 1 ) b 
			ON a.fuser_id = b.fuser_id
		LEFT JOIN ( SELECT fuser_id, max( fdate ) '最大断签日期' FROM t_user_attendence WHERE fis_sign_in = 0 GROUP BY 1 ) c 
			ON a.fuser_id = c.fuser_id 
	WHERE 
		a.fdate = '2021/6/6'
	)t
where 
	t.fcontinuous_days !=0


			