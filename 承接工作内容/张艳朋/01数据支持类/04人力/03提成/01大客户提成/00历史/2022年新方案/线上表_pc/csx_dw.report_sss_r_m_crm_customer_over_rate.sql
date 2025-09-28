---财务销售结算 crm 销售员销售结算 提成趋势

--SET hive.execution.engine=mr;
--动态分区
SET hive.exec.parallel=true;
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

--中间结果压缩
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
SET mapred.output.compression.type=BLOCK;
SET parquet.compression=SNAPPY;
--启用引号识别
set hive.support.quoted.identifiers=none;
set hive.map.aggr=true;

----昨天
set last_1day=regexp_replace(date_sub(current_date,1),'-','');
set created_time = from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss');
set created_by='zhangyanpeng';
set month_start_day ='20220201';	
set month_end_day ='20220228';

insert overwrite table csx_dw.report_sss_r_m_crm_customer_over_rate partition(sdt) 
select
	concat_ws('&',cast(f.customer_id as string),'202202') as biz_id,
	f.sales_region_code as region_code,
	f.sales_region_name as region_name,
	f.province_code,
	f.province_name,
	f.city_group_code,
	f.city_group_name,
	'202202' as yearmonth,
	a.channel_code,
	a.channel_name,	-- 渠道
	f.customer_id,	
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	f.sales_id,
	d.work_no,	-- 销售员工号
	d.sales_name,	-- 销售员
	g.service_user_id as service_id,
	d.service_user_work_no as service_work_no,
	d.service_user_name as service_name,
	d.is_part_time_service_manager,
	a.payment_terms,	-- 账期编码
	a.payment_days,	-- 帐期天数
	a.payment_name,	-- 账期名称
	a.company_code,	-- 公司代码
	a.company_name,	-- 公司名称,
	a.receivable_amount,	-- 应收金额
	a.overdue_amount,	-- 逾期金额
	overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
	overdue_coefficient_denominator, -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	coalesce(round(case when coalesce(case when a.receivable_amount>=0 then a.receivable_amount else 0 end, 0) <= 1 then 0  
		else coalesce(case when overdue_coefficient_numerator>=0 and a.receivable_amount>0 then overdue_coefficient_numerator else 0 end, 0)
		/(case when overdue_coefficient_denominator>=0 and a.receivable_amount>0 then overdue_coefficient_denominator else 0 end) end, 6),0) as over_rate, -- 逾期系数
	if(receivable_amount>=1,'是','否') as is_greater_0,
	${hiveconf:created_by} as create_by,
	${hiveconf:created_time} as created_time,
	${hiveconf:created_time} as update_time,
	${hiveconf:last_1day} as sdt		
from
	(
	select
		customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from 
		csx_tmp.tc_cust_overdue_0  
	where 
		channel_name = '大客户' 
		and sdt = '20220228'
		--and customer_no in ('105235','105381','105557')
	)a
	--剔除业务代理与内购客户
	join		
		(
		select 
			* 
		from 
			csx_dw.dws_crm_w_a_customer 
		where 
			sdt=${hiveconf:month_end_day} 
			and (channel_code in('1','7','8'))  ----渠道编号-1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 7.企业购 8.其他 9.业务代理
			and (customer_name not like '%内%购%' and customer_name not like '%临保%')	
		)b on b.customer_no=a.customer_no  
	--剔除当月有城市服务商与批发内购业绩的客户逾期系数
	left join 
		(
		select 
			distinct customer_no 
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>=${hiveconf:month_start_day} 
			and sdt<=${hiveconf:month_end_day} 
			and business_type_code in('3','4')
			-- 不剔除城市服务商2.0，按大客户提成方案计算
			and customer_no not in('112207','115023','116959','117817','119262','120294','120939','121276','121298','121472','121625','123244','124219','124473','124498',
			'124601','125161','125284')
		)e on e.customer_no=a.customer_no
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_no,service_user_work_no,service_user_name,	  
			work_no,sales_name,is_part_time_service_manager
		from 
			csx_tmp.tc_customer_service_manager_info_new
		)d on d.customer_no=a.customer_no	
	left join
		(
		select
			customer_id,customer_no,sales_id,work_no,sales_name,sales_region_code,sales_region_name,province_code,province_name,city_group_code,city_group_name
		from
			csx_dw.dws_crm_w_a_customer
		where
			sdt='20220228'
		) f on f.customer_no=a.customer_no	
	left join
		(
		select
			customer_no,sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,
			concat_ws(';', collect_list(cast(service_user_id as string))) as service_user_id,
			concat_ws(';', collect_list(service_user_work_no)) as service_user_work_no,
			concat_ws(';', collect_list(service_user_name)) as service_user_name
		from
			(
			select
				distinct customer_no,service_user_id,service_user_work_no,service_user_name,sales_region_code,sales_region_name,sales_province_code,sales_province_name,
				city_group_code,city_group_name
			from
				csx_dw.dws_crm_w_a_customer_sales_link
			where
				sdt='20220228'
				and is_additional_info = 1 
				and service_user_id <> 0
			) t1
		group by 
			customer_no,sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name
		) g on g.customer_no=a.customer_no		
where 
	e.customer_no is null
	and (a.receivable_amount>0 or a.receivable_amount is null)
;