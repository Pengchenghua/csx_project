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
set month_start_day ='20220501';	
set month_end_day ='20220510';

insert overwrite table csx_tmp.report_sss_r_m_crm_customer_over_rate_detail partition(sdt) 
select
	concat_ws('&',cast(d.customer_id as string),a.company_code,${hiveconf:last_1day}) as biz_id,
	d.region_code,
	d.region_name,
	d.province_code,
	d.province_name,
	d.city_group_code,
	d.city_group_name,
	'202205' as yearmonth,
	a.channel_code,
	a.channel_name,	-- 渠道
	d.customer_id,	
	a.customer_no,	-- 客户编码
	a.customer_name,	-- 客户名称
	d.sales_id_new as sales_id,
	d.work_no_new as work_no,	-- 销售员工号
	d.sales_name_new as sales_name,	-- 销售员
	d.rp_service_user_id_new as rp_service_user_id,
	d.rp_service_user_work_no_new as rp_service_user_work_no,
	d.rp_service_user_name_new as rp_service_user_name,
	
	d.fl_service_user_id_new as fl_service_user_id,
	d.fl_service_user_work_no_new as fl_service_user_work_no,
	d.fl_service_user_name_new as fl_service_user_name,	
	
	d.bbc_service_user_id_new as bbc_service_user_id,
	d.bbc_service_user_work_no_new as bbc_service_user_work_no,
	d.bbc_service_user_name_new as bbc_service_user_name,
	
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
		sdt,
		customer_code as customer_no,
		--customer_no,
		customer_name,company_code,company_name,channel_code,channel_name,payment_terms,payment_days,payment_name,receivable_amount,overdue_amount,max_overdue_day,
		overdue_coefficient_numerator, -- 逾期金额*逾期天数 计算因子，用于计算逾期系数分子
		overdue_coefficient_denominator -- 应收金额*账期天数 计算因子，用于计算逾期系数分母
	from
		csx_dw.dws_sss_r_d_customer_settle_detail
	where
		sdt=${hiveconf:month_end_day}
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
			and customer_no not in('123244')
		)e on e.customer_no=a.customer_no
	--关联客户对应销售员与服务管家
	left join		
		(  
		select 
			distinct customer_id,customer_no,customer_name,sales_id_new,work_no_new,sales_name_new,
			rp_service_user_id_new,rp_service_user_work_no_new,rp_service_user_name_new,
			fl_service_user_id_new,fl_service_user_work_no_new,fl_service_user_name_new,
			bbc_service_user_id_new,bbc_service_user_work_no_new,bbc_service_user_name_new,
			region_code,region_name,province_code,province_name,city_group_code,city_group_name,
			channel_code,channel_name
		from 
			csx_tmp.report_crm_w_a_customer_service_manager_info_business_new
		where
			month='202205'
		)d on d.customer_no=a.customer_no		
where 
	e.customer_no is null
	and (a.receivable_amount>0 or a.receivable_amount is null)
;