--============================================================================================================================================================
-- 华西大区销售数据
insert overwrite directory '/tmp/zhangyanpeng/20210414_linshi_1' row format delimited fields terminated by '\t' 
select
	c.region_name,
	c.province_name,
	a.s_sdt,
	a.customer_no,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	d.first_order_date,
	a.business_type_name,
	case when d.first_order_date between '20200101' and '20210413' then '是' else '否' end as is_new,
	sum(sales_value) as sales_value,
	sum(profit) as profit,
	sum(front_profit) as front_profit,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	count(distinct a.sdt) as days_cnt
from
	(
	select
		region_code,province_code,substr(sdt,1,6) as s_sdt,customer_no,business_type_name,sales_value,profit,front_profit,excluding_tax_sales,excluding_tax_profit,sdt
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20200101' 
		and sdt <= '20210413'
		and channel_code in ('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and region_name='华西大区'
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		group by 
			customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		) b on a.customer_no=b.customer_no
	left join 
		(
		select 
			province_code,province_name,region_code,region_name
		from 
			csx_dw.dws_sale_w_a_area_belong
		group by 
			province_code,province_name,region_code,region_name
		) c on c.province_code=a.province_code	
	left join 
		(
		select 
			customer_no,customer_name,first_order_date
		from 
			csx_dw.dws_crm_w_a_customer_active
		where 
			sdt='current'
		group by 
			customer_no,customer_name,first_order_date
		) d on d.customer_no=a.customer_no	
group by 
	c.region_name,
	c.province_name,
	a.s_sdt,
	a.customer_no,
	b.customer_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	d.first_order_date,
	a.business_type_name,
	case when d.first_order_date between '20200101' and '20210413' then '是' else '否' end 	
		
		
		
-- 20年1季度和20年4季度如上业务销售金额，按月统计
select
	a.s_sdt,
	a.province_name,
	-- a.customer_no,
	-- b.customer_name,
	a.business_type_name,
	a.sales_value
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		province_name,
		business_type_name,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20201001' 
		and sdt <= '20201231'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),
		province_name,
		business_type_name
	) as a
	-- left join 
	-- 	(
	-- 	select 
	-- 		customer_no,customer_name,work_no,sales_name,regexp_replace(split(sign_time, ' ')[0], '-', '') as sign_date
	-- 	from 
	-- 		csx_dw.dws_crm_w_a_customer
	-- 	where 
	-- 		sdt='current'
	-- 	) b on a.customer_no=b.customer_no
	

--============================================================================================================================================================
-- 二、1、2、3月各省区新签约客户数量（按照新签约并履约的，需区分业务类型）、销售金额（按照新签约并履约的，需区分业务类型），20年1季度和20年4季度各省区新签约客户数量(有履约)，按月统计；

select
	a.sign_date,
	a.province_name,
	a.business_type_name,
	count(distinct a.customer_no) as customer_cnt,
	sum(sales_value) as sales_value
from
	(
	select
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6) as sign_date,
		province_name,
		customer_no,
		business_type_name,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		regexp_replace(split_part(sign_time, ' ',1), '-', '') >= '20210101' 
		and regexp_replace(split_part(sign_time, ' ',1), '-', '') <= '20210331'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6),
		province_name,
		customer_no,
		business_type_name
	) as a 
	join
		(
		select
			customer_no,first_order_date,last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
			and first_order_date between '20210101' and '20210331'
		group by
			customer_no,first_order_date,last_order_date
		) as b on b.customer_no=a.customer_no
group by 	
	a.sign_date,
	a.province_name,
	a.business_type_name
	
	
-- 20年1季度和20年4季度各省区新签约客户数量(有履约)，按月统计；
select
	a.sign_date,
	a.province_name,
	count(distinct a.customer_no) as customer_cnt
from
	(
	select
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6) as sign_date,
		province_name,
		customer_no
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		regexp_replace(split_part(sign_time, ' ',1), '-', '') >= '20201001' 
		and regexp_replace(split_part(sign_time, ' ',1), '-', '') <= '20201231'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6),
		province_name,
		customer_no
	) as a 
	join
		(
		select
			customer_no,first_order_date,last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
			and first_order_date between '20201001' and '20201231'
		group by
			customer_no,first_order_date,last_order_date
		) as b on b.customer_no=a.customer_no
group by 	
	a.sign_date,
	a.province_name	


--============================================================================================================================================================
-- 三、1、2、3月各省区销售金额（客户 按一级分类字段区分）、新增客户的行业分类数量、销售额；	
select
	substr(sdt,1,6) as s_sdt,
	province_name,
	second_category_name,
	sum(sales_value) as sales_value
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt >= '20210101' 
	and sdt <= '20210331'
	and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
group by 
	substr(sdt,1,6),
	province_name,
	second_category_name
--新增客户的行业分类数量、销售额；
select
	a.s_sdt,
	a.province_name,
	a.second_category_name,
	count(distinct a.customer_no) as customer_cnt,
	sum(sales_value) as sales_value
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		province_name,
		customer_no,
		second_category_name,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' 
		and sdt <= '20210331'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),
		province_name,
		customer_no,
		second_category_name
	) as a 
	join
		(
		select
			customer_no,first_order_date,last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
			and first_order_date between '20210101' and '20210331'
		group by
			customer_no,first_order_date,last_order_date
		) as b on b.customer_no=a.customer_no
group by 	
	a.s_sdt,
	a.province_name,
	a.second_category_name


--============================================================================================================================================================
-- 四、1、2、3月各省区各客户下日配单天数统计（如A客户1月下单1天，2月下单1天，3月下单1天）；
insert overwrite directory '/tmp/zhangyanpeng/20210406_linshi_2' row format delimited fields terminated by '\t' 
select
	a.s_sdt,
	a.province_name,
	a.customer_no,
	b.customer_name,
	count(distinct a.sdt) as days_cnt
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		province_name,
		customer_no,
		sdt,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' 
		and sdt <= '20210331'
		and business_type_code in ('1') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),
		province_name,
		customer_no,
		sdt
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		) b on a.customer_no=b.customer_no
group by 
	a.s_sdt,
	a.province_name,
	a.customer_no,
	b.customer_name	
		
		
--===================================================================================================================================================================
-- 五、1、2、3月各省区客户购买品类数（按管理大类一级计算）；
insert overwrite directory '/tmp/zhangyanpeng/20210406_linshi_3' row format delimited fields terminated by '\t' 
select
	a.s_sdt,
	a.province_name,
	a.customer_no,
	b.customer_name,
	count(distinct a.classify_large_code) as cnt
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		province_name,
		customer_no,
		classify_large_code,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' 
		and sdt <= '20210331'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),
		province_name,
		customer_no,
		classify_large_code
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		) b on a.customer_no=b.customer_no
group by 
	a.s_sdt,
	a.province_name,
	a.customer_no,
	b.customer_name	
		

--===================================================================================================================================================================
-- 六、1、2、3月客户断约情况（参见之前做过的表格）；
-- 按大区和省份统计总下单客户、新增下单客户、断约客户
	select
		a.region_name,
		a.province_name,
		a.s_sdt,
		a.customer_cnt,
		coalesce(b.add_customer_cnt,0) as add_customer_cnt,
		coalesce(c.sub_customer_cnt,0) as sub_customer_cnt
	from
		( --下单客户数
		select 
			region_name,province_name,substr(sdt,1,6) as s_sdt,count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			region_name,province_name,substr(sdt,1,6)
		) a
		left join -- 新增客户
			(
			select
				region_name,province_name,substr(min_sdt,1,6) as s_sdt,count(distinct customer_no) as add_customer_cnt
			from
				(
				select 
					region_name,province_name,customer_no,
					min(sdt) as min_sdt
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				min_sdt>='20210101'
				and min_sdt<='20210331'	
			group by 
				region_name,province_name,substr(min_sdt,1,6)
			) b on b.province_name=a.province_name and b.s_sdt=a.s_sdt
		left join
			(
			select
				region_name,province_name,s_sdt,count(distinct customer_no) as sub_customer_cnt
			from
				(
				select 
					region_name,province_name,customer_no,
					substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as s_sdt
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				s_sdt>='202101'
				and s_sdt<='202103'
			group by 
				region_name,province_name,s_sdt
			) c on c.province_name=a.province_name and c.s_sdt=a.s_sdt
			
			
-- 明细
select
	a.region_name,
	a.province_name,
	a.customer_no,
	b.customer_name,
	a.add_day,
	b.work_no,
	b.sales_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	coalesce(a.sales_value,0) as sales_value,
	coalesce(a.profit,0) as profit,
	coalesce(a.profit_rate,0) as profit_rate,
	coalesce(a.goods_cnt,0) as goods_cnt,
	coalesce(a.days_cnt) as days_cnt,
	(case when a.days_cnt=1 then '1次'
		when a.days_cnt>=2 and a.days_cnt<=3 then '2~3次'
		when a.days_cnt>=4 and a.days_cnt<=5 then '4~5次'
		when a.days_cnt>=6 and a.days_cnt<=10 then '6~10次'
		when a.days_cnt>10 then '10次以上'
		else '其他'
	end) as days_cnt_type,
	coalesce(c.receivable_amount,0) as receivable_amount,
	coalesce(c.overdue_amount,0) as overdue_amount,
	coalesce(c.overdue_amount_90,0) as overdue_amount_90,
	coalesce(a.sales_value,0)/a.days_cnt as customer_avg_price,
	(case when a.days_cnt<=3 then '履约磨合期'
		when a.days_cnt>=256 then '合同结束'
		else '履约中'
	end) as days_cnt_type_2	
from
	(		
	select
		region_name,province_name,customer_no,add_day,sales_value,profit,profit_rate,goods_cnt,days_cnt
	from
		(
		select 
			region_name,province_name,customer_no,
			substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as add_day,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate,
			count(distinct goods_code) as goods_cnt,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			region_name,province_name,customer_no
		) tmp1
	where
		add_day>='202101'
		and add_day<='202103'	
	) a
	left join
		(
		select 
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		) b on b.customer_no = a.customer_no
	left join
		(
		select --应收逾期
			customer_no,
			sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount, -- 应收金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount-overdue_amount1-overdue_amount15-overdue_amount30-overdue_amount60 else 0 end) overdue_amount_90	-- 逾期90天以上金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt='20210405'
		group by 
			customer_no
		) c on c.customer_no=a.customer_no

		
		
--===================================================================================================================================================================
-- 七、按城市统计3月份在0-6、6-12、12-15、15-18、18-21、21-24几个时间段内的订单数 下单时间
		
	select
		per,
		region_name,
		province_name,
		city_group_name,
		time_type,
		order_cnt,
		case when time_type= '0-6' then 1
			when time_type= '7-12' then 2
			when time_type= '13-15' then 3
			when time_type= '16-18' then 4
			when time_type= '19-21' then 5
			when time_type= '22-23' then 6
			else null
		end as time_type_2
	from	
		(
		select 
			'20210301至20210331' as per,
			region_name,province_name,city_group_name,
			case when hour(order_time) <=6 then '0-6'
				when hour(order_time) <=12 then '7-12'
				when hour(order_time) <=15 then '13-15'
				when hour(order_time) <=18 then '16-18'
				when hour(order_time) <=21 then '19-21'
				when hour(order_time) <=23 then '22-23'
				else null
			end as time_type,
			count(distinct order_no) as order_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			regexp_replace(split_part(order_time, ' ',1), '-', '') between '20210301' and '20210331'
			and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			region_name,province_name,city_group_name,
			case when hour(order_time) <=6 then '0-6'
				when hour(order_time) <=12 then '7-12'
				when hour(order_time) <=15 then '13-15'
				when hour(order_time) <=18 then '16-18'
				when hour(order_time) <=21 then '19-21'
				when hour(order_time) <=23 then '22-23'
				else null
			end
		) as t1
	order by 
		city_group_name,time_type_2
		
		
		
--===================================================================================================================================================================
--新增客户的投标情况
select
	a.sign_date,
	a.province_name,
	c.business_type_name,
	count(distinct a.customer_no) as customer_cnt
from
	(
	select
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6) as sign_date,
		province_name,
		customer_no
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		regexp_replace(split_part(sign_time, ' ',1), '-', '') >= '20210101' 
		and regexp_replace(split_part(sign_time, ' ',1), '-', '') <= '20210331'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6),
		province_name,
		customer_no
	) as a 
	join
		(
		select
			customer_no,first_order_date,last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
			and first_order_date between '20210101' and '20210331'
		group by
			customer_no,first_order_date,last_order_date
		) as b on b.customer_no=a.customer_no
	join
		(
		select 
			customer_no,customer_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,business_type_name
		) c on c.customer_no = a.customer_no
group by 	
	a.sign_date,
	a.province_name,
	c.business_type_name




--===================================================================================================================================================================
-- 五-2、1、2、3月各省区客户购买品类数（按管理大类一级计算）；
insert overwrite directory '/tmp/zhangyanpeng/20210407_linshi_1' row format delimited fields terminated by '\t' 
select
	a.s_sdt,
	a.province_name,
	a.customer_no,
	b.customer_name,
	a.classify_large_name,
	a.classify_middle_name
from
	(
	select
		substr(sdt,1,6) as s_sdt,
		province_name,
		customer_no,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		sdt >= '20210101' 
		and sdt <= '20210331'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(sdt,1,6),
		province_name,
		customer_no,
		classify_large_code,
		classify_large_name,
		classify_middle_code,
		classify_middle_name
	) as a
	left join 
		(
		select 
			customer_no,customer_name,work_no,sales_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt='current'
		) b on a.customer_no=b.customer_no
group by 
	a.s_sdt,
	a.province_name,
	a.customer_no,
	b.customer_name,
	a.classify_large_name,
	a.classify_middle_name
	
	

--===================================================================================================================================================================
--新增客户的投标情况 销售额
select
	a.sign_date,
	a.province_name,
	c.business_type_name,
	count(distinct a.customer_no) as customer_cnt,
	sum(sales_value) as sales_value
from
	(
	select
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6) as sign_date,
		province_name,
		customer_no,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail
	where 
		regexp_replace(split_part(sign_time, ' ',1), '-', '') >= '20210101' 
		and regexp_replace(split_part(sign_time, ' ',1), '-', '') <= '20210331'
		and business_type_code in ('1','2','3','4','5','6') --业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	group by 
		substr(regexp_replace(split_part(sign_time, ' ',1), '-', ''),1,6),
		province_name,
		customer_no
	) as a 
	join
		(
		select
			customer_no,first_order_date,last_order_date
		from
			csx_dw.dws_crm_w_a_customer_active
		where
			sdt='current'
			and first_order_date between '20210101' and '20210331'
		group by
			customer_no,first_order_date,last_order_date
		) as b on b.customer_no=a.customer_no
	join
		(
		select 
			customer_no,customer_name,business_type_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,business_type_name
		) c on c.customer_no = a.customer_no
group by 	
	a.sign_date,
	a.province_name,
	c.business_type_name
	
	

--===================================================================================================================================================================
-- 六、1、2、3月客户断约情况（参见之前做过的表格）；
-- 按大区和省份统计总下单客户、新增下单客户、断约客户
	select
		a.region_name,
		a.province_name,
		a.customer_cnt,
		coalesce(b.add_customer_cnt,0) as add_customer_cnt,
		coalesce(c.sub_customer_cnt,0) as sub_customer_cnt
	from
		( --下单客户数
		select 
			region_name,province_name,count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			region_name,province_name
		) a
		left join -- 新增客户
			(
			select
				region_name,province_name,count(distinct customer_no) as add_customer_cnt
			from
				(
				select 
					region_name,province_name,customer_no,
					min(sdt) as min_sdt
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				min_sdt>='20210101'
				and min_sdt<='20210331'	
			group by 
				region_name,province_name
			) b on b.province_name=a.province_name
		left join
			(
			select
				region_name,province_name,count(distinct customer_no) as sub_customer_cnt
			from
				(
				select 
					region_name,province_name,customer_no,
					substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as s_sdt
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				s_sdt>='202101'
				and s_sdt<='202103'
			group by 
				region_name,province_name
			) c on c.province_name=a.province_name
			
			

				
				
				
--===================================================================================================================================================================
-- 六、1、2、3月客户断约情况（参见之前做过的表格）；
-- 按大区和省份统计总下单客户、新增下单客户、断约客户
	select
		a.region_name,
		a.province_name,
		a.s_sdt,
		a.customer_cnt,
		coalesce(b.add_customer_cnt,0) as add_customer_cnt,
		coalesce(c.sub_customer_cnt,0) as sub_customer_cnt
	from
		( --下单客户数
		select 
			region_name,province_name,substr(sdt,1,6) as s_sdt,count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			region_name,province_name,substr(sdt,1,6)
		) a
		left join -- 新增客户
			(
			select
				region_name,province_name,substr(min_sdt,1,6) as s_sdt,count(distinct customer_no) as add_customer_cnt
			from
				(
				select 
					region_name,province_name,customer_no,
					min(sdt) as min_sdt
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				min_sdt>='20210101'
				and min_sdt<='20210331'	
			group by 
				region_name,province_name,substr(min_sdt,1,6)
			) b on b.province_name=a.province_name and b.s_sdt=a.s_sdt
		left join
			(
			select
				region_name,province_name,s_sdt,count(distinct customer_no) as sub_customer_cnt
			from
				(
				select 
					region_name,province_name,customer_no,
					substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90)),'-',''),1,6) as s_sdt
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and sales_type !='fanli' -- 排除返利单
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				s_sdt>='202101'
				and s_sdt<='202103'
			group by 
				region_name,province_name,s_sdt
			) c on c.province_name=a.province_name and c.s_sdt=a.s_sdt
			
			
-- 明细
select
	a.region_name,
	a.province_name,
	a.customer_no,
	b.customer_name,
	a.add_day,
	b.work_no,
	b.sales_name,
	b.first_category_name,
	b.second_category_name,
	b.third_category_name,
	coalesce(a.sales_value,0) as sales_value,
	coalesce(a.profit,0) as profit,
	coalesce(a.profit_rate,0) as profit_rate,
	coalesce(a.goods_cnt,0) as goods_cnt,
	coalesce(a.days_cnt) as days_cnt,
	(case when a.days_cnt=1 then '1次'
		when a.days_cnt>=2 and a.days_cnt<=3 then '2~3次'
		when a.days_cnt>=4 and a.days_cnt<=5 then '4~5次'
		when a.days_cnt>=6 and a.days_cnt<=10 then '6~10次'
		when a.days_cnt>10 then '10次以上'
		else '其他'
	end) as days_cnt_type,
	coalesce(c.receivable_amount,0) as receivable_amount,
	coalesce(c.overdue_amount,0) as overdue_amount,
	coalesce(c.overdue_amount_90,0) as overdue_amount_90,
	coalesce(a.sales_value,0)/a.days_cnt as customer_avg_price,
	(case when a.days_cnt<=3 then '履约磨合期'
		when a.days_cnt>=256 then '合同结束'
		else '履约中'
	end) as days_cnt_type_2	
from
	(		
	select
		region_name,province_name,customer_no,add_day,sales_value,profit,profit_rate,goods_cnt,days_cnt
	from
		(
		select 
			region_name,province_name,customer_no,
			substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as add_day,
			sum(sales_value) as sales_value,
			sum(profit) as profit,
			sum(profit)/abs(sum(sales_value)) as profit_rate,
			count(distinct goods_code) as goods_cnt,
			count(distinct sdt) as days_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli' -- 排除返利单
		group by 
			region_name,province_name,customer_no
		) tmp1
	where
		add_day>='202101'
		and add_day<='202103'	
	) a
	left join
		(
		select 
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		from 
			csx_dw.dws_crm_w_a_customer
		where 
			sdt = 'current'
			and customer_no<>''	
		group by
			customer_no,customer_name,attribute,first_supervisor_work_no,first_supervisor_name,work_no,sales_name,
			first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name
		) b on b.customer_no = a.customer_no
	left join
		(
		select --应收逾期
			customer_no,
			sum(case when receivable_amount>=0 then receivable_amount else 0 end) receivable_amount, -- 应收金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount-overdue_amount1-overdue_amount15-overdue_amount30-overdue_amount60 else 0 end) overdue_amount_90	-- 逾期90天以上金额
		from
			csx_dw.dws_sss_r_a_customer_accounts
		where
			sdt='20210405'
		group by 
			customer_no
		) c on c.customer_no=a.customer_no
		
				
				
-- 按大区和省份统计总下单客户、新增下单客户、断约客户、新增客户履约金额
	select
		a.region_name,
		a.province_name,
		a.customer_cnt,
		coalesce(b.add_customer_cnt,0) as add_customer_cnt,
		coalesce(c.sub_customer_cnt,0) as sub_customer_cnt,
		coalesce(b.sales_value,0) as add_customer_sales_value,
		coalesce(c.sales_value,0) as sub_customer_sales_value
	from
		( --下单客户数
		select 
			region_name,province_name,count(distinct customer_no) as customer_cnt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20210101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		group by 
			region_name,province_name
		) a
		left join -- 新增客户
			(
			select
				region_name,province_name,count(distinct customer_no) as add_customer_cnt,sum(sales_value) as sales_value
			from
				(
				select 
					region_name,province_name,customer_no,
					min(sdt) as min_sdt,
					sum(sales_value) as sales_value
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				min_sdt>='20210101'
				and min_sdt<='20210331'	
			group by 
				region_name,province_name
			) b on b.province_name=a.province_name
		left join --断约客户
			(
			select
				region_name,province_name,count(distinct customer_no) as sub_customer_cnt,sum(sales_value) as sales_value
			from
				(
				select 
					region_name,province_name,customer_no,
					substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90)),'-',''),1,6) as s_sdt,
					sum(case when sdt between '20201001' and '20201231' then sales_value else 0 end) as sales_value -- 上个季度履约金额
				from 
					csx_dw.dws_sale_r_d_detail 
				where 
					sdt between '20190101' and '20210331'
					and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and sales_type !='fanli' -- 排除返利单
				group by 
					region_name,province_name,customer_no
				) tmp1
			where
				s_sdt>='202101'
				and s_sdt<='202103'
			group by 
				region_name,province_name
			) c on c.province_name=a.province_name
			
			
-- 断约客户在上个季度的履约金额
		
select	
	a.region_name,
	a.province_name,
	sum(b.sales_value) as sales_value
from
	(
	select
		region_name,province_name,customer_no
	from
		(
		select 
			region_name,province_name,customer_no,
			substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90)),'-',''),1,6) as s_sdt
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli' -- 排除返利单
		group by 
			region_name,province_name,customer_no
		) tmp1
	where
		s_sdt>='202101'
		and s_sdt<='202103'
	group by 
		region_name,province_name,customer_no
	) as a
	left join
		(
		select
			customer_no,sum(sales_value) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail 
		where
			sdt between '20201001' and '20201231'
			and business_type_code in ('1')
			and channel_code in('1','7','9')
			and sales_type !='fanli' -- 排除返利单
		group by 
			customer_no
		) as b on b.customer_no=a.customer_no
group by 
	a.region_name,
	a.province_name		




	select
		region_name,province_name,sum(sales_value) as sales_value
	from
		(
		select 
			region_name,province_name,customer_no,
			substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90)),'-',''),1,6) as s_sdt,
			sum(case when sdt between '20201001' and '20201231' then sales_value else 0 end) as sales_value
		from 
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt between '20190101' and '20210331'
			and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
			and sales_type !='fanli' -- 排除返利单
		group by 
			region_name,province_name,customer_no
		) tmp1
	where
		s_sdt>='202101'
		and s_sdt<='202103'
	group by 
		region_name,province_name	