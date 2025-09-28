-- ================================================================================================================	
-- 按大区和省份统计总下单客户、新增下单客户、断约客户
	select
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.sale_month,
		a.ly_customer_cnt, -- 履约客户数
		coalesce(b.add_customer_cnt,0) as add_customer_cnt, -- 新增客户数
		coalesce(c.dy_customer_cnt,0) as dy_customer_cnt, -- 断约客户数
		coalesce(d.re_ly_customer_cnt,0) as re_ly_customer_cnt -- 断约再下单客户
	from
		( -- 履约客户数
		select 
			a.performance_region_name,a.performance_province_name,a.performance_city_name,substr(a.sdt,1,6) as sale_month,count(distinct a.customer_code) as ly_customer_cnt
		from 
			(
			select
				performance_region_name,performance_province_name,performance_city_name,sdt,customer_code,sale_amt,profit
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt between '20220901' and '20230228'
				and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
				and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
			) a 
			join
				(
				select 
					customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
					performance_province_name
				from 
					-- csx_dw.dws_crm_w_a_customer
					csx_dim.csx_dim_crm_customer_info
				where 
					sdt = 'current'
					and channel_code in('1','7','9')
					and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
				) b on b.customer_code=a.customer_code
		group by 
			a.performance_region_name,a.performance_province_name,a.performance_city_name,substr(a.sdt,1,6)
		) a
		left join -- 新增客户
			(
			select
				a.performance_region_name,a.performance_province_name,a.performance_city_name,
				a.first_business_sale_month,count(distinct a.customer_code) as add_customer_cnt
			from
				(
				select 
					performance_region_name,performance_province_name,performance_city_name,
					customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_business_sale_month
				from 
					csx_dws.csx_dws_crm_customer_business_active_di
				where 
					sdt='current'
					and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
				) a
				join
					(
					select 
						customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
						performance_province_name
					from 
						-- csx_dw.dws_crm_w_a_customer
						csx_dim.csx_dim_crm_customer_info
					where 
						sdt = 'current'
						and channel_code in('1','7','9')
						and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
					) b on b.customer_code=a.customer_code				
			where
				a.first_business_sale_month between '202209' and '202302'
			group by 
				a.performance_region_name,a.performance_province_name,a.performance_city_name,a.first_business_sale_month
			) b on b.performance_province_name=a.performance_province_name and b.performance_city_name=a.performance_city_name and b.first_business_sale_month=a.sale_month
		left join -- 断约客户
			(
			select
				a.performance_region_name,a.performance_province_name,a.performance_city_name,
				a.dy_month,count(distinct a.customer_code) as dy_customer_cnt
			from
				(
				select 
					performance_region_name,performance_province_name,performance_city_name,
					customer_code,last_business_sale_date, -- substr(last_business_sale_date,1,6) as last_business_sale_month,
					substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd')),90)),'-',''),1,6) as dy_month
				from 
					csx_dws.csx_dws_crm_customer_business_active_di
				where 
					sdt='current'
					and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
				) a
				join
					(
					select 
						customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
						performance_province_name
					from 
						-- csx_dw.dws_crm_w_a_customer
						csx_dim.csx_dim_crm_customer_info
					where 
						sdt = 'current'
						and channel_code in('1','7','9')
						and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
					) b on b.customer_code=a.customer_code				
			where
				a.dy_month between '202209' and '202302'
			group by 
				a.performance_region_name,a.performance_province_name,a.performance_city_name,a.dy_month
			) c on c.performance_province_name=a.performance_province_name and c.performance_city_name=a.performance_city_name and c.dy_month=a.sale_month
		left join -- 断约再下单客户			
			(	
			select 
				a.performance_region_name,a.performance_province_name,a.performance_city_name,substr(a.sdt,1,6) as sale_month,
				count(distinct a.customer_code) as re_ly_customer_cnt
			from
				(
				select
					performance_region_name,performance_province_name,performance_city_name,customer_code,sdt,
					to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))) as sdt_date,
					to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd'))) as lead_sdt_date,
					datediff(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd')))) as diff_days
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt between '20190101' and '20230228' 
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
					and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
				) a 
				join
					(
					select 
						customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
						performance_province_name
					from 
						-- csx_dw.dws_crm_w_a_customer
						csx_dim.csx_dim_crm_customer_info
					where 
						sdt = 'current'
						and channel_code in('1','7','9')
						and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
					) b on b.customer_code=a.customer_code	
			where
				a.diff_days>90
			group by 
				a.performance_region_name,a.performance_province_name,a.performance_city_name,substr(a.sdt,1,6)
			) d on d.performance_province_name=a.performance_province_name and d.performance_city_name=a.performance_city_name and d.sale_month=a.sale_month
			
-- ================================================================================================================	
-- 新客下单 每月销售明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_sale;
create table csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_sale
as
select
	b.performance_region_name,b.performance_province_name,b.performance_city_name,b.first_business_sale_month,b.customer_code,b.customer_name,
	b.first_category_name,b.second_category_name,b.third_category_name,
	a.sale_month,a.sale_amt,a.profit,a.profit_rate,c.customer_large_level,
	row_number()over(partition by b.customer_code order by a.sale_month) as rn
from
	( -- 销售明细
	select
		substr(sdt,1,6) as sale_month,customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20190101' and '20230228' 
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
		and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	group by 
		substr(sdt,1,6),customer_code
	) a 
	join	
		(
		select -- 新客
			a.performance_region_name,a.performance_province_name,a.performance_city_name,a.first_business_sale_month,a.customer_code,
			b.customer_name,b.first_category_name,b.second_category_name,b.third_category_name
		from
			(
			select 
				performance_region_name,performance_province_name,performance_city_name,
				customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_business_sale_month
			from 
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt='current'
				and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
				and first_business_sale_date between '20220901' and '20230228'
			) a
			join
				(
				select 
					customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
					performance_province_name
				from 
					-- csx_dw.dws_crm_w_a_customer
					csx_dim.csx_dim_crm_customer_info
				where 
					sdt = 'current'
					and channel_code in('1','7','9')
					and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
				) b on b.customer_code=a.customer_code
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='201901' and month<='202302'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) c on c.customer_no=a.customer_code and c.month=a.sale_month		
;
select * from csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_sale

-- ================================================================================================================	
-- 断约再下单客户 激活客户明细 销售明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zx_jh_customer_sale;
create table csx_analyse_tmp.csx_analyse_tmp_zx_jh_customer_sale
as
select
	b.performance_region_name,b.performance_province_name,b.performance_city_name,b.sale_month as jh_month,b.customer_code,b.customer_name,
	b.first_category_name,b.second_category_name,b.third_category_name,
	a.sale_month,a.sale_amt,a.profit,a.profit_rate,c.customer_large_level,
	row_number()over(partition by b.customer_code order by a.sale_month) as rn
from
	( -- 销售明细
	select
		substr(sdt,1,6) as sale_month,customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20220101' and '20230228' -- 销售日期
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
		and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	group by 
		substr(sdt,1,6),customer_code
	) a 			
	join	
		(	
		select
			a.performance_region_name,a.performance_province_name,a.performance_city_name,a.customer_code,a.customer_name,a.sale_month,
			a.first_category_name,a.second_category_name,a.third_category_name,a.rn		
		from
			(
			select 
				a.performance_region_name,a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,substr(a.sdt,1,6) as sale_month,
				b.first_category_name,b.second_category_name,b.third_category_name,row_number()over(partition by a.customer_code order by a.sdt desc) as rn
			from
				(
				select
					performance_region_name,performance_province_name,performance_city_name,customer_code,sdt,
					-- to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))) as sdt_date,
					-- to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd'))) as lead_sdt_date,
					datediff(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),to_date(from_unixtime(unix_timestamp(lead(sdt,1,null)over(partition by customer_code order by sdt desc),'yyyyMMdd')))) as diff_days
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt between '20190101' and '20230228' -- 历史所有数据
					and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
					and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
					and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
				) a 
				join
					(
					select 
						customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
						performance_province_name
					from 
						csx_dim.csx_dim_crm_customer_info
					where 
						sdt = 'current'
						and channel_code in('1','7','9')
						and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
					) b on b.customer_code=a.customer_code	
			where
				a.diff_days>90
			) a 
		where
			rn=1
			and sale_month>='202201'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='201901' and month<='202302'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) c on c.customer_no=a.customer_code and c.month=a.sale_month	
;
select * from csx_analyse_tmp.csx_analyse_tmp_zx_jh_customer_sale				

-- ================================================================================================================	
-- 断约客户 销售明细
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zx_dy_customer_sale;
create table csx_analyse_tmp.csx_analyse_tmp_zx_dy_customer_sale
as
select
	b.performance_region_name,b.performance_province_name,b.performance_city_name,b.dy_month,b.customer_code,b.customer_name,
	b.first_category_name,b.second_category_name,b.third_category_name,
	a.sale_month,a.sale_amt,a.profit,a.profit_rate,c.customer_large_level,d.customer_large_level as last_customer_large_level,
	row_number()over(partition by b.customer_code order by a.sale_month) as rn
from
	( -- 销售明细
	select
		substr(sdt,1,6) as sale_month,customer_code,sum(sale_amt) as sale_amt,sum(profit) as profit,sum(profit)/abs(sum(sale_amt)) as profit_rate
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20220101' and '20230228' -- 销售日期
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in (1) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)	
		and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
	group by 
		substr(sdt,1,6),customer_code
	) a 
	right join
		(
		select
			a.performance_region_name,a.performance_province_name,a.performance_city_name,
			a.dy_month,a.customer_code,b.customer_name,b.first_category_name,b.second_category_name,b.third_category_name
		from
			(
			select 
				performance_region_name,performance_province_name,performance_city_name,
				customer_code,last_business_sale_date,
				substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd')),90)),'-',''),1,6) as dy_month
			from 
				csx_dws.csx_dws_crm_customer_business_active_di
			where 
				sdt='current'
				and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			) a
			join
				(
				select 
					customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
					performance_province_name
				from 
					csx_dim.csx_dim_crm_customer_info
				where 
					sdt = 'current'
					and channel_code in('1','7','9')
					and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
				) b on b.customer_code=a.customer_code				
		where
			a.dy_month between '202209' and '202302'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='201901' and month<='202302'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) c on c.customer_no=a.customer_code and c.month=a.sale_month
	left join
		(
		select
			month,customer_no,customer_large_level,rn
		from
			(
			select
				month,customer_no,customer_large_level,row_number()over(partition by customer_no order by month desc) as rn
			from 
				-- csx_dw.report_sale_r_m_customer_level
				csx_analyse.csx_analyse_report_sale_customer_level_mf
			where
				month>='201901'
				-- and customer_large_level in ('A','B')
				and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
			) a 
		where
			rn=1
		) d on d.customer_no=b.customer_code		
;
select * from csx_analyse_tmp.csx_analyse_tmp_zx_dy_customer_sale
	
-- ================================================================================================================	
-- 断约客户所属行业及等级统计
select
	a.performance_region_name,a.performance_province_name,a.performance_city_name,a.dy_month,
	b.first_category_code,b.first_category_name,b.second_category_code,b.second_category_name,
	c.customer_large_level,
	count(distinct a.customer_code) as dy_customer_cnt
from
	(
	select 
		performance_region_name,performance_province_name,performance_city_name,
		customer_code,last_business_sale_date, -- substr(last_business_sale_date,1,6) as last_business_sale_month,
		substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd')),90)),'-',''),1,6) as dy_month
	from 
		csx_dws.csx_dws_crm_customer_business_active_di
	where 
		sdt='current'
		and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	) a
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		) b on b.customer_code=a.customer_code	
	left join
		(
		select
			month,customer_no,customer_large_level,rn
		from
			(
			select
				month,customer_no,customer_large_level,row_number()over(partition by customer_no order by month desc) as rn
			from 
				-- csx_dw.report_sale_r_m_customer_level
				csx_analyse.csx_analyse_report_sale_customer_level_mf
			where
				month>='201901'
				-- and customer_large_level in ('A','B')
				and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
			) a 
		where
			rn=1
		) c on c.customer_no=a.customer_code		
where
	a.dy_month between '202209' and '202302'
group by 
	a.performance_region_name,a.performance_province_name,a.performance_city_name,a.dy_month,
	b.first_category_code,b.first_category_name,b.second_category_code,b.second_category_name,
	c.customer_large_level
;

-- ================================================================================================================	
-- 断约客户list
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zx_dy_customer_list;
create table csx_analyse_tmp.csx_analyse_tmp_zx_dy_customer_list
as
select
	a.performance_region_name,a.performance_province_name,a.performance_city_name,a.customer_code,b.customer_name,a.dy_month,
	b.first_category_code,b.first_category_name,b.second_category_code,b.second_category_name,
	c.customer_large_level
from
	(
	select 
		performance_region_name,performance_province_name,performance_city_name,
		customer_code,last_business_sale_date, -- substr(last_business_sale_date,1,6) as last_business_sale_month,
		substr(regexp_replace(to_date(date_add(from_unixtime(unix_timestamp(last_business_sale_date,'yyyyMMdd')),90)),'-',''),1,6) as dy_month
	from 
		csx_dws.csx_dws_crm_customer_business_active_di
	where 
		sdt='current'
		and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	) a
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		) b on b.customer_code=a.customer_code	
	left join
		(
		select
			month,customer_no,customer_large_level,rn
		from
			(
			select
				month,customer_no,customer_large_level,row_number()over(partition by customer_no order by month desc) as rn
			from 
				-- csx_dw.report_sale_r_m_customer_level
				csx_analyse.csx_analyse_report_sale_customer_level_mf
			where
				month>='201901'
				-- and customer_large_level in ('A','B')
				and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
			) a 
		where
			rn=1
		) c on c.customer_no=a.customer_code		
where
	a.dy_month between '202209' and '202302'
;
select * from csx_analyse_tmp.csx_analyse_tmp_zx_dy_customer_list	

-- ====================================================================================================================================================

--日配_所有客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_all_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_all_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	-- floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	floor(months_between(from_unixtime(unix_timestamp(concat(b.smonth,'01'),'yyyyMMdd')),from_unixtime(unix_timestamp(concat(a.new_smonth,'01'),'yyyyMMdd')))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(profit_no_tax) as profit_no_tax,
	sum(days_cnt) as days_cnt
from
	(
	select 
		performance_region_name,performance_province_name,performance_city_name,customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as new_smonth
	from
		csx_dws.csx_dws_crm_customer_business_active_di
	where 
		sdt='current'
		and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and first_business_sale_date between '20210101' and '20230228'
	)a
	join 	
		(
		select 
			customer_code,substr(sdt,1,6) smonth,sum(sale_amt_no_tax) sale_amt_no_tax,sum(profit_no_tax) as profit_no_tax,count(distinct sdt) as days_cnt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20210101' and sdt<='20230228'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
			and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
		group by 
			customer_code,substr(sdt,1,6)
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name
		from 
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_code=a.customer_code	
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	-- floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	-- concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
	floor(months_between(from_unixtime(unix_timestamp(concat(b.smonth,'01'),'yyyyMMdd')),from_unixtime(unix_timestamp(concat(a.new_smonth,'01'),'yyyyMMdd'))))
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_all_customer

-- ====================================================================================================================================================

--日配_至少A、B类有1个月的客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_customer;
create table csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_customer
as
select 
	c.performance_province_name,
	a.new_smonth,
	c.second_category_name,
	-- floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
	floor(months_between(from_unixtime(unix_timestamp(concat(b.smonth,'01'),'yyyyMMdd')),from_unixtime(unix_timestamp(concat(a.new_smonth,'01'),'yyyyMMdd')))) diff_month,
	count(distinct b.customer_code) counts,
	sum(b.sale_amt_no_tax) sale_amt_no_tax,
	sum(profit_no_tax) as profit_no_tax,
	sum(days_cnt) as days_cnt
from
	(
	select 
		performance_region_name,performance_province_name,performance_city_name,customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as new_smonth
	from
		csx_dws.csx_dws_crm_customer_business_active_di
	where 
		sdt='current'
		and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and first_business_sale_date between '20210101' and '20230228'
	)a
	join
		(
		select 
			customer_code,substr(sdt,1,6) smonth,sum(sale_amt_no_tax) sale_amt_no_tax,sum(profit_no_tax) as profit_no_tax,count(distinct sdt) as days_cnt
		from 
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20210101' and sdt<='20230228'
			and channel_code in('1','7','9')
			and business_type_code in (1) -- 日配
			and order_channel_code !=4 -- 订单来源渠道: 1-b端 2-m端 3-bbc 4-调价返利 -1-sap
		group by 
			customer_code,substr(sdt,1,6)
		)b on a.customer_code=b.customer_code
	join
		(
		select 
			customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
			performance_province_name
		from 
			-- csx_dw.dws_crm_w_a_customer
			csx_dim.csx_dim_crm_customer_info
		where 
			sdt = 'current'
			and channel_code in('1','7','9')
			and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
		)c on c.customer_code=a.customer_code	
	join
		( --至少出现过1个月AB类的客户
		select
			customer_no,count(distinct month) as months_cnt
		from 
			-- csx_dw.report_sale_r_m_customer_level
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202302'
			and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no
		having
			count(distinct month)>=1
		) d on d.customer_no=a.customer_code
group by
	c.performance_province_name, 
	a.new_smonth,
	c.second_category_name,
	-- floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),
	-- concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01')))
	floor(months_between(from_unixtime(unix_timestamp(concat(b.smonth,'01'),'yyyyMMdd')),from_unixtime(unix_timestamp(concat(a.new_smonth,'01'),'yyyyMMdd'))))
;
select * from csx_analyse_tmp.csx_analyse_tmp_ripei_ab_two_months_customer
		
-- ================================================================================================================	
-- 新客首月等级分布
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_level;
create table csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_level
as
select
	a.performance_region_name,a.performance_province_name,a.performance_city_name,a.first_business_sale_month,a.customer_code,a.customer_name,
	a.first_category_name,a.second_category_name,a.third_category_name,c.customer_large_level
from	
	(
	select -- 新客
		a.performance_region_name,a.performance_province_name,a.performance_city_name,a.first_business_sale_month,a.customer_code,
		b.customer_name,b.first_category_name,b.second_category_name,b.third_category_name
	from
		(
		select 
			performance_region_name,performance_province_name,performance_city_name,
			customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_business_sale_month
		from 
			csx_dws.csx_dws_crm_customer_business_active_di
		where 
			sdt='current'
			and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			and first_business_sale_date between '20210301' and '20230228'
		) a
		join
			(
			select 
				customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				performance_province_name
			from 
				-- csx_dw.dws_crm_w_a_customer
				csx_dim.csx_dim_crm_customer_info
			where 
				sdt = 'current'
				and channel_code in('1','7','9')
				and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
			) b on b.customer_code=a.customer_code
	) a 
	left join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='201901' and month<='202302'
			-- and customer_large_level in ('A','B')
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) c on c.customer_no=a.customer_code and c.month=a.first_business_sale_month		
;
select * from csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_level
;

-- ================================================================================================================	
-- 新客首月等级分布
drop table if exists csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_level_02;
create table csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_level_02
as
select
	a.performance_region_name,a.performance_province_name,a.performance_city_name,a.first_business_sale_month,a.customer_code,a.customer_name,
	a.first_category_name,a.second_category_name,a.third_category_name,c.customer_large_level
from	
	(
	select -- 新客
		a.performance_region_name,a.performance_province_name,a.performance_city_name,a.first_business_sale_month,a.customer_code,
		b.customer_name,b.first_category_name,b.second_category_name,b.third_category_name
	from
		(
		select 
			performance_region_name,performance_province_name,performance_city_name,
			customer_code,first_business_sale_date,substr(first_business_sale_date,1,6) as first_business_sale_month
		from 
			csx_dws.csx_dws_crm_customer_business_active_di
		where 
			sdt='current'
			and business_type_code=1 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			and first_business_sale_date between '20210301' and '20230228'
		) a
		join
			(
			select 
				customer_code,customer_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				performance_province_name
			from 
				-- csx_dw.dws_crm_w_a_customer
				csx_dim.csx_dim_crm_customer_info
			where 
				sdt = 'current'
				and channel_code in('1','7','9')
				and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
			) b on b.customer_code=a.customer_code
	) a 
	left join
		(
		select
			customer_no,customer_large_level,month_cnt,rn
		from
			(
			select
				customer_no,customer_large_level,count(month) as month_cnt,row_number()over(partition by customer_no order by count(month) desc,customer_large_level) as rn
			from 
				csx_analyse.csx_analyse_report_sale_customer_level_mf
			where
				month>='201901' and month<='202302'
				-- and customer_large_level in ('A','B')
				and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
			group by 
				customer_no,customer_large_level
			) a 
		where
			rn=1
		) c on c.customer_no=a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_zx_new_customer_level_02

	
