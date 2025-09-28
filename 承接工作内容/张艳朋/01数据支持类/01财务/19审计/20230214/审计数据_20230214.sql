-- 大单品数据需求 合计
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01
as
select
	a.performance_province_name,a.channel_name,a.customer_code,b.customer_name,
	'20221001-20230131' as qijian,
	a.goods_code,c.goods_name,a.sale_amt,b.first_category_name,b.second_category_name,b.third_category_name,a.rn
from
	(
	select
		a.performance_province_name,a.channel_name,a.customer_code,a.goods_code,a.sale_amt,b.rn
	from
		(
		select
			performance_province_name,channel_name,customer_code,goods_code,sum(sale_amt) as sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230131'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='北京市'
		group by 
			performance_province_name,channel_name,customer_code,goods_code
		) a
		join
			(
			select
				goods_code,rn
			from
				(
				select
					goods_code,sum(sale_amt) as sale_amt,row_number()over(order by sum(sale_amt) desc) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230131'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='北京市'
				group by 
					goods_code	
				) a 
			where
				rn<=20
			) b on b.goods_code=a.goods_code
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
order by 
	a.rn,a.sale_amt desc		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01


-- 大单品数据需求 业务类型
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02
as
select
	a.performance_province_name,a.channel_name,a.customer_code,b.customer_name,
	'20221001-20230131' as qijian,
	a.goods_code,c.goods_name,a.sale_amt,a.business_type_name,b.first_category_name,b.second_category_name,b.third_category_name,a.rn
from
	(
	select
		a.performance_province_name,a.channel_name,a.customer_code,a.goods_code,a.business_type_name,a.sale_amt,b.rn
	from
		(
		select
			performance_province_name,channel_name,customer_code,goods_code,business_type_name,sum(sale_amt) as sale_amt
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230131'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='北京市'
		group by 
			performance_province_name,channel_name,customer_code,goods_code,business_type_name
		) a
		join
			(
			select
				business_type_name,goods_code,rn
			from
				(
				select
					business_type_name,goods_code,sum(sale_amt) as sale_amt,row_number()over(partition by business_type_name order by sum(sale_amt) desc) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230131'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='北京市'
				group by 
					business_type_name,goods_code	
				) a 
			where
				rn<=20
			) b on b.goods_code=a.goods_code and b.business_type_name=a.business_type_name
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code	
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02

-- ===============================================================================================================================================================
-- 退货数据需求 合计
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03
as
select
	a.performance_province_name,a.channel_name,a.customer_code,b.customer_name,a.order_code,a.original_order_code,a.sale_amt,a.sale_amt_refund,
	'20221001-20230131' as qijian,
	a.days_cnt,a.min_sdt_refund,a.min_sdt
from
	(
	select
		a.performance_province_name,a.channel_name,a.customer_code,a.order_code,a.original_order_code,c.sale_amt,a.sale_amt_refund,b.days_cnt,a.min_sdt_refund,c.min_sdt,b.rn
	from
		(
		select
			performance_province_name,channel_name,customer_code,order_code,original_order_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230131'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='北京市'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
		group by 
			performance_province_name,channel_name,customer_code,order_code,original_order_code
		) a
		join
			(
			select
				customer_code,sale_amt,days_cnt,rn
			from
				(
				select
					customer_code,sum(sale_amt) as sale_amt,count(distinct sdt) as days_cnt,row_number()over(order by sum(sale_amt)) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230131'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='北京市'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
				group by 
					customer_code	
				) a 
			where
				rn<=30
			) b on b.customer_code=a.customer_code
		left join
			(
			select
				order_code,sum(sale_amt) as sale_amt,min(sdt) as min_sdt
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230131'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='北京市'
			group by 
				order_code
			) c on c.order_code=a.original_order_code
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	-- left join
	-- 	(
	-- 	select
	-- 		goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
	-- 	from
	-- 		csx_dim.csx_dim_basic_goods
	-- 	where
	-- 		sdt='current'
	-- 	) c on c.goods_code=a.goods_code		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03

-- 退货数据需求 详情
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04
as
select
	a.performance_province_name,a.channel_name,a.customer_code,b.customer_name,a.order_code,a.original_order_code,a.sale_amt,a.sale_amt_refund,
	'20221001-20230131' as qijian,
	a.days_cnt,a.min_sdt_refund,a.min_sdt,a.goods_code,c.goods_name
from
	(
	select
		a.performance_province_name,a.channel_name,a.customer_code,a.order_code,a.original_order_code,a.goods_code,c.sale_amt,a.sale_amt_refund,b.days_cnt,a.min_sdt_refund,c.min_sdt,b.rn
	from
		(
		select
			performance_province_name,channel_name,customer_code,order_code,original_order_code,goods_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230131'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='北京市'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
		group by 
			performance_province_name,channel_name,customer_code,order_code,original_order_code,goods_code
		) a
		join
			(
			select
				customer_code,sale_amt,days_cnt,rn
			from
				(
				select
					customer_code,sum(sale_amt) as sale_amt,count(distinct sdt) as days_cnt,row_number()over(order by sum(sale_amt)) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230131'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='北京市'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
				group by 
					customer_code	
				) a 
			where
				rn<=30
			) b on b.customer_code=a.customer_code
		left join
			(
			select
				order_code,goods_code,sum(sale_amt) as sale_amt,min(sdt) as min_sdt
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230131'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='北京市'
			group by 
				order_code,goods_code
			) c on c.order_code=a.original_order_code and c.goods_code=a.goods_code
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	 left join
	 	(
	 	select
	 		goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
	 	from
	 		csx_dim.csx_dim_basic_goods
	 	where
	 		sdt='current'
	 	) c on c.goods_code=a.goods_code		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04

-- ===============================================================================================================================================================

-- 项目供应商需求 销售金额维度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05
as
select
	a.performance_province_name,a.channel_name,a.business_type_name,a.customer_code,b.customer_name,
	'20221001-20230131' as qijian,
	a.sale_amt,c.overdue_amount,c.max_overdue_day,a.sale_amt_refund,first_category_name,second_category_name,third_category_name
from
	(
	select
		a.performance_province_name,a.channel_name,a.customer_code,a.business_type_name,a.sale_amt,a.sale_amt_refund
	from
		(
		select
			performance_province_name,channel_name,customer_code,business_type_name,sum(sale_amt) as sale_amt,sum(if(refund_order_flag=1,sale_amt,0)) as sale_amt_refund,
			row_number()over(order by sum(sale_amt) desc) as rn
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230131'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='北京市'
			and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		group by 
			performance_province_name,channel_name,customer_code,business_type_name
		) a
	where
		rn<=20
	) a 
	left join
		(
		select 
			customer_code,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
			max(max_overdue_day) as max_overdue_day
		from 
			csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
		where 
			sdt = '20230213'
		group by 
			customer_code
		) c on c.customer_code=a.customer_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	-- left join
	--  	(
	--  	select
	--  		goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
	--  	from
	--  		csx_dim.csx_dim_basic_goods
	--  	where
	--  		sdt='current'
	--  	) c on c.goods_code=a.goods_code		
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05

-- 项目供应商需求 逾期金额维度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06
as
select
	c.province_name,c.channel_name,a.business_type_name,c.customer_code,b.customer_name,
	'20221001-20230131' as qijian,
	a.sale_amt,c.overdue_amount,c.max_overdue_day,a.sale_amt_refund,first_category_name,second_category_name,third_category_name,c.rn
from
	(
	select
		a.performance_province_name,a.channel_name,a.customer_code,a.business_type_name,a.sale_amt,a.sale_amt_refund
	from
		(
		select
			performance_province_name,channel_name,customer_code,business_type_name,sum(sale_amt) as sale_amt,sum(if(refund_order_flag=1,sale_amt,0)) as sale_amt_refund,
			row_number()over(order by sum(sale_amt) desc) as rn
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230131'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='北京市'
			and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		group by 
			performance_province_name,channel_name,customer_code,business_type_name
		) a
	-- where
	-- 	rn<=20
	) a 
	right join
		(
		select
			province_name,customer_code,channel_name,overdue_amount,max_overdue_day,rn
		from
			(
			select 
				province_name,channel_name,customer_code,
				sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
				max(max_overdue_day) as max_overdue_day,
				row_number()over(order by sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) desc) as rn
			from 
				csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
			where 
				sdt = '20230213'
				and province_name='北京市'
			group by 
				province_name,channel_name,customer_code
			) a 
		where
			rn<=20
		) c on c.customer_code=a.customer_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=c.customer_code
order by 
	rn
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06


-- 项目供应商需求 退货金额维度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07
as
select
	a.performance_province_name,a.channel_name,a.business_type_name,a.customer_code,b.customer_name,
	'20221001-20230131' as qijian,
	a.sale_amt,c.overdue_amount,c.max_overdue_day,a.sale_amt_refund,first_category_name,second_category_name,third_category_name,a.rn
from
	(
	select
		a.performance_province_name,a.channel_name,a.customer_code,a.business_type_name,a.sale_amt,a.sale_amt_refund,a.rn
	from
		(
		select
			performance_province_name,channel_name,customer_code,business_type_name,sum(sale_amt) as sale_amt,sum(if(refund_order_flag=1,sale_amt,0)) as sale_amt_refund,
			row_number()over(order by sum(if(refund_order_flag=1,sale_amt,0))) as rn
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230131'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='北京市'
			and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		group by 
			performance_province_name,channel_name,customer_code,business_type_name
		) a
	where
		rn<=20
	) a 
	left join
		(
		select
			customer_code,overdue_amount,max_overdue_day
		from
			(
			select 
				customer_code,
				sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
				max(max_overdue_day) as max_overdue_day
				-- row_number()over(order by sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) desc) as rn
			from 
				csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
			where 
				sdt = '20230213'
				-- and province_name='北京市'
			group by 
				customer_code
			) a 
		where
			1=1
			-- rn<=20
		) c on c.customer_code=a.customer_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=c.customer_code
order by 
	rn
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07

-- ===============================================================================================================================================================

-- 应收数据需求
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08
as
select
	a.province_name,a.customer_code,b.customer_name,
	-- '20221001-20230131' as qijian,
	a.receivable_amount,a.overdue_amount,
	bill_amt_all,
	invoice_amount_all,
	a.max_overdue_day,
	first_category_name,second_category_name,third_category_name,a.rn
from
	(
	select
		province_name,customer_code,channel_name,overdue_amount,max_overdue_day,receivable_amount,rn,bill_amt_all,invoice_amount_all
	from
		(
		select 
			province_name,channel_name,customer_code,
			sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) overdue_amount,	-- 逾期金额
			max(max_overdue_day) as max_overdue_day,
			sum(receivable_amount) as receivable_amount,
			sum(bill_amt_all) as bill_amt_all, -- 对账金额
			sum(invoice_amount_all) as invoice_amount_all, -- 开票金额
			row_number()over(order by sum(case when overdue_amount>=0 and receivable_amount>0 then overdue_amount else 0 end) desc) as rn
		from 
			csx_dws.csx_dws_sss_customer_invoice_bill_settle_stat_di
		where 
			sdt = '20230213'
			and province_name='北京市'
		group by 
			province_name,channel_name,customer_code
		) a 
	where
		rn<=50
	) a
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
order by 
	rn
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08


-- ===============================================================================================================================================================
-- 自提业务需求
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.order_code,
	a.sale_amt,a.delivery_type_name,a.business_type_name,huikuan,huikuan_amt,kaipiao,kaipiao_amt,
	first_category_name,second_category_name,third_category_name
from
	(
	select
		performance_province_name,customer_code,order_code,
		sum(sale_amt) as sale_amt,
		delivery_type_name,business_type_name,
		'' as huikuan,
		'' as huikuan_amt,
		'' as kaipiao,
		'' as kaipiao_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230131'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and performance_province_name='北京市'
	group by 
		performance_province_name,customer_code,order_code,delivery_type_name,business_type_name
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full,channel_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09

-- 自提业务需求
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_10;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_10
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.order_code,a.goods_code,c.goods_name,
	a.sale_amt,a.delivery_type_name,a.business_type_name,huikuan,d.paid_amt,kaipiao,d.invoice_amount,
	first_category_name,second_category_name,third_category_name
from
	(
	select
		performance_province_name,customer_code,order_code,goods_code,
		sum(sale_amt) as sale_amt,
		delivery_type_name,business_type_name,
		'' as huikuan,
		'' as kaipiao
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230131'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and performance_province_name='北京市'
	group by 
		performance_province_name,customer_code,order_code,delivery_type_name,business_type_name,goods_code
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full,channel_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code
	left join
		(
		select 
			source_bill_no,customer_code,
			sum(paid_amt) as paid_amt, -- 已回款金额_原销售结算
			sum(invoice_amount) -- 开票金额
		from 
			csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
		where 
			sdt='20230214' 
			and money_back_status !='NON'
			-- and source_bill_no='B2210220306454338A' 
		group by 
			source_bill_no,customer_code
		) d on d.customer_code=a.customer_code and d.source_bill_no=a.order_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_10

-- 自提业务需求
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_11;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_11
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.order_code,
	a.sale_amt,a.delivery_type_name,a.business_type_name,huikuan,d.paid_amt,kaipiao,d.invoice_amount,
	first_category_name,second_category_name,third_category_name
from
	(
	select
		performance_province_name,customer_code,order_code,
		sum(sale_amt) as sale_amt,
		delivery_type_name,business_type_name,
		'' as huikuan,
		'' as kaipiao
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230131'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and performance_province_name='北京市'
	group by 
		performance_province_name,customer_code,order_code,delivery_type_name,business_type_name
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full,channel_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	-- left join
	-- 	(
	-- 	select
	-- 		goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
	-- 	from
	-- 		csx_dim.csx_dim_basic_goods
	-- 	where
	-- 		sdt='current'
	-- 	) c on c.goods_code=a.goods_code
	left join
		(
		select 
			source_bill_no,customer_code,
			sum(paid_amt) as paid_amt, -- 已回款金额_原销售结算
			sum(invoice_amount) as invoice_amount -- 开票金额
		from 
			csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
		where 
			sdt='20230214' 
			and money_back_status !='NON'
			-- and source_bill_no='B2210220306454338A' 
		group by 
			source_bill_no,customer_code
		) d on d.customer_code=a.customer_code and d.source_bill_no=a.order_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_11

-- 验数
select 
	source_bill_no,customer_code,
	sum(paid_amt) as paid_amt, -- 已回款金额_原销售结算
	sum(invoice_amount) -- 开票金额
from 
	csx_dws.csx_dws_sss_order_credit_invoice_bill_settle_detail_di 
where 
	sdt='20230214' 
	and money_back_status !='NON'
	and source_bill_no='B2210220306454338A' 
group by 
	source_bill_no,customer_code


