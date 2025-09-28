-- 广东大单品数据需求 合计
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01
as
select
	'20221001-20230228' as qijian,
	a.performance_province_name,a.order_code,a.order_sale_date,a.order_sale_amt,a.customer_code,b.customer_name,a.goods_code,c.goods_name,a.sale_amt,a.rn,a.unit_name,a.sale_qty
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.goods_code,a.sale_amt,b.rn,c.order_sale_amt,c.order_sale_date,unit_name,sale_qty
	from
		(
		select
			performance_province_name,customer_code,order_code,goods_code,unit_name,sum(sale_amt) as sale_amt,sum(sale_qty) as sale_qty
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and business_type_code not in (4,6) -- 不含项目供应商和BBC
			and performance_province_name='广东省'
		group by 
			performance_province_name,customer_code,order_code,goods_code,unit_name
		) a
		join
			(
			select
				goods_code,sale_amt,rn
			from
				(
				select
					goods_code,sum(sale_amt) as sale_amt,row_number()over(order by sum(sale_amt) desc) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230228'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and business_type_code not in (4,6) -- 不含项目供应商和BBC
					and performance_province_name='广东省'
				group by 
					goods_code	
				) a 
			where
				rn<=30
			) b on b.goods_code=a.goods_code
		left join
			(
			select
				order_code,sum(sale_amt) as order_sale_amt,min(sdt) as order_sale_date
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20221001' and sdt<='20230228'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and business_type_code not in (4,6) -- 不含项目供应商和BBC
				and performance_province_name='广东省'
			group by 
				order_code
			) c on c.order_code=a.order_code	
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


-- 广东大单品数据需求 业务类型
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02
as
select
	'20221001-20230228' as qijian,
	a.performance_province_name,a.order_code,a.order_sale_date,a.order_sale_amt,a.business_type_name,a.customer_code,b.customer_name,a.goods_code,c.goods_name,a.sale_amt,a.rn,
	unit_name,sale_qty
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.goods_code,a.business_type_name,a.sale_amt,b.rn,c.order_sale_amt,c.order_sale_date,unit_name,sale_qty
	from
		(
		select
			performance_province_name,customer_code,order_code,goods_code,business_type_name,sum(sale_amt) as sale_amt,unit_name,sum(sale_qty) as sale_qty
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			-- and business_type_code not in (4,6) -- 不含项目供应商和BBC
			and performance_province_name='广东省'
		group by 
			performance_province_name,customer_code,order_code,goods_code,business_type_name,unit_name
		) a
		join
			(
			select
				business_type_name,goods_code,sale_amt,rn
			from
				(
				select
					business_type_name,goods_code,sum(sale_amt) as sale_amt,row_number()over(partition by business_type_name order by sum(sale_amt) desc) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230228'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					-- and business_type_code not in (4,6) -- 不含项目供应商和BBC
					and performance_province_name='广东省'
				group by 
					business_type_name,goods_code	
				) a 
			where
				rn<=30
			) b on b.goods_code=a.goods_code and b.business_type_name=a.business_type_name
		left join
			(
			select
				business_type_name,order_code,sum(sale_amt) as order_sale_amt,min(sdt) as order_sale_date
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20221001' and sdt<='20230228'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				-- and business_type_code not in (4,6) -- 不含项目供应商和BBC
				and performance_province_name='广东省'
			group by 
				business_type_name,order_code
			) c on c.business_type_name=a.business_type_name and c.order_code=a.order_code
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
	a.business_type_name,a.rn,a.sale_amt desc	
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02

-- ===============================================================================================================================================================
-- 广东退货数据需求 单笔退货金额TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03
as
select
	'20221001-20230131' as qijian,
	a.performance_province_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.customer_code,b.customer_name,a.business_type_name,a.sale_amt_refund,
	a.goods_code,c.goods_name,a.rn
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.goods_code,c.order_sale_amt,a.sale_amt_refund,b.days_cnt,a.min_sdt_refund,c.order_min_sdt,b.rn
	from
		(
		select
			performance_province_name,customer_code,order_code,original_order_code,business_type_name,goods_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='广东省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
		group by 
			performance_province_name,customer_code,order_code,original_order_code,business_type_name,goods_code
		) a
		join
			(
			select
				order_code,business_type_name,sale_amt,days_cnt,rn
			from
				(
				select
					order_code,business_type_name,sum(sale_amt) as sale_amt,count(distinct sdt) as days_cnt,row_number()over(partition by business_type_name order by sum(sale_amt)) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230228'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='广东省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
				group by 
					order_code,business_type_name
				) a 
			where
				rn<=30
			) b on b.order_code=a.order_code and b.business_type_name=a.business_type_name
		left join
			(
			select
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code) as order_code,
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code) as order_code,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230228' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code)
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
	a.business_type_name,a.rn,a.sale_amt_refund 			
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03


-- 广东退货数据需求 退货单品TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04
as
(
select
	'20221001-20230131' as qijian,
	a.performance_province_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.customer_code,b.customer_name,a.business_type_name,a.sale_amt_refund,
	a.goods_code,c.goods_name,a.rn
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.goods_code,c.order_sale_amt,a.sale_amt_refund,b.days_cnt,a.min_sdt_refund,c.order_min_sdt,b.rn
	from
		(
		select
			performance_province_name,customer_code,order_code,original_order_code,business_type_name,goods_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='广东省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
		group by 
			performance_province_name,customer_code,order_code,original_order_code,business_type_name,goods_code
		) a
		join
			(
			select
				goods_code,business_type_name,sale_amt,days_cnt,rn
			from
				(
				select
					goods_code,business_type_name,sum(sale_amt) as sale_amt,count(distinct sdt) as days_cnt,row_number()over(partition by business_type_name order by sum(sale_amt)) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230228'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='广东省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
				group by 
					goods_code,business_type_name
				) a 
			where
				rn<=30
			) b on b.goods_code=a.goods_code and b.business_type_name=a.business_type_name
		left join
			(
			select
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code) as order_code,
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code) as order_code,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230228' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code)
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
	a.business_type_name,a.rn,a.sale_amt_refund )
union all
(select
	'20221001-20230131' as qijian,
	a.performance_province_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.customer_code,b.customer_name,a.business_type_name,a.sale_amt_refund,
	a.goods_code,c.goods_name,a.rn
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.goods_code,c.order_sale_amt,a.sale_amt_refund,b.days_cnt,a.min_sdt_refund,c.order_min_sdt,b.rn
	from
		(
		select
			performance_province_name,customer_code,order_code,original_order_code,'B端合并' as business_type_name,goods_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='广东省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and business_type_code not in (6)
		group by 
			performance_province_name,customer_code,order_code,original_order_code,goods_code
		) a
		join
			(
			select
				goods_code,sale_amt,days_cnt,rn
			from
				(
				select
					goods_code,sum(sale_amt) as sale_amt,count(distinct sdt) as days_cnt,row_number()over(order by sum(sale_amt)) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230228'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='广东省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
					and business_type_code not in (6)
				group by 
					goods_code
				) a 
			where
				rn<=30
			) b on b.goods_code=a.goods_code
		left join
			(
			select
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code) as order_code,
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code) as order_code,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230228' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
				and business_type_code not in (6)
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code)
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
	a.business_type_name,a.rn,a.sale_amt_refund )	
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04


-- 广东退货数据需求 退货间隔超90天以上TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05
as
select
	'20221001-20230131' as qijian,
	a.performance_province_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.customer_code,b.customer_name,a.business_type_name,a.sale_amt_refund,
	a.diff_days
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.sale_amt_refund,a.min_sdt_refund,c.order_min_sdt,
		datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,customer_code,order_code,original_order_code,business_type_name,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='广东省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and business_type_code not in (6)
		group by 
			performance_province_name,customer_code,order_code,original_order_code,business_type_name
		) a
		left join
			(
			select
				order_code,business_type_name,sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230228' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
				and business_type_code not in (6)
			group by 
				order_code,business_type_name
			) c on c.order_code=a.original_order_code and c.business_type_name=a.business_type_name	
	where
		datediff(a.min_sdt_refund_date,c.order_min_date)>90
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
	a.diff_days desc
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05


-- 广东退货数据需求 退货频次TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06
as
select
	'20221001-20230131' as qijian,
	a.performance_province_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.order_sale_amt,a.customer_code,b.customer_name,a.business_type_name,
	a.sale_amt_refund,a.diff_days,a.days_cnt,a.rn
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,c.order_min_date,a.sale_amt_refund,b.days_cnt,
		a.min_sdt_refund,c.order_min_sdt,c.order_sale_amt,b.rn,datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,customer_code,order_code,original_order_code,business_type_name,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='广东省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
		group by 
			performance_province_name,customer_code,order_code,original_order_code,business_type_name
		) a
		join
			(
			select
				customer_code,business_type_name,sale_amt,days_cnt,rn
			from
				(
				select
					customer_code,business_type_name,sum(sale_amt) as sale_amt,count(distinct sdt) as days_cnt,
					row_number()over(partition by business_type_name order by count(distinct sdt) desc) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230228'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='广东省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
				group by 
					customer_code,business_type_name
				) a 
			where
				rn<=30
			) b on b.customer_code=a.customer_code and b.business_type_name=a.business_type_name
		left join
			(
			select
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code) as order_code,
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				business_type_name,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230228' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code),
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code),
				business_type_name
				
			) c on c.order_code=a.original_order_code and c.business_type_name=a.business_type_name
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
	a.business_type_name,a.rn 
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06


-- 广东退货数据需求 累计退货TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07
as
select
	'20221001-20230131' as qijian,
	a.performance_province_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.order_sale_amt,a.customer_code,b.customer_name,a.business_type_name,
	a.sale_amt_refund,a.diff_days,a.days_cnt,a.rn
from
	(
	select
		a.performance_province_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,c.order_min_date,a.sale_amt_refund,b.days_cnt,
		a.min_sdt_refund,c.order_min_sdt,c.order_sale_amt,b.rn,datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,customer_code,order_code,original_order_code,business_type_name,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='广东省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
		group by 
			performance_province_name,customer_code,order_code,original_order_code,business_type_name
		) a
		join
			(
			select
				customer_code,business_type_name,sale_amt,days_cnt,rn
			from
				(
				select
					customer_code,business_type_name,sum(sale_amt) as sale_amt,count(distinct sdt) as days_cnt,
					row_number()over(partition by business_type_name order by sum(sale_amt)) as rn
				from
					csx_dws.csx_dws_sale_detail_di
				where 
					sdt>='20221001' and sdt<='20230228'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='广东省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
				group by 
					customer_code,business_type_name
				) a 
			where
				rn<=30
			) b on b.customer_code=a.customer_code and b.business_type_name=a.business_type_name
		left join
			(
			select
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code) as order_code,
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				business_type_name,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20211001' and sdt<='20230228' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code),business_type_name
			) c on c.order_code=a.original_order_code and c.business_type_name=a.business_type_name
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
	a.business_type_name,a.rn 
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07

-- ===============================================================================================================================================================
-- 广东 项目供应商需求 销售金额维度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08
as
select
	'20221001-20230228' as qijian,
	a.performance_province_name,a.order_code,a.sdt,a.customer_code,b.customer_name,a.business_type_name,a.sale_amt,c.rn
from
	(
	select
		performance_province_name,customer_code,order_code,sdt,business_type_name,sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230228'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and performance_province_name='广东省'
		and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,customer_code,order_code,sdt,business_type_name
	) a 
	join
		(
		select
			customer_code,sale_amt,rn
		from
			(
			select
				customer_code,sum(sale_amt) as sale_amt,row_number()over(order by sum(sale_amt) desc) as rn
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20221001' and sdt<='20230228'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
				and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_code
			) a 
		where
			rn<=30
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
order by
	c.rn
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08


-- ===============================================================================================================================================================
-- 广东 直送
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09
as
select
	'20221001-20230228' as qijian,
	a.performance_province_name,a.order_code,a.sdt,a.customer_code,d.customer_name,a.business_type_name,a.sale_amt as zs_sale_amt,a.delivery_type_name,
	c.sale_amt,b.sale_amt_total,c.sale_amt/b.sale_amt_total as rate,c.rn
from
	(
	select
		performance_province_name,customer_code,order_code,sdt,business_type_name,delivery_type_name,sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230228'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and performance_province_name='广东省'
		and delivery_type_code=2 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and business_type_code in (1,2) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,customer_code,order_code,sdt,business_type_name,delivery_type_name
	) a 
	join
		(
		select
			customer_code,business_type_name,sale_amt,rn
		from
			(
			select
				customer_code,business_type_name,sum(sale_amt) as sale_amt,row_number()over(partition by business_type_name order by sum(sale_amt) desc) as rn
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20221001' and sdt<='20230228'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
				and delivery_type_code=2 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
				and business_type_code in (1,2) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_code,business_type_name
			) a 
		where
			rn<=30
		) c on c.customer_code=a.customer_code and c.business_type_name=a.business_type_name
	left join
		(
		select
			customer_code,business_type_name,sum(sale_amt) as sale_amt_total
		from
			csx_dws.csx_dws_sale_detail_di
		where 
			sdt>='20221001' and sdt<='20230228'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='广东省'
			-- and delivery_type_code=2 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
			-- and business_type_code in (1,2) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		group by 
			customer_code,business_type_name
		) b on b.customer_code=a.customer_code and b.business_type_name=a.business_type_name
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code				
order by
	c.rn
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09

-- ===============================================================================================================================================================
-- 广东 自提业务需求
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_10;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_10
as
select
	'20221001-20230228' as qijian,
	a.performance_province_name,a.order_code,a.sdt,a.customer_code,d.customer_name,a.goods_code,e.goods_name,a.sale_amt,a.delivery_type_name,a.business_type_name,
	d.first_category_name,d.second_category_name,d.third_category_name,c.rn
from
	(
	select
		performance_province_name,customer_code,order_code,sdt,business_type_name,delivery_type_name,goods_code,sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230228'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and performance_province_name='广东省'
		and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,customer_code,order_code,sdt,business_type_name,delivery_type_name,goods_code
	) a 
	join
		(
		select
			customer_code,business_type_name,sale_amt,rn
		from
			(
			select
				customer_code,business_type_name,sum(sale_amt) as sale_amt,row_number()over(partition by business_type_name order by sum(sale_amt) desc) as rn
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20221001' and sdt<='20230228'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
				and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
				and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_code,business_type_name
			) a 
		where
			rn<=30
		) c on c.customer_code=a.customer_code and c.business_type_name=a.business_type_name
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code,
			sales_user_number,sales_user_name,customer_address_full
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) d on d.customer_code=a.customer_code	
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) e on e.goods_code=a.goods_code		
order by
	c.rn
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_10
;

select
	'20221001-20230228' as qijian,
	a.performance_province_name,a.order_code,a.sdt,a.customer_code,d.customer_name,a.goods_code,e.goods_name,a.sale_amt,a.delivery_type_name,a.business_type_name,
	d.first_category_name,d.second_category_name,d.third_category_name,c.rn
from
	(
	select
		performance_province_name,customer_code,order_code,sdt,'自营合计' as business_type_name,delivery_type_name,goods_code,sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20221001' and sdt<='20230228'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and performance_province_name='广东省'
		and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,customer_code,order_code,sdt,delivery_type_name,goods_code
	) a 
	join
		(
		select
			customer_code,sale_amt,rn
		from
			(
			select
				customer_code,sum(sale_amt) as sale_amt,row_number()over( order by sum(sale_amt) desc) as rn
			from
				csx_dws.csx_dws_sale_detail_di
			where 
				sdt>='20221001' and sdt<='20230228'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='广东省'
				and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
				and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_code
			) a 
		where
			rn<=30
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
		) d on d.customer_code=a.customer_code	
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) e on e.goods_code=a.goods_code		
order by
	c.rn



