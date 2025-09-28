-- 临时表
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
as
select
	*
from
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>='20211001' and sdt<='20230630'
	and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	and performance_province_name='浙江省'
;
-- 大单品 业务类型合计
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_01
as
select
	'20221001-20230630' as qijian,a.performance_province_name,a.performance_city_name,a.business_type_name,a.delivery_type_name,
	a.order_code,a.order_sale_date,
	-- a.order_sale_amt/a.order_sale_qty as order_sale_price,
	-- a.order_sale_qty,a.sale_price,
	a.order_sale_amt,a.customer_code,b.customer_name,a.goods_code,c.goods_name,a.sale_amt,a.rn,a.unit_name,a.sale_qty
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.business_type_name,a.delivery_type_name,
		a.customer_code,a.order_code,a.goods_code,a.sale_amt,b.rn,c.order_sale_qty,c.order_sale_amt,c.order_sale_date,sale_qty,a.sale_price,a.unit_name
	from
		(
		select
			performance_province_name,customer_code,order_code,goods_code,unit_name,sum(sale_amt) as sale_amt,sum(sale_qty) as sale_qty,sale_price,
			performance_city_name,business_type_name,delivery_type_name
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and business_type_code not in (4,6) -- 不含项目供应商和BBC
			and performance_province_name='浙江省'
		group by 
			performance_province_name,customer_code,order_code,goods_code,unit_name,sale_price,performance_city_name,business_type_name,delivery_type_name
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
					csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
				where 
					sdt>='20221001' and sdt<='20230630'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and business_type_code not in (4,6) -- 不含项目供应商和BBC
					and performance_province_name='浙江省'
				group by 
					goods_code	
				) a 
			where
				rn<=40
			) b on b.goods_code=a.goods_code
		left join
			(
			select
				order_code,sum(sale_amt) as order_sale_amt,min(sdt) as order_sale_date,sum(sale_qty) as order_sale_qty
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20221001' and sdt<='20230630'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and business_type_code not in (4,6) -- 不含项目供应商和BBC
				and performance_province_name='浙江省'
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


-- 大单品 业务类型
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02
as
select
	'20221001-20230630' as qijian,a.performance_province_name,a.performance_city_name,a.business_type_name,a.delivery_type_name,
	a.order_code,a.order_sale_date,
	-- a.order_sale_amt/a.order_sale_qty as order_sale_price,
	-- a.order_sale_qty,a.sale_price,
	a.order_sale_amt,a.customer_code,b.customer_name,a.goods_code,c.goods_name,a.sale_amt,a.rn,a.unit_name,a.sale_qty
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.business_type_name,a.delivery_type_name,
		a.customer_code,a.order_code,a.goods_code,a.sale_amt,b.rn,c.order_sale_qty,c.order_sale_amt,c.order_sale_date,sale_qty,a.sale_price,a.unit_name
	from
		(
		select
			performance_province_name,customer_code,order_code,goods_code,unit_name,sum(sale_amt) as sale_amt,sum(sale_qty) as sale_qty,sale_price,
			performance_city_name,business_type_name,delivery_type_name
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			-- and business_type_code not in (4,6) -- 不含项目供应商和BBC
			and performance_province_name='浙江省'
		group by 
			performance_province_name,customer_code,order_code,goods_code,unit_name,sale_price,performance_city_name,business_type_name,delivery_type_name
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
					csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
				where 
					sdt>='20221001' and sdt<='20230630'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					-- and business_type_code not in (4,6) -- 不含项目供应商和BBC
					and performance_province_name='浙江省'
				group by 
					goods_code	
				) a 
			where
				rn<=40
			) b on b.goods_code=a.goods_code
		left join
			(
			select
				order_code,sum(sale_amt) as order_sale_amt,min(sdt) as order_sale_date,sum(sale_qty) as order_sale_qty
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20221001' and sdt<='20230630'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				-- and business_type_code not in (4,6) -- 不含项目供应商和BBC
				and performance_province_name='浙江省'
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
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_02

-- ===============================================================================================================================================================
-- 退货 单笔退货金额TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_03
as
select
	-- '20221001-20230131' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.order_sale_amt,a.customer_code,b.customer_name,a.business_type_name,a.sale_amt_refund,
	a.goods_code,c.goods_name,a.diff_days,a.days_cnt,a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.goods_code,c.order_sale_amt,a.sale_amt_refund,d.days_cnt,a.min_sdt_refund,c.order_min_sdt,b.rn,
		datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name,goods_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and order_channel_code not in(4,6)
		group by 
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name,goods_code
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
					csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
				where 
					sdt>='20221001' and sdt<='20230630'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='浙江省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
					and order_channel_code not in(4,6)
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
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code) as order_code,
				original_order_code,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,
				from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20211001' and sdt<='20230630' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
				and original_order_code !=''
				and performance_province_name='浙江省'
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code)
				original_order_code
			) c on c.original_order_code=a.original_order_code
		left join
			(
			select
				customer_code,count(distinct sdt) as days_cnt
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20221001' and sdt<='20230630'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='浙江省'
				and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
				and order_channel_code not in(4,6)
			group by 
				customer_code
			) d on d.customer_code=a.customer_code
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


-- 退货 退货单品TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_04
as
(
select
	-- '20221001-20230131' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_sale_amt,a.order_min_sdt,a.customer_code,b.customer_name,a.business_type_name,a.sale_qty_refund,a.sale_amt_refund,a.unit_name,
	a.goods_code,c.goods_name,a.diff_days,a.days_cnt,a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.goods_code,c.order_sale_amt,a.unit_name,a.sale_amt_refund,a.sale_qty_refund,b.days_cnt,a.min_sdt_refund,c.order_min_sdt,b.rn,
		datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name,goods_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,sum(sale_qty) as sale_qty_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date,unit_name
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and order_channel_code not in(4,6)
		group by 
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name,goods_code,unit_name
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
					csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
				where 
					sdt>='20221001' and sdt<='20230630'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='浙江省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
					and order_channel_code not in(4,6)
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
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code) as order_code,
				original_order_code,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,
				from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20211001' and sdt<='20230630' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and original_order_code !=''
				and performance_province_name='浙江省'
				and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code)
				original_order_code
			) c on c.original_order_code=a.original_order_code
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
	-- '20221001-20230131' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_sale_amt,a.order_min_sdt,a.customer_code,b.customer_name,a.business_type_name,a.sale_qty_refund,a.sale_amt_refund,a.unit_name,
	a.goods_code,c.goods_name,a.diff_days,a.days_cnt,a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.goods_code,c.order_sale_amt,a.unit_name,a.sale_amt_refund,a.sale_qty_refund,b.days_cnt,a.min_sdt_refund,c.order_min_sdt,b.rn,
		datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,'B端合并' as business_type_name,goods_code,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,sum(sale_qty) as sale_qty_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date,unit_name
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and business_type_code not in (6)
			and order_channel_code not in(4,6)
		group by 
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,goods_code,unit_name
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
					csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
				where 
					sdt>='20221001' and sdt<='20230630'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='浙江省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
					and business_type_code not in (6)
					and order_channel_code not in(4,6)
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
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code) as order_code,
				original_order_code,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,
				from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20211001' and sdt<='20230630' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and original_order_code !=''
				and performance_province_name='浙江省'
				and business_type_code not in (6)
				and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',substr(order_code,2,16),order_code)
				original_order_code
			) c on c.original_order_code=a.original_order_code
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


-- 退货 退货间隔超90天以上TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_05
as
select
	-- '20221001-20230131' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.order_sale_amt,a.customer_code,b.customer_name,a.business_type_name,a.sale_amt_refund,
	a.diff_days,a.days_cnt
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,a.sale_amt_refund,a.min_sdt_refund,c.order_min_sdt,
		datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days,c.order_sale_amt,a.days_cnt
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date,count(distinct sdt) as days_cnt
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and order_channel_code not in(4,6)
		group by 
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name
		) a
		left join
			(
			select
				original_order_code,business_type_name,sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20211001' and sdt<='20230630' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
				and original_order_code !=''
				and performance_province_name='浙江省'
			group by 
				original_order_code,business_type_name
			) c on c.original_order_code=a.original_order_code and c.business_type_name=a.business_type_name	
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


-- 退货 退货频次TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_06
as
select
	-- '20221001-20230131' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.order_sale_amt,a.customer_code,b.customer_name,a.business_type_name,
	a.sale_qty_refund,a.sale_amt_refund,a.diff_days,a.days_cnt,a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,c.order_min_date,a.sale_amt_refund,b.days_cnt,a.sale_qty_refund,
		a.min_sdt_refund,c.order_min_sdt,c.order_sale_amt,b.rn,datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,sum(sale_qty) as sale_qty_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and order_channel_code not in(4,6)
		group by 
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name
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
					csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
				where 
					sdt>='20221001' and sdt<='20230630'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='浙江省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
					and order_channel_code not in(4,6)
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
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				original_order_code,
				business_type_name,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20211001' and sdt<='20230630' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
				and original_order_code !=''
				and performance_province_name='浙江省'
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code),
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code),
				original_order_code,
				business_type_name
			) c on c.original_order_code=a.original_order_code and c.business_type_name=a.business_type_name
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


-- 退货 累计退货TOP30
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_07
as
select
	-- '20221001-20230131' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.min_sdt_refund,a.original_order_code,a.order_min_sdt,a.order_sale_amt,a.customer_code,b.customer_name,a.business_type_name,
	a.sale_qty_refund,a.sale_amt_refund,a.diff_days,a.days_cnt,a.rn
from
	(
	select
		a.performance_province_name,a.performance_city_name,a.customer_code,a.order_code,a.original_order_code,a.business_type_name,c.order_min_date,a.sale_amt_refund,a.sale_qty_refund,b.days_cnt,
		a.min_sdt_refund,c.order_min_sdt,c.order_sale_amt,b.rn,datediff(a.min_sdt_refund_date,c.order_min_date) as diff_days
	from
		(
		select
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name,min(sdt) as min_sdt_refund,sum(sale_amt) as sale_amt_refund,sum(sale_qty) as sale_qty_refund,
			from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as min_sdt_refund_date
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and order_channel_code not in(4,6)
		group by 
			performance_province_name,performance_city_name,customer_code,order_code,original_order_code,business_type_name
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
					csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
				where 
					sdt>='20221001' and sdt<='20230630'
					and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
					and performance_province_name='浙江省'
					and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
					and order_channel_code not in(4,6)
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
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code) as order_code,
				original_order_code,
				business_type_name,
				sum(sale_amt) as order_sale_amt,min(sdt) as order_min_sdt,from_unixtime(unix_timestamp(min(sdt),'yyyyMMdd'),'yyyy-MM-dd') as order_min_date
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20211001' and sdt<='20230630' -- 多往前追1年
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
				and original_order_code !=''
				and performance_province_name='浙江省'
			group by 
				-- if(business_type_name='BBC',if(order_code='B2211180606585688A',order_code,substr(order_code,2,16)),order_code)
				-- if(business_type_name='BBC',if(substr(order_code,1,1)='B',order_code,substr(order_code,2,16)),order_code),
				original_order_code,
				business_type_name
			) c on c.original_order_code=a.original_order_code and c.business_type_name=a.business_type_name
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
-- 项目供应商 销售金额维度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_08
as
select
	'20221001-20230630' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.refund_order_flag_name,a.sale_amt,a.sdt,a.customer_code,b.customer_name,
	a.business_type_name,
	-- coalesce(d.min_order_code,'') as min_order_code,coalesce(d.order_min_month,'') as order_min_month,coalesce(d.order_sale_amt,0) as order_sale_amt,
	c.rn,c.days_cnt
from
	(
	select
		performance_province_name,performance_city_name,customer_code,order_code,sdt,business_type_name,sum(sale_amt) as sale_amt,original_order_code,
		if(refund_order_flag=1,'逆向单','正向单') as refund_order_flag_name
	from
		csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
	where 
		sdt>='20221001' and sdt<='20230630'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and performance_province_name='浙江省'
		and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,performance_city_name,customer_code,order_code,sdt,business_type_name,original_order_code,if(refund_order_flag=1,'逆向单','正向单')
	) a 
	join
		(
		select
			customer_code,sale_amt,rn,days_cnt
		from
			(
			select
				customer_code,sum(sale_amt) as sale_amt,row_number()over(order by sum(sale_amt) desc) as rn,count(distinct sdt) as days_cnt
			from
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20221001' and sdt<='20230630'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='浙江省'
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
			original_order_code,min(order_code) as min_order_code,
			sum(sale_amt) as order_sale_amt,substr(min(sdt),1,6) as order_min_month
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20211001' and sdt<='20230630' -- 多往前追1年
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			and refund_order_flag=1 -- 退货订单标识(0-正向单 1-逆向单)
			and performance_province_name='浙江省'
		group by 
			original_order_code
		) d on d.original_order_code=a.original_order_code
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
-- 直送
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_09
as
select
	'20221001-20230630' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.sdt,a.customer_code,d.customer_name,a.business_type_name,a.sale_amt as zs_sale_amt,a.delivery_type_name,
	c.sale_amt,b.sale_amt_total,c.sale_amt/b.sale_amt_total as rate,c.rn
from
	(
	select
		performance_province_name,performance_city_name,customer_code,order_code,sdt,business_type_name,delivery_type_name,sum(sale_amt) as sale_amt
	from
		csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
	where 
		sdt>='20221001' and sdt<='20230630'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and performance_province_name='浙江省'
		and delivery_type_code=2 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and business_type_code in (1,2,3,5,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,performance_city_name,customer_code,order_code,sdt,business_type_name,delivery_type_name
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
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20221001' and sdt<='20230630'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='浙江省'
				and delivery_type_code=2 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
				and business_type_code in (1,2,3,5,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_code,business_type_name
			) a 
		where
			--1=1
			rn<=30
		) c on c.customer_code=a.customer_code and c.business_type_name=a.business_type_name
	left join
		(
		select
			customer_code,business_type_name,sum(sale_amt) as sale_amt_total
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
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
;
-- ===============================================================================================================================================================
-- 自提
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_11;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_11
as
select
	'20221001-20230630' as qijian,
	a.performance_province_name,a.performance_city_name,a.order_code,a.sdt,a.customer_code,d.customer_name,a.sale_qty,a.unit_name,a.goods_code,e.goods_name,a.sale_amt,a.delivery_type_name,a.business_type_name,
	d.first_category_name,d.second_category_name,d.third_category_name,c.rn
from
	(
	select
		performance_province_name,performance_city_name,customer_code,order_code,sdt,business_type_name,delivery_type_name,goods_code,sum(sale_amt) as sale_amt,sum(sale_qty) as sale_qty,unit_name
	from
		csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
	where 
		sdt>='20221001' and sdt<='20230630'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and performance_province_name='浙江省'
		and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		-- and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,performance_city_name,customer_code,order_code,sdt,business_type_name,delivery_type_name,goods_code,unit_name
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
				csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
			where 
				sdt>='20221001' and sdt<='20230630'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and performance_province_name='浙江省'
				and delivery_type_code=3 -- 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
				-- and business_type_code not in (4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
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
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_11
;

-- ===============================================================================================================================================================
-- 调价 
-- 客户返利管理：返利原因不要：合同规定返利
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_12;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_12
as
select
	b.performance_province_name,a.tf_order_code,a.original_order_code,
	case when a.business_type='NORMAL' then '日配业务' 
		when a.business_type='WELFARE' then '福利业务' 
		when a.business_type='BBC' then 'BBC'
		when a.business_type='BIGAMOUNT_TRADE' then '省区大宗'
		when a.business_type='INNER' then '批发内购' else business_type end as business_type_name,
	a.tf_adjust_type_name,a.flag,a.sign_company_name,a.customer_code,a.customer_name,a.tf_date,a.tf_sales_value,c.sale_amt,
	abs(a.tf_sales_value/c.sale_amt) as rate,abs(a.tf_sales_value) as abs_tf_sales_value
from
	(
	select
		'调价单' as flag,adjust_price_order_code as tf_order_code,to_date(adjust_time) as tf_date,customer_code,customer_name,sign_company_code,sign_company_name,adjust_type_name as tf_adjust_type_name,
		business_type,original_order_code,sum(sales_value) as tf_sales_value
	from
		csx_dwd.csx_dwd_sss_customer_adjust_price_detail_di
	where
		sdt>='20200101'
		and audit_status_code=1 -- 审核状态编码:(0:待审核 1:审核通过 2:审核拒绝)
		and to_date(adjust_time) between '2022-10-01' and '2023-06-30'
	group by 
		adjust_price_order_code,to_date(adjust_time),customer_code,customer_name,sign_company_code,sign_company_name,adjust_type_name,business_type,original_order_code
		
	union all 

	select
		'返利单' as flag,rebate_order_code as tf_order_code,to_date(rebate_time) as tf_date,customer_code,customer_name,sign_company_code,sign_company_name,rebate_type_name as tf_adjust_type_name,
		business_type,original_order_code,sum(total_rebate_amount) as tf_sales_value
	from
		csx_dwd.csx_dwd_sss_customer_rebate_detail_di
	where
		sdt>='20200101'
		and audit_status_code='1' -- 审核状态编码:(0:待审核 1:审核通过 2:审核拒绝)
		and to_date(rebate_time) between '2022-10-01' and '2023-06-30'
		and rebate_reason_code !=0 -- 返利原因编码:(0:合同规定返利 1:促销扣款 2:调整尾差)
	group by 
		rebate_order_code,to_date(rebate_time),customer_code,customer_name,sign_company_code,sign_company_name,rebate_type_name,business_type,original_order_code
	) a 
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
			and performance_province_name='浙江省'
		) b on b.customer_code=a.customer_code	
	left join
		(
		select
			customer_code,sum(sale_amt) as sale_amt
		from
			csx_analyse_tmp.csx_analyse_tmp_finance_shenji_sale_detail
		where 
			sdt>='20221001' and sdt<='20230630'
			and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
			and performance_province_name='浙江省'
		group by 
			customer_code
		) c on c.customer_code=a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_12;

drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_shenji_15;
create table csx_analyse_tmp.csx_analyse_tmp_finance_shenji_15
as
select
	b.performance_province_name,a.tf_order_code,a.original_order_code,a.goods_code,a.goods_name,
	case when a.business_type='NORMAL' then '日配业务' 
		when a.business_type='WELFARE' then '福利业务' 
		when a.business_type='BBC' then 'BBC'
		when a.business_type='BIGAMOUNT_TRADE' then '省区大宗'
		when a.business_type='INNER' then '批发内购' else business_type end as business_type_name,
	a.tf_adjust_type_name,a.flag,a.sign_company_name,a.customer_code,a.customer_name,a.tf_date,a.tf_sales_value
from
	(
	select
		'调价单' as flag,adjust_price_order_code as tf_order_code,to_date(adjust_time) as tf_date,customer_code,customer_name,sign_company_code,sign_company_name,adjust_type_name as tf_adjust_type_name,
		business_type,original_order_code,goods_code,goods_name,sum(sales_value) as tf_sales_value
	from
		csx_dwd.csx_dwd_sss_customer_adjust_price_detail_di
	where
		sdt>='20200101'
		and audit_status_code=1 -- 审核状态编码:(0:待审核 1:审核通过 2:审核拒绝)
		and to_date(adjust_time) between '2022-10-01' and '2023-04-30'
	group by 
		adjust_price_order_code,to_date(adjust_time),customer_code,customer_name,sign_company_code,sign_company_name,adjust_type_name,business_type,original_order_code,goods_code,goods_name
	) a 
	join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name,sign_company_code
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
			and performance_province_name in('浙江省','江苏苏州','江苏南京')
		) b on b.customer_code=a.customer_code	
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_shenji_15;

-- ===================================================================================================================================================================
-- TOP20项目供应商销售
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_01;
create table csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_01
as
select
	a.customer_code,b.customer_name,a.sale_amt,a.sdt,a.business_type_name,a.performance_province_name,c.rn
from
	(
	select
		performance_province_name,customer_code,sdt,business_type_name,sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230701' and sdt<='20230930'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,customer_code,sdt,business_type_name
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
				sdt>='20230701' and sdt<='20230930'
				and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
				and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_code
			) a 
		where
			rn<=20
		) c on c.customer_code=a.customer_code
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code	
order by
	c.rn
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_01;

-- 项目供应商退货率
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_02;
create table csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_02
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.smonth,a.business_type_name,a.order_type,a.order_channel_name,a.sale_amt
from
	(
	select
		performance_province_name,customer_code,substr(sdt,1,6) as smonth,business_type_name,
		case when refund_order_flag=0 then '正向单' else '逆向单' end as order_type,
		case when order_channel_code in (4,6) then '调价返利' else '非调价返利' end as order_channel_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230701' and sdt<='20230930'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code=4 -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,customer_code,substr(sdt,1,6) ,business_type_name,
		case when refund_order_flag=0 then '正向单' else '逆向单' end ,
		case when order_channel_code in (4,6) then '调价返利' else '非调价返利' end 
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code	
order by
	a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_02;

-- 自营退货率
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_03;
create table csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_03
as
select
	a.performance_province_name,a.customer_code,b.customer_name,a.smonth,a.business_type_name,a.order_type,a.order_channel_name,a.sale_amt
from
	(
	select
		performance_province_name,customer_code,substr(sdt,1,6) as smonth,business_type_name,
		case when refund_order_flag=0 then '正向单' else '逆向单' end as order_type,
		case when order_channel_code in (4,6) then '调价返利' else '非调价返利' end as order_channel_name,
		sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230701' and sdt<='20230930'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
		and business_type_code in (1,2,3,5) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
	group by 
		performance_province_name,customer_code,substr(sdt,1,6) ,business_type_name,
		case when refund_order_flag=0 then '正向单' else '逆向单' end ,
		case when order_channel_code in (4,6) then '调价返利' else '非调价返利' end 
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,performance_city_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code	
order by
	a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_03;

-- Q2-Q3季度品类及行业分析
drop table if exists csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_04;
create table csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_04
as
select
	d.quarter_of_year,a.customer_code,b.customer_name,a.performance_province_name,a.channel_name,a.business_type_name,
	b.first_category_name,b.second_category_name,b.third_category_name,c.business_division_name,
	c.purchase_group_code,c.purchase_group_name,c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,c.classify_small_name,
	sum(sale_amt) as sale_amt,sum(sale_amt_no_tax)as sale_amt_no_tax,sum(sale_cost) as sale_cost,sum(sale_cost_no_tax) as sale_cost_no_tax,
	sum(profit) as profit,sum(profit_no_tax) as profit_no_tax
from
	(
	select
		*
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230401' and sdt<='20230930'
		and channel_code in('1','7','9') -- 渠道编码(1.大客户 2.商超 4.大宗 5.供应链(食百) 6.供应链(生鲜) 8.其他 9.业务代理)
	) a 
	left join
		(
		select
			customer_code,customer_name,performance_region_name,performance_province_name,first_category_name,second_category_name,third_category_name
		from
			csx_dim.csx_dim_crm_customer_info
		where
			sdt='current'
		) b on b.customer_code=a.customer_code
	left join
		(
		select
			goods_code,goods_name,classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,business_division_name,purchase_group_code,purchase_group_name,unit_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) c on c.goods_code=a.goods_code	
	left join
		(
		select
			calday,quarter_of_year,csx_week,csx_week_begin,csx_week_end,month_of_year
		from
			csx_dim.csx_dim_basic_date
		) d on d.calday=a.sdt
group by 
	performance_province_name,customer_code,substr(sdt,1,6),channel_name,business_type_name,goods_code,delivery_type_name
order by 
	a.customer_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_finance_jiduhui_04;
