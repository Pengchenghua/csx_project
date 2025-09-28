drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_price_type_01;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_price_type_01 as 
select * from 
(select 
	t1.*,
	case t1.price_type
		when '客户定价->合同固定价' then '客户定价'
		when '客户定价->单独议价' then '客户定价'
		when '对标定价->全对标' then '对标定价'
		when '' then '自主定价'
		when null then '自主定价'
		when '空' then '自主定价'
		when '自主定价->采购或车间定价' then '自主定价'
		when '临时报价->单品项' then '临时定价'
		when '临时报价->下单时' then '临时定价'
		when '客户定价->多方比价' then '客户定价'
		when '对标定价->半对标' then '对标定价'
		when '自主定价->建议售价' then '自主定价'
		end as price_type1,	
	case t1.price_type
		when '客户定价->合同固定价' then '合同固定价'
		when '客户定价->单独议价' then '单独议价'
		when '对标定价->全对标' then '全对标'
		when '' then '建议售价'
		when null then '建议售价'
		when '空' then '建议售价'
		when '自主定价->采购或车间定价' then '建议售价'   
		when '临时报价->单品项' then '临时定价'
		when '临时报价->下单时' then '临时定价'
		when '客户定价->多方比价' then '多方比价'
		when '对标定价->半对标' then '半对标'
		when '自主定价->建议售价' then '建议售价'
	end as price_type2 
from 
(select 
    customer_code,
    (case when a.price_period_code=1 then '每日' 
          when a.price_period_code=2 then '每周' 
          when a.price_period_code=3 then '每半月' 
          when a.price_period_code=4 then '每月' end) as price_period_name,-- 报价周期 
    price_date_name,
	concat(a.price_set_type_first,'->',a.price_set_type_sec) as price_type -- 定价类型 
from 
    (select 
        *,
        (case when split(price_set_type,',')[0]='1' then '对标定价' 
              when split(price_set_type,',')[0]='4' then '客户定价' 
              when split(price_set_type,',')[0]='8' then '自主定价' 
              when split(price_set_type,',')[0]='11' then '临时报价' 
              when split(price_set_type,',')[0]='2' then '全对标' 
              when split(price_set_type,',')[0]='3' then '半对标' 
              when split(price_set_type,',')[0]='5' then '合同固定价' 
              when split(price_set_type,',')[0]='6' then '多方比价' 
              when split(price_set_type,',')[0]='7' then '单独议价' 
              when split(price_set_type,',')[0]='9' then '采购或车间定价' 
              when split(price_set_type,',')[0]='10' then '建议售价' 
              when split(price_set_type,',')[0]='12' then '下单时' 
              when split(price_set_type,',')[0]='13' then '单品项' 
        end) as price_set_type_first,
        (case when split(price_set_type,',')[1]='1' then '对标定价' 
              when split(price_set_type,',')[1]='4' then '客户定价' 
              when split(price_set_type,',')[1]='8' then '自主定价' 
              when split(price_set_type,',')[1]='11' then '临时报价' 
              when split(price_set_type,',')[1]='2' then '全对标' 
              when split(price_set_type,',')[1]='3' then '半对标' 
              when split(price_set_type,',')[1]='5' then '合同固定价' 
              when split(price_set_type,',')[1]='6' then '多方比价' 
              when split(price_set_type,',')[1]='7' then '单独议价' 
              when split(price_set_type,',')[1]='9' then '采购或车间定价' 
              when split(price_set_type,',')[1]='10' then '建议售价' 
              when split(price_set_type,',')[1]='12' then '下单时' 
              when split(price_set_type,',')[1]='13' then '单品项' 
        end) as price_set_type_sec, 
        row_number()over(partition by customer_code order by business_number desc) as ranks 
    from csx_dim.csx_dim_crm_business_info 
    where sdt='current' 
    and business_attribute_code=1 
    and status=1 
    -- and sign_type_code=1 
    )a 
where a.ranks=1
) t1  
)t1;

drop table if exists csx_analyse_tmp.csx_city_customer_goods_sale;
create table if not exists csx_analyse_tmp.csx_city_customer_goods_sale 
as 
select *
from 
(select
	a.performance_region_name ,
	a.performance_province_name ,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1) price_type1, -- 定价类型1
	nvl(f.sec_price_type,e.price_type2) price_type2, -- 定价类型2
	e.price_period_name , -- 报价周期
	e.price_date_name, -- 报价日
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	h.customer_large_level ,
	c.first_sales_date ,
	b.business_division_name ,
	b.purchase_group_code ,
	b.purchase_group_name ,
	b.classify_large_code ,
	b.classify_large_name ,
	b.classify_middle_code ,
	b.classify_middle_name ,
	b.classify_small_code ,
	b.classify_small_name ,
	a.goods_code ,
	b.goods_name ,
	(case when g.goods_code is not null then '是' else '否' end) shidiao_type,
	sales_type, -- `是否调价`,
	fanli_type, -- `是否返利`,
	tuihuo_type, -- `是否退货`,
	delivery_type_name, -- `物流模式`,
	new_direct_delivery_type as direct_delivery_type, -- as `物流模式细分`,
	price_type,	-- -- 价格类型 1-周期进 2-售价下浮 3-不指定
	assign_supplier,	--  1、客户指定，联营直送、2 供应链指定
	inventory_dc_code,-- `DC编码`,
	types, --as `是否直送仓`,
	fjc, -- `福建剔除仓`,
	nvl(if(c.first_sales_date >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and c.first_sales_date<= regexp_replace(add_months('${i_sdate}',0),'-',''),'新客','老客'),0) as xinlaok,
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_amt end),0) by_sale_amt,
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.sale_qty end),0) by_sale_qty,
	nvl(sum(case when a.sdt >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',0),'-','') then a.profit end),0) by_profit,	
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_amt end),0) sy_sale_amt,
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.sale_qty end),0) sy_sale_qty,
	nvl(sum(case when a.sdt >= regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','') and a.sdt <= regexp_replace(add_months('${i_sdate}',-1),'-','') then a.profit end),0) sy_profit,	
	nvl(sum(case when a.week=weekofyear(date_sub('${i_sdate}', 2)) then a.sale_amt end),0) bz_sale_amt,
	nvl(sum(case when a.week=weekofyear(date_sub('${i_sdate}', 2)) then a.sale_qty end),0) bz_sale_qty,
	nvl(sum(case when a.week=weekofyear(date_sub('${i_sdate}', 2)) then a.profit end),0) bz_profit,	
	nvl(sum(case when a.week=weekofyear(date_sub('${i_sdate}', 9)) then a.sale_amt end),0) sz_sale_amt,
	nvl(sum(case when a.week=weekofyear(date_sub('${i_sdate}', 9)) then a.sale_qty end),0) sz_sale_qty,
	nvl(sum(case when a.week=weekofyear(date_sub('${i_sdate}', 9)) then a.profit end),0) sz_profit
	
from 
	(
	 select 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		sdt,
		substr(sdt,1,6) smonth,
	 	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week, 
		business_type_name,
		business_type_code,
		customer_code,
		if(order_channel_code=6 ,'是','否') sales_type,
		if(order_channel_code=4 ,'是','否') fanli_type,
		if(refund_order_flag=1,'是','否') as tuihuo_type,
		delivery_type_name,
		case 
			when a.delivery_type_name<>'直送' then ''
			when a.direct_delivery_type=1 then 'R直送1'
			when a.direct_delivery_type=2 then 'Z直送2'
			when a.direct_delivery_type=11 then '临时加单'
			when a.direct_delivery_type=12 then '紧急补货'
			when a.direct_delivery_type=0 then '普通' else '普通' 
	    end direct_delivery_type,
		price_type,
	-- 价格类型 1-周期进 2-售价下浮 3-不指定
		assign_type,
	--  1、客户指定，联营直送、2 供应链指定
		assign_supplier,
		case
			when a.delivery_type_name <> '直送' then '配送'
			when (a.direct_delivery_type = 1 or m.assign_type IN (1, 2)) then '计划直送' -- 客户指定包含直送1+采购客户指定，细分优先直送1
			when a.direct_delivery_type in (11, 12, 2) then '紧急直送'
		else 'T+0调度直送'
		end direct_delivery_large_type,

		case
			when a.delivery_type_name <> '直送' then '配送' 
			when a.direct_delivery_type = 0 and m.assign_type = 2 then '供应链指定'
			when(a.direct_delivery_type = 1 or m.assign_type = 1 ) then '客户指定' -- 客户指定包含直送1+采购客户指定，细分优先直送1
			when a.direct_delivery_type = 2 then '客户自购' -- 直送2
			when a.direct_delivery_type = 11 then '临时加单'
			when a.direct_delivery_type = 12 then '紧急补货'
			else 'T+0调度直送'
		end new_direct_delivery_type,	
		if(m.float_ratio <> 0, '是', '否') float_flag
		goods_code,
		a.inventory_dc_code,
		if( c.shop_code is null,'否','是') types,
		if(a.inventory_dc_code in ('W0J2','W0AJ','W0G6','WB71'),'是','否')  as fjc, -- 福建异常仓
		sum(sale_amt)as sale_amt,
		sum(profit)as profit,		
		sum(if(order_channel_detail_code=26,0,sale_qty)) as sale_qty
	 from 
		(select * 
	     from csx_dws.csx_dws_sale_detail_di 
		 left join 
		(select
			agreement_order_code,
			sale_order_code,
			delivery_type,
			goods_code,
			float_ratio,
			-- 为固定扣点,不等于0即为固定扣点
			price_type,
			-- 价格类型 1-周期进 2-售价下浮 3-不指定
			assign_type,
			--  1、客户指定，联营直送、2 供应链指定
			assign_supplier,
			--指定供应商 0-否 1-是
			auto_audit_flag
			-- 自动审核标识 0-否 1-是
		from
		    csx_dwd.csx_dwd_oms_agreement_order_detail_di 
		where sdt >= '20240501'
		) m  on regexp_replace(a.wms_order_code, '-.*', '') = m.agreement_order_code  and a.goods_code = m.goods_code

	     where 
	        sdt >=regexp_replace(add_months(trunc('${i_sdate}','MM'),-1),'-','')   	  
				and sdt<= regexp_replace(add_months('${i_sdate}',0),'-','')  
	        and channel_code <> '2' and substr(customer_code, 1, 1) <> 'S' -- 剔除商超数据 
	        and business_type_code in ('1','2','6') 
			-- and performance_province_name in ('上海松江','浙江省','北京市')
		) a
		left join 
		(select  
			distinct shop_code 
		from csx_dim.csx_dim_shop 
		where sdt='current' 
			and shop_low_profit_flag=1  
		)c
		on a.inventory_dc_code = c.shop_code
     group by 
		performance_region_name,
    	performance_province_name,
		performance_city_name,
		sdt,
		substr(sdt,1,6) ,
	 	weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) , 
		business_type_name,
		business_type_code,
		customer_code,
		if(order_channel_code=6 ,'是','否'),
		if(order_channel_code=4 ,'是','否'),
		if(refund_order_flag=1,'是','否'),
		delivery_type_name,
		case 
			when a.delivery_type_name<>'直送' then ''
			when a.direct_delivery_type=1 then 'R直送1'
			when a.direct_delivery_type=2 then 'Z直送2'
			when a.direct_delivery_type=11 then '临时加单'
			when a.direct_delivery_type=12 then '紧急补货'
			when a.direct_delivery_type=0 then '普通' else '普通' 
	    end,
		goods_code,
		a.inventory_dc_code,
		if( c.shop_code is null,'否','是'),
		if( a.inventory_dc_code in ('W0J2','W0AJ','W0G6','WB71'),'是','否')	,
			price_type,
	-- 价格类型 1-周期进 2-售价下浮 3-不指定
		assign_type,
	--  1、客户指定，联营直送、2 供应链指定
		assign_supplier,
		case
			when a.delivery_type_name <> '直送' then '配送'
			when (a.direct_delivery_type = 1 or m.assign_type IN (1, 2)) then '计划直送' -- 客户指定包含直送1+采购客户指定，细分优先直送1
			when a.direct_delivery_type in (11, 12, 2) then '紧急直送'
		else 'T+0调度直送'
		end  ,

		case
			when a.delivery_type_name <> '直送' then '配送' 
			when a.direct_delivery_type = 0 and m.assign_type = 2 then '供应链指定'
			when(a.direct_delivery_type = 1 or m.assign_type = 1 ) then '客户指定' -- 客户指定包含直送1+采购客户指定，细分优先直送1
			when a.direct_delivery_type = 2 then '客户自购' -- 直送2
			when a.direct_delivery_type = 11 then '临时加单'
			when a.direct_delivery_type = 12 then '紧急补货'
			else 'T+0调度直送'
		end  ,	
		if(m.float_ratio <> 0, '是', '否')
	) a  
	left join 
		(
		select 
			*  
		from  csx_dim.csx_dim_basic_goods 
		where sdt = 'current'
		) b on b.goods_code = a.goods_code 
	left join  -- 首单日期
		(
		select 
			customer_code,
			business_type_code,
			min(first_business_sale_date) first_sales_date
		from csx_dws.csx_dws_crm_customer_business_active_di
		where sdt ='current' 
			-- and business_type_code in (1)
		group by 
			customer_code,
			business_type_code
		)c on c.customer_code=a.customer_code 
			and c.business_type_code=a.business_type_code
	left join  
		(
		select
			customer_code,
			customer_name,
			first_category_name,
			second_category_name,
			third_category_name 
		from  csx_dim.csx_dim_crm_customer_info 
		where sdt='current'	       
		)d on d.customer_code=a.customer_code 
	left join  -- 线上客户定价类型
		csx_analyse_tmp.csx_analyse_tmp_customer_price_type_01 e 
		on a.customer_code=e.customer_code 
	left join  -- 线下客户定价类型
		csx_ods.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
		on a.customer_code=f.customer_code 
	left join -- 客户市调表商品
		(select 
			g1.customer_code,
			g2.product_code as goods_code
		from 
			(select *  
			from csx_dwd.csx_dwd_price_market_customer_research_price_di 
			where status='1' 
			) g1 
			left join 
			(select * 
			from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
			where sdt=regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
			) g2 
			on g1.product_id=g2.id 
		group by 
			g1.customer_code,
			g2.product_code
		) g 
		on a.customer_code=g.customer_code and a.goods_code=g.goods_code 
	left join  -- 客户等级
		(select 
			customer_no,
			customer_large_level
		from csx_analyse.csx_analyse_report_sale_customer_level_mf 
		where month = substr(regexp_replace(trunc('${i_sdate}','MM'),'-',''),1,6)
			and tag= 2 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		)h on h.customer_no=a.customer_code 		
group by 
	a.performance_region_name ,
	a.performance_province_name ,
	a.performance_city_name,
	a.business_type_code,
	a.business_type_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1) , -- 定价类型1
	nvl(f.sec_price_type,e.price_type2) , -- 定价类型2
	e.price_period_name , -- 报价周期
	e.price_date_name, -- 报价日
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	h.customer_large_level ,
	c.first_sales_date ,
	b.business_division_name ,
	b.purchase_group_code ,
	b.purchase_group_name ,
	b.classify_large_code ,
	b.classify_large_name ,
	b.classify_middle_code ,
	b.classify_middle_name ,
	b.classify_small_code ,
	b.classify_small_name ,
	a.goods_code ,
	b.goods_name ,
	(case when g.goods_code is not null then '是' else '否' end) ,
	sales_type, -- `是否调价`,
	fanli_type, -- `是否返利`,
	tuihuo_type, -- `是否退货`,
	delivery_type_name, -- `物流模式`,
	direct_delivery_type, -- as `物流模式细分`,
	inventory_dc_code,-- `DC编码`,
	types, --as `是否直送仓`,
	fjc, -- `福建剔除仓`,
	nvl(if(c.first_sales_date >= regexp_replace(trunc('${i_sdate}','MM'),'-','') and c.first_sales_date<= regexp_replace(add_months('${i_sdate}',0),'-',''),'新客','老客'),0)

)a;



	
select * from csx_analyse_tmp.csx_city_customer_goods_sale
where performance_province_name in ('北京市','河南省','河北省','陕西省')
	and business_type_code='1'
	-- and types ='否';


	
-- 客户维度
select 
	performance_region_name ,
	performance_province_name ,
	performance_city_name,
	business_type_code,
	business_type_name,
	customer_code,
	customer_name,
	price_type1, -- 定价类型1
	price_type2, -- 定价类型2
	price_period_name , -- 报价周期
	price_date_name, -- 报价日
	first_category_name,
	second_category_name,
	third_category_name,
	customer_large_level ,
	first_sales_date ,	
	xinlaok,
	sum(by_sale_amt)by_sale_amt,
	sum(by_sale_qty)by_sale_qty,
	sum(by_profit)  by_profit,

	sum(sy_sale_amt)sy_sale_amt,
	sum(sy_sale_qty)sy_sale_qty,
	sum(sy_profit)  sy_profit,	

	sum(bz_sale_amt)bz_sale_amt,
	sum(bz_sale_qty)bz_sale_qty,
	sum(bz_profit)  bz_profit,	

	sum(sz_sale_amt)sz_sale_amt,
	sum(sz_sale_qty)sz_sale_qty,
	sum(sz_profit)  sz_profit	

from csx_analyse_tmp.csx_city_customer_goods_sale
where  business_type_code='1'
	and types ='否' and fjc ='否' 
group by 
	performance_region_name ,
	performance_province_name ,
	performance_city_name,
	business_type_code,
	business_type_name,
	customer_code,
	customer_name,
	price_type1, -- 定价类型1
	price_type2, -- 定价类型2
	price_period_name , -- 报价周期
	price_date_name, -- 报价日
	first_category_name,
	second_category_name,
	third_category_name,
	customer_large_level ,
	first_sales_date ,	
	xinlaok;
	
	
	
	