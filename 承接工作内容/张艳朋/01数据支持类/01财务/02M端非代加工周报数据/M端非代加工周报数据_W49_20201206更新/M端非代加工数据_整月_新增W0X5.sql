--==============================================================================================================
--月累计
insert overwrite directory '/tmp/zhangyanpeng/_w49_m_m' row format delimited fields terminated by '\t' 
			
select
	a.sdt,
	a.province_name, 
	a.dc_code,
	coalesce(regexp_replace(a.dc_name, '\n|\t|\r|\,|\"', ''), '') dc_name,
	--a.dc_name,
	a.customer_no,
	coalesce(regexp_replace(a.customer_name, '\n|\t|\r|\,|\"', ''), '') customer_name,
	--a.customer_name,
	coalesce(regexp_replace(b.sales_belong_flag, '\n|\t|\r|\,|\"', ''), '') sales_belong_flag,
	--b.sales_belong_flag,
	a.department_code,
	a.department_name,
	a.goods_code,
	a.goods_name,
	a.sales_qty,
	a.unit,
	a.sales_value,
	a.sales_cost,
	a.profit,
	a.profit/a.sales_value as profit_rate,
	case when b.sales_belong_flag like '%云超%' then '云超'
		when b.sales_belong_flag like '%云创%' then '云创'
		when a.customer_name rlike '红旗|中百' then '关联方'
		when b.sales_belong_flag='' or b.sales_belong_flag is null then '外部'
		else '其他'
	end as sales_belong_type,
	if(a.department_code in ('U01','H03','H05','H01','H02','H04','104'),a.department_name,'其他') as department_type,--熟食课,蔬菜课,猪肉课,干货课,水果课,家禽课,易耗品采购组,其他
	regexp_replace(c.region_name,'大区','') as region_name
from	
	(	
	select
		sdt,
		province_code,
		province_name, 
		dc_code,
		dc_name,
		customer_no,
		customer_name,					
		department_code,
		department_name,
		goods_code,
		goods_name,
		unit,
		sum(sales_qty) as sales_qty,
		sum(sales_value) as sales_value,
		sum(sales_cost) as sales_cost,
		sum(profit) as profit
	from 
		csx_dw.dws_sale_r_d_customer_sale
	where 
		(sdt between '20201101' and '20201130' or sdt between '20201001' and '20201031')
		and channel = '2' --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
		and (dc_code not in ('W0R1','W0T6','W0M4','W0T3','W0T7','W0M6','W0S8','W0X4','W0T5','W0X5') or (dc_code in ('W0M4') and department_code not in ('H03','H01')))--数据不含代加工DC 
	group by
		sdt,
		province_code,
		province_name, 
		dc_code,
		dc_name,
		customer_no,
		customer_name,					
		department_code,
		department_name,
		goods_code,
		goods_name,
		unit
	) as a 
	left join
		(
		select 
			shop_id,company_code,sales_belong_flag
		from 
			csx_dw.dws_basic_w_a_csx_shop_m
		where 
			sdt = 'current'
		) b on a.customer_no = concat('S', b.shop_id)
	left join
		(
		select 
			province_code,province_name,region_code,region_name 
		from 
			csx_dw.dim_area 
		where 
			area_rank=13
		group by
			province_code,province_name,region_code,region_name 
		) c on c.province_code=a.province_code
