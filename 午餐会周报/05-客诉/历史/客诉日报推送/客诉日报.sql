-- 客诉日报推送数据：
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	complaint_code, -- 客诉单号
	require_delivery_date,  -- 要求送货日期
	customer_code,
	customer_name,   
	sub_customer_code,
	sub_customer_name,
	main_category_name,
	sub_category_name,
	goods_code,
	goods_name,
	complaint_describe, -- 问题描述
	second_category_name, -- 二级部门 
	purchase_unit_name, -- 下单单位
    unit_name,-- 客诉单位
	sum(complaint_amt) complaint_amt, -- 客诉金额
	sum(purchase_qty) purchase_qty, -- 下单数量
	sum(complaint_qty) complaint_qty -- 基础单位客诉商品数量
from  csx_dws.csx_dws_oms_complaint_detail_di
where sdt = '20240422'	
	and performance_province_name not in ('东北','平台-B')
	and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
	and complaint_deal_status in(10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	and complaint_amt <> 0
	and main_category_code != '001'  -- 剔除一级退货原因编码 001送货后调整数量	 
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	complaint_code, -- 客诉单号
	require_delivery_date,  -- 配送日期
	customer_code,
	customer_name,   
	sub_customer_code,
	sub_customer_name,
	main_category_name,
	sub_category_name,
	goods_code,
	goods_name,
	complaint_describe, -- 问题描述
	second_category_name, -- 二级部门 
	purchase_unit_name, -- 下单单位
    unit_name-- 客诉单位
	
	
	
	
	
==========================================================

-- 一级二级部门客诉

select 
	c.week_be,
	a.*
from 
	(select 
		sdt,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		first_level_department_name,
		second_level_department_name, -- 二级部门 
		count(goods_code) goods_num
	from  csx_dws.csx_dws_oms_complaint_detail_di
	where sdt >= '20240401'	
		and performance_province_name = '北京市'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status in(10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
		and complaint_amt <> 0
		and main_category_code != '001'  -- 剔除一级退货原因编码 001送货后调整数量	 
	group by
		sdt, 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		first_level_department_name,
		second_level_department_name
	)a	
	left join -- 周信息
	(
	select
		calday,concat(week_of_year,'(',week_begin,'-',week_end,')') as week_be
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt



	
============================================================

select 
	d.customer_large_level,
	c.week_be,
	a.*
from 
	(select 
		sdt,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name,   
		-- sub_customer_code,
		-- sub_customer_name,
		count(goods_code) goods_num,
		count(case when sdt > '${bz_1}' then goods_code end ) bz_goods_num
	from  csx_dws.csx_dws_oms_complaint_detail_di
	where sdt >= '${sdate}'	
		and performance_province_name = '${sq}'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		and complaint_deal_status in(10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'货单生成
		and complaint_amt <> 0
		and main_category_code != '001'  -- 剔除一级退货原因编码 001送货后调整数量	 
	group by
		sdt, 
		performance_region_name,
		performance_province_name,
		performance_city_name,
		customer_code,
		customer_name  
		-- sub_customer_code,
		-- sub_customer_name
	)a	
	left join -- 周信息
	(
	select
		calday,concat(week_of_year,'(',week_begin,'-',week_end,')') as week_be
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt
	left join  -- 客户等级
	(
	select
		customer_no,customer_large_level,month
	from 
		csx_analyse.csx_analyse_report_sale_customer_level_mf
	where
		month ='${sdate_m}'	
		and tag=1  --数据标识：1：全量数据；2：剔除不统计业绩仓数据
	) d on d.customer_no=a.customer_code
order by bz_goods_num desc
limit 10