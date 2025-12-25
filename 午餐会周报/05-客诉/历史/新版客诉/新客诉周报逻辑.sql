---- 客诉明细			
drop table csx_analyse_tmp.csx_analyse_ks; 
create table csx_analyse_tmp.csx_analyse_ks 
as 
select 
	substr(a.sdt,1,6) as smonth,
	c.week_of_year,
	a.*,
	h.customer_large_level
from 	
(select
	sdt   -- 客诉发生日期
	,complaint_code	-- 客诉单编码
	,performance_region_name
	,performance_province_name
	,performance_city_name
	,require_delivery_date -- 要求送货日期
	,customer_code	
	,customer_name	
	,sub_customer_code	
	,sub_customer_name
	,goods_code	
	,goods_name
	,classify_large_code	
	,classify_large_name	
	,classify_middle_code	
	,classify_middle_name	
	,classify_small_code	
	,classify_small_name
	,complaint_qty	      -- 客诉数量
	,unit_name             -- 单位
	,purchase_qty	      -- 下单数量
	,purchase_unit_name    -- 单位
	,complaint_amt    -- 客诉金额
	,complaint_type_name    -- 客诉类型
	,main_category_code   -- 客诉大类编码
	,main_category_name
	,sub_category_code	 -- 客诉小类编码
	,sub_category_name
	-- ,reason   -- 客诉部门产生原因
	,complaint_status_code	 -- 客诉状态: 10-待判责 20-处理中 21-待审核 30-已完成 -1-已取消
	,complaint_deal_status   -- 客诉部门状态 10-待处理 20-待修改 30-已处理待审 31-已驳回待审核 40-已完成 -1-已取消
	,need_process -- 是否判责(-1.待判责 0-无需判责 1-已判责)
	,complaint_level  -- 客诉等级：0-一级紧急 1-一级非紧急 2-二级 3-三级
	,complaint_source  -- 客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	,first_level_department_code  -- 一级部门	
	,first_level_department_name	  
	,second_level_department_code -- 二级部门
	,second_level_department_name
	,cost_department_code	-- 成本归属部门
	,cost_department_name
	,cancel_reason -- 取消原因
from  csx_dws.csx_dws_oms_complaint_detail_di
where 
	sdt>='${sdt_3m}' and sdt<='${sdt_tdm}'
	and performance_province_name not in ('东北','平台-B')
	-- and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
	and complaint_deal_status in(40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
	-- and complaint_source = 1  --客诉来源:1-单独发起客诉单 2-客退单生成 3-补货单生成
	and complaint_amt <> 0
	and main_category_code != '001'  -- 剔除一级退货原因编码 001送货后调整数量
	and customer_name not like '%XM%' 
)a
left join -- 周信息--自然周
	(
	select
		calday,week_of_year
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt
left join -- 客户等级
	(
	select 
		customer_no,customer_large_level,month
	from csx_analyse.csx_analyse_report_sale_customer_level_mf
	where month = substr('${sdt_tdm}',1,6)
		and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
	) h on h.customer_no=a.customer_code;





-- 数据源：签收SKU：各省区签收SKU，客诉
select 
	substr(a.sdt,1,6) as smonth,
	c.week_of_year,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	sum(sku_cnt) as qs_sku, -- 签收SKU

	sum(ks_num) ks_num,	
	sum(th_num) th_num,	
	sum(bh_num) bh_num,
	sum(all_ks_num) all_ks_num
from
	( 
	select	
	    sdt,
		performance_region_name,
		performance_province_name,
		performance_city_name,
		count(goods_code) as sku_cnt -- 签收SKU
	from csx_dws.csx_dws_sale_detail_di
	where sdt>='${sdt_3m}' and sdt<='${sdt_tdm}' -- 最近三个月
		and channel_code in ('1','9')
		and business_type_code <> 4 -- (1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code = 1  -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag = 0 		
	group by 
		sdt,
		performance_region_name,
		performance_province_name,
		performance_city_name
	)a 
left join 
	(
	select 
		sdt
		,performance_region_name
		,performance_province_name
		,performance_city_name
		,count(case when complaint_source = 1 then goods_code end ) ks_num
		,count(case when complaint_source = 2 then goods_code end ) th_num
		,count(case when complaint_source = 3 then goods_code end ) bh_num
		,count(goods_code) all_ks_num		
	from csx_analyse_tmp.csx_analyse_ks 
	group by
		sdt
		,performance_region_name
		,performance_province_name
		,performance_city_name
	)b on a.sdt=b.sdt and a.performance_region_name=b.performance_region_name and a.performance_province_name=b.performance_province_name and a.performance_city_name=b.performance_city_name		
left join -- 周信息--自然周
	(
	select
		calday,week_of_year 
	from
		csx_dim.csx_dim_basic_date
	)c on c.calday=a.sdt
group by 
	substr(a.sdt,1,6),
	c.week_of_year,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name


-- 数据源：二级责任部门客诉数
select 
	smonth
	,week_of_year
	,performance_region_name
	,performance_province_name
	,performance_city_name
	,first_level_department_name	  
	,second_level_department_name
	,count(case when complaint_source = 1 then goods_code end ) ks_num
	,count(case when complaint_source = 2 then goods_code end ) th_num
	,count(case when complaint_source = 3 then goods_code end ) bh_num
	,count(goods_code) all_ks_num	
from csx_analyse_tmp.csx_analyse_ks 
group by
	smonth
	,week_of_year
	,performance_region_name
	,performance_province_name
	,performance_city_name
	,first_level_department_name	  
	,second_level_department_name
	
	
-- 数据源：AC类客诉
-- 周维度：
	select
		*,
		row_number() over(partition by week_of_year,performance_province_name, customer_large_level order by ks_num desc) w_r_num
	
	from
	(select 
		week_of_year
		,performance_province_name
		,customer_code	
		,customer_name
		,customer_large_level
		,count(goods_code) ks_num
	from csx_analyse_tmp.csx_analyse_ks 
	where customer_large_level in ('A','C')
	group by
		week_of_year
		,performance_province_name
		,customer_code	
		,customer_name
		,customer_large_level
	)a
	
-- 月维度：	
	select
		*,
		row_number() over(partition by smonth,performance_province_name, customer_large_level order by ks_num desc) m_r_num
	
	from
	(select 
		smonth
		,performance_province_name
		,customer_code	
		,customer_name
		,customer_large_level
		,count(goods_code) ks_num
	from csx_analyse_tmp.csx_analyse_ks 
	where customer_large_level in ('A','C')
	group by
		smonth
		,performance_province_name
		,customer_code	
		,customer_name
		,customer_large_level
	)a















-- 客诉表和退货表关联验证
select 
	a.*,
	b.source_type,
    b.first_level_reason_code,
	b.reason_detail,
	b.refund_total_amt,
	b.refund_scale_total_amt
from 
	(select *
	from  csx_dws.csx_dws_oms_complaint_detail_di
	where 
		sdt ='${sdt_3m}'
	) a
	left join 
	(
	select 
		sale_order_code,
		refund_code,
		goods_code,
		source_type,  -- 订单来源：改单退货
		first_level_reason_code,  -- 一级退货原因编码 001送货后调整数量!= '001' 
		reason_detail,   -- 仓位调整
		refund_total_amt, 
		refund_scale_total_amt 
	from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di	
	) b on a.sale_order_code =b.sale_order_code and a.refund_code=b.refund_code and a.goods_code=b.goods_code
