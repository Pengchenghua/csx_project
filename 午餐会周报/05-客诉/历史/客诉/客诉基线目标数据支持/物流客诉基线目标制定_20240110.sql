-- 签收SKU数 只取渠道为大客户、业务代理，业务类型剔除BBC，城市服务商，不包含退货和调价返利
-- 20231220改：不剔除退货；剔除改单退、剔除送货后调整数量
-- 20240104改：增加补货数据：补货不含退货，


-- 退货类型	
drop table csx_analyse_tmp.csx_analyse_th; 
create table csx_analyse_tmp.csx_analyse_th 
as 
select *
from 
(
	select row_number() over(partition by sale_order_code,goods_code order by refund_total_amt desc) as rno,
		inventory_dc_code,
		inventory_dc_name, 
		sdt,	
		refund_code,
		source_type, -- 订单来源(0-签收差异或退货 1-改单退货)
		order_status_code,  -- 退货单状态: 10-差异待审(预留) 20-处理中 30-处理完成 -1-差异拒绝
		case order_status_code
		when -1 then '差异拒绝'
		when 10 then '差异待审'
		when 20 then '处理中'
		when 30 then '处理完成'
		else order_status_code end as order_status_name,  -- 退货单状态
		sale_order_code,
		customer_code,
		goods_code,
		regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name, 
		case source_biz_type
			when -1 then 'B端订单管理退货'
			when 0 then 'OMS物流审核'
			when 1 then '结算调整数量'
			when 2 then 'OMS调整数量'
			when 3 then 'CRM客诉退货'
			when 4 then 'CRM订单售后退货'
			when 5 then 'CRM预退货审核'
			when 6 then 'CRM签收'
			when 7 then '司机送达时差异'
			when 8 then '司机发起退货'
			when 9 then '实物退仓收货差异'
			when 10 then 'OMS签收'
		end as source_biz_type_name, 
		case refund_operation_type
			when -1 then '不处理'
			when 0 then '立即退'
			when 1 then '跟车退'
		end as refund_operation_type_name,  -- 退货处理方式 -1-不处理 0-立即退 1-跟车退
		case has_goods
			when 0 then '无实物'
			when 1 then '有实物'
		end as has_goods_name,	
		responsibility_reason,
		regexp_replace(regexp_replace(reason_detail,'\n',''),'\r','') as reason_detail,
		case child_return_type_code
			when 0 then '父退货单'
			when 1 then '子退货单逆向'
			when 2 then '子退货单正向'
		end as child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
		refund_order_type_code,
		case refund_order_type_code
			when 0 then '差异单'
			when 1 then '退货单'
		end as refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
		first_level_reason_code, -- 一级退货原因编码 001送货后调整数量
		first_level_reason_name,
		regexp_replace(regexp_replace(second_level_reason_name,'\n',''),'\r','') as second_level_reason_name
	from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
	where sdt>='20230101'
	and child_return_type_code in(1,2)
	and parent_refund_code<>''
 	)a
	where rno=1	


-- 紧急补货类型	

drop table csx_analyse_tmp.csx_analyse_bh;
create table csx_analyse_tmp.csx_analyse_bh 
as 
select a.*
from 
(select 
	e.replenishment_order_code,e.apply_code,e.goods_code,e.replace_goods_code,
	e.scm_source_type,f.sale_order_code,e.reason,
	row_number() over(partition by f.sale_order_code,e.goods_code order by e.update_time desc) as rno
from 
	(
	select 
		replenishment_order_code,     -- 补货单号
		apply_code,    -- 申请补货订单
		case scm_source_type
		when 0 then '缺货'
		when 1 then '客诉'
		when 2 then '签收差异'
		when 3 then '订单补货'
		else scm_source_type end as scm_source_type, -- 来源类型 0-缺货 1-客诉 2-签收差异 3-订单补货
		customer_code,
		goods_code,replace_goods_code,
		regexp_replace(regexp_replace(reason,'\n',''),'\r','') as reason,
		update_time
		-- row_number() over(partition by apply_code,goods_code order by update_time desc) as rno		
	from csx_dwd.csx_dwd_oms_emergency_replenishment_detail_df
	where item_create_time>='2023-01-01'  --无分区
	and (coalesce(cancel_flag,0)<>1 or cancel_flag is NULL or cancel_flag is null)	
	)e 
	left join  -- 补货单关联销售单
	(
	select replenishment_order_code,sale_order_code
	from csx_dwd.csx_dwd_oms_emergency_sale_order_relation_df
	)f on e.replenishment_order_code =f.replenishment_order_code
)a	
where a.rno=1		
	

	
-- 签收SKU，退货sku,补货sku
select 
	substr(a.sdt,1,6) as smonth,c.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	count(a.goods_code) as sku_cnt,
	count(case when a.refund_order_flag=1  and  h.source_type !=1  and h.first_level_reason_code != '001'  then h.goods_code else null end ) as th_sku,	
	count(case when h.refund_order_type_code is null then i.goods_code else null end ) as bh_sku		
from
	( 
	select
		sdt,customer_code,order_code,original_order_code,goods_code,sale_amt,profit,performance_region_name,performance_province_name,performance_city_name,refund_order_flag,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='${sdt_3m}' and sdt<='${sdt_tdm}' -- 最近三个月
		and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		-- and refund_order_flag= 0  剔除退货，后改不剔
		and performance_province_name != '平台-B'
		
	) a 
	left join
	(
	select
		goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_code,business_division_name
	from
		csx_dim.csx_dim_basic_goods
	where
		sdt='current'
	) b on b.goods_code=a.goods_code	
	left join
	(
	select
		calday,week_of_year -- 自然周
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt
-- 退货类型	
left join	
	(select * from csx_analyse_tmp.csx_analyse_th	
	)h on a.original_order_code=h.sale_order_code and a.goods_code=h.goods_code
-- 紧急补货类型	
left join
	(select * from csx_analyse_tmp.csx_analyse_bh	
	)i on i.sale_order_code= a.order_code_new -- 补货之后的单子
		-- i.sale_order_code= a.order_code -- 补货之后的单子
		-- i.apply_code =a.original_order_code  -- 补货最早单子
		and i.goods_code =a.goods_code
	group by 
		substr(a.sdt,1,6),c.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name;		
	

-- 责任部门统计 同一个客诉单涉及多个责任部门则每个部门+1
drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_03;
create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_03
as
select 
	a.smonth,e.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.first_level_department_name,
	a.second_level_department_name,
	count(distinct a.complaint_code) as ks_cnt
from
	(
	select
		substr(sdt,1,6) as smonth,sdt,performance_region_name,performance_province_name,performance_city_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,
		first_level_department_name,second_level_department_name,main_category_name,sub_category_name
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='${sdt_3m}' and sdt<='${sdt_tdm}'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		--and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and complaint_deal_status in(10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		--and second_level_department_name !=''
	) a
	left join
	(
	select
		calday,week_of_year
	from
		csx_dim.csx_dim_basic_date
	) e on e.calday=a.sdt
where
	a.performance_province_name !='平台-B'
group by 
	a.smonth,e.week_of_year,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	a.first_level_department_name,
	a.second_level_department_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_03;




-- 退货责任部门统计 
select
	substr(a.sdt,1, 6) smonth,    --	退货申请日期
	c.week_of_year,
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	e.responsible_department_name,  -- 责任部门名称
	count(a.goods_code) ths
from 
(
select * from  csx_analyse_tmp.csx_analyse_th 
where source_type !=1  and first_level_reason_code != '001'
)a
left join 
	(
	select  
		customer_code,
		performance_region_name,     --  销售大区名称(业绩划分)
		performance_province_name,     --  销售归属省区名称
		performance_city_name     --  城市组名称(业绩划分)
	from csx_dim.csx_dim_crm_customer_info
	where sdt='current'
	and customer_type_code=4
	)b on a.customer_code=b.customer_code
left join
	(
	select
		calday,week_of_year
	from
		csx_dim.csx_dim_basic_date
	) c on c.calday=a.sdt
-- 客退责任单行表
left join 
	(
	select 
		responsible_no,	  -- 判责单号
		product_code,	  -- 商品编码
		sale_order_no,
		parent_refund_no,  -- 退货主单号
		refund_no,  -- 退货子单号
		-- stock_process_type,  -- 库存处理方式：1-报损 2-退供 3-调拨 4-二次上架
		case 
		when stock_process_type='1' then '报损'
		when stock_process_type='2' then '退供'
		when stock_process_type='3' then '调拨'
		when stock_process_type='4' then '二次上架'
		else stock_process_type end as stock_process_type,
		-- stock_process_confirm  -- 是否确认：0-待确认 1-已确认
		case 
		when stock_process_confirm='0' then '待确认'
		when stock_process_confirm='1' then '已确认'
		else stock_process_confirm end as stock_process_confirm	
	from csx_ods.csx_ods_csx_b2b_oms_refund_responsible_item_df
	)d on a.refund_code=d.refund_no and a.goods_code=d.product_code
-- 客退责任单部门处理表
left join 
	(
	select 
		responsible_no,	  -- 判责单号
		product_code,	  -- 商品编码 
		responsible_department_name,  -- 责任部门名称
		-- status,  -- 10-待判责 20-待处理 21-已申诉 22-申诉驳回 30-已完成 -1-已取消
		case 
		when status='10' then '待判责'
		when status='20' then '待处理'
		when status='21' then '已申诉'
		when status='22' then '申诉驳回'
		when status='30' then '已完成'
		when status='-1' then '已取消'
		else status end as status,	
		if(appeal_person_name<>'','是','否') as is_appeal, -- 是否申诉
		regexp_replace(regexp_replace(appeal_reason,'\n',''),'\r','') as appeal_reason  -- 申诉理由
	from csx_ods.csx_ods_csx_b2b_oms_refund_responsible_department_deal_df
	)e on d.responsible_no=e.responsible_no and d.product_code=e.product_code
group by 
	substr(a.sdt,1, 6) ,    --	退货申请日期
	c.week_of_year,
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)
	e.responsible_department_name























===  客户单

select e.week_of_year,a.* from 
(	select
		substr(sdt,1,6) as smonth,sdt,performance_region_name,performance_province_name,performance_city_name,customer_code,sale_order_code,goods_code,goods_name,complaint_code,
		first_level_department_name,second_level_department_name,main_category_name,sub_category_name
	from
		csx_analyse.csx_analyse_fr_oms_complaint_detail_di
	where 
		sdt>='20230801' and sdt<='20231112'
		and complaint_status_code in(20,30) -- 客诉单状态 10'待判责' 20'处理中' 21'待审核' 30'已完成' -1'已取消'
		-- and complaint_deal_status !=-1 -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		and complaint_deal_status in(10,40) -- 责任环节状态 10'待处理' 20'待修改' 30'已处理待审' 31'已驳回待审核' 40'已完成' -1'已取消'
		-- and second_level_department_name !=''
		and performance_province_name ='广东省'
) a
	left join
		(
		select
			calday,week_of_year
		from
			csx_dim.csx_dim_basic_date
		) e on e.calday=a.sdt
where  e.week_of_year ='202345'
