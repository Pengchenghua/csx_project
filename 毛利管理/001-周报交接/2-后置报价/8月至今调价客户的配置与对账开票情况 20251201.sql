-- 跑数前需先同步后置定价配置表：dev.csx_ods_csx_b2b_sss_sss_customer_config_ss_df
-- 导入清单客户
drop table csx_analyse_tmp.cust_list_hzdj_r; 
select * 
from csx_analyse_tmp.cust_list_hzdj_r 
limit 20; 


select * 
from csx_analyse_tmp.cust_hzdj_bill_invoice 



-- 8月至今后置定价调价客户的配置与对账开票情况 20251009
drop table csx_analyse_tmp.cust_hzdj_bill_invoice; 
create table csx_analyse_tmp.cust_hzdj_bill_invoice as  
with
-- 后置定价调价客户清单
tj_cust_list as 
(
  select customer_code
  from 
  (
    select customer_code
    from csx_analyse_tmp.cust_list_hzdj_r 
  
    union all 
    select 
      customer_code
    from dev.csx_ods_csx_b2b_sss_sss_customer_config_ss_df
	-- csx_ods_csx_b2b_sss_sss_customer_config_df 后续更新为此表

    where is_deleted=0  -- 删除状态：0:正常、1:删除
    and is_post_offer=1
    and shipper_code='YHCSX'
  )a 
  group by customer_code  
),
 
adjust_detail as 
(
select
    a.customer_code,
	sum(case when a.smonth between '202509' and '202512' then a.sale_amt else 0 end) as sale_amt_tj,
	sum(case when g2.smonth='202509' then a.sale_amt end) as sale_amt8_tjps,  -- 发货月份
	sum(case when g2.smonth='202510' then a.sale_amt end) as sale_amt9_tjps,	
	sum(case when g2.smonth='202511' then a.sale_amt end) as sale_amt10_tjps,	
	sum(case when g2.smonth='202512' then a.sale_amt end) as sale_amt11_tjps,	
	-- 调价
	if(sum(case when g.adjust_reason is not null then 1 else 0 end)>0,'有后置定价调价','无后置定价调价') as adjust_reason	
from
(
	select * ,
		regexp_replace(substr(delivery_time,1,7),'-','') smonth,
		weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week,
		if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new,
		case when partner_type_code in (1,3) then '是' else '否' end as is_qzc		
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >=regexp_replace(add_months(trunc(current_date,'MM'),-2),'-','') 
	-- and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
	and order_channel_code=6 -- '调价单'
	and shipper_code='YHCSX'
	and customer_code in(select customer_code from tj_cust_list)
) a
-- 调价单原单配送日期
left join 
(
	select order_code,goods_code,
		substr(max_delivery_sdt,1,6) smonth
	from 
	(
		select order_code,goods_code,max(sdt) max_sdt,
		max(regexp_replace(substr(delivery_time,1,10),'-','')) max_delivery_sdt
			-- if((order_channel_code in ('4','5','6') or refund_order_flag=1),original_order_code,order_code) as order_code_new
		from csx_dws.csx_dws_sale_detail_di 
		where sdt >=regexp_replace(add_months(trunc(current_date,'MM'),-8),'-','') 
		and business_type_code in ('1') 
		and shipper_code='YHCSX'
		group by order_code,goods_code
	)a
) g2 on g2.order_code=a.original_order_code and g2.goods_code=a.goods_code and a.order_channel_code=6
-- 调价类型	
left join	
(
	select * 
	from 
	(
	select 
		original_order_code,
		adjust_price_order_code,
		goods_code,
		-- a.adjusted_total_amount,
		(CASE WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 10 THEN '报价错误-报价失误'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 11 THEN '报价错误-报价客户不认可'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 20 THEN '客户对账差异-税率调整'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 21 THEN '客户对账差异-其他'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 30 THEN '后端履约问题-商品等级/规格未达要求'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 31 THEN '后端履约问题-商品质量问题折扣处理'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 32 THEN '后端履约问题-其他'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 40 THEN '发货后报价类型'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 50 THEN '无原单退款'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 60 THEN '其他'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 70 THEN '单据超90天未处理'
		      
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 71 THEN '报价错误-产品运营共享报价错误'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 72 THEN '报价错误-采购原因价格错误'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 73 THEN '报价错误-销售原因价格错误'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 74 THEN '报价客户不认可-报价未确认'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 75 THEN '报价客户不认可-报价已确认客户仍不认可'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 76 THEN '发货后报价类型'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 77 THEN '后端履约问题-商品等级/规格未达要求'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 78 THEN '后端履约问题-商品质量问题折扣处理'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 79 THEN '税率差异调整'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 80 THEN '客户对账差异-其他'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 81 THEN '历史账款处理'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 82 THEN '非坏账客户罚款处理'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 83 THEN '其他'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 84 THEN '合同约定返利'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 85 THEN '超出客户预算调整'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 86 THEN '合同约定后置定价'
			  WHEN coalesce(after_adjust_reason_code,adjust_reason_code) = 87 THEN '数量无法调整需调整价格补差'
		END) as adjust_reason,
		if(check_bill_status='1','已对账','未对账') as check_bill_status,  -- 对账状态：1-已对账，0-未对账
		row_number() over(partition by adjust_price_order_code,goods_code order by update_time desc,adjusted_total_amount desc) as rno 				
	-- from csx_dwd.csx_dwd_sss_customer_credit_adjust_price_item_di
	from csx_dwd.csx_dwd_sss_customer_adjust_price_detail_di
	where sdt>=regexp_replace(add_months(trunc(current_date,'MM'),-8),'-','')
	and shipper_code='YHCSX'
	and coalesce(after_adjust_reason_code,adjust_reason_code) = 86  -- '合同约定后置定价'
 	)a
	where rno=1
)g on a.order_code=g.adjust_price_order_code and a.goods_code=g.goods_code
group by 
    a.customer_code
),

-- 后置定价调价客户清单
-- tj_cust_list as 
-- (
--   select customer_code
--   from adjust_detail
--   group by customer_code	
-- ),

cust_sale as
(
	select customer_code,
		sum(case when substr(sdt,1,6)='202509' then sale_amt end) as sale_amt8,
		sum(case when substr(sdt,1,6)='202509' then profit end) as profit8,
		sum(case when substr(sdt,1,6)='202510' then sale_amt end) as sale_amt9,
		sum(case when substr(sdt,1,6)='202510' then profit end) as profit9,	
		sum(case when substr(sdt,1,6)='202511' then sale_amt end) as sale_amt10,
		sum(case when substr(sdt,1,6)='202511' then profit end) as profit10,
		sum(case when substr(sdt,1,6)='202512' then sale_amt end) as sale_amt11,
		sum(case when substr(sdt,1,6)='202512' then profit end) as profit11		
	from csx_dws.csx_dws_sale_detail_di 
	where sdt >=regexp_replace(add_months(trunc(current_date,'MM'),-4),'-','') 
	-- and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
	and business_type_code in ('1') 
	-- and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71','WC65','WD38','WD53')
	and shipper_code='YHCSX'
	and customer_code in(select customer_code from tj_cust_list)
	group by customer_code
),

-- cust_sale_ps as
-- (
-- 	select customer_code,
-- 		sum(case when require_delivery_date between '20250801' and '20250831' then sale_amt end) as sale_amt8_ps,
-- 		-- sum(case when require_delivery_date between '20250801' and '20250831' then profit end) as profit8_ps,
-- 		sum(case when require_delivery_date between '20250901' and '20250930' then sale_amt end) as sale_amt9_ps,
-- 		-- sum(case when require_delivery_date between '20250901' and '20250930' then profit end) as profit9_ps,	
-- 		sum(case when require_delivery_date between '20251001' and '20251031' then sale_amt end) as sale_amt10_ps
-- 		-- sum(case when require_delivery_date between '20251001' and '20251031' then profit end) as profit10_ps
-- 	from csx_dws.csx_dws_sale_detail_di 
-- 	where sdt >=regexp_replace(add_months(trunc(current_date,'MM'),-6),'-','') 
-- 	-- and sdt <= regexp_replace(add_months(date_sub(current_date,1),0),'-','') 
-- 	and business_type_code in ('1') 
-- 	-- and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71','WC65','WD38','WD53')
-- 	and shipper_code='YHCSX'
-- 	and customer_code in(select customer_code from tj_cust_list)
-- 	group by customer_code
-- ),

-- 各月销售的对账开票完成情况
customer_bill_invoice as
(
  select 
  customer_code,  -- 客户编码
  if(order_amt_m7=0,'',if(bill_finish_m7<>0,'未完成','已完成')) as bill_finish_m7,
  if(order_amt_m8=0,'',if(bill_finish_m8<>0,'未完成','已完成')) as bill_finish_m8,
  if(order_amt_m9=0,'',if(bill_finish_m9<>0,'未完成','已完成')) as bill_finish_m9,
  if(order_amt_m10=0,'',if(bill_finish_m10<>0,'未完成','已完成')) as bill_finish_m10,
  				
  if(order_amt_m7=0,'',if(invoice_finish_m7<>0,'未完成','已完成')) as invoice_finish_m7,
  if(order_amt_m8=0,'',if(invoice_finish_m8<>0,'未完成','已完成')) as invoice_finish_m8,
  if(order_amt_m9=0,'',if(invoice_finish_m9<>0,'未完成','已完成')) as invoice_finish_m9,
  if(order_amt_m10=0,'',if(invoice_finish_m10<>0,'未完成','已完成')) as invoice_finish_m10,
  
  bill_minsdt,
  invoice_minsdt,
  
  bill_maxsdt,
  invoice_maxsdt  
  from 
  (
  select 
  -- source_bill_no,  -- 来源单号
  -- customer_code,  -- 客户编码
  -- customer_bill_date,  -- 客户对账日期
  -- invoice_status_code,  -- 开票状态 10 待开票20 开票完成30 部分开票
  -- sdt
  customer_code,  -- 客户编码
  sum(case when substr(sdt,1,6)='202508' then order_amt else 0 end) as order_amt_m7,
  sum(case when substr(sdt,1,6)='202509' then order_amt else 0 end) as order_amt_m8,
  sum(case when substr(sdt,1,6)='202510' then order_amt else 0 end) as order_amt_m9,
  sum(case when substr(sdt,1,6)='202511' then order_amt else 0 end) as order_amt_m10,  
  
  sum(case when substr(sdt,1,6)='202508' and nvl(customer_bill_date,'')='' then order_amt else 0 end) as bill_finish_m7,
  sum(case when substr(sdt,1,6)='202509' and nvl(customer_bill_date,'')='' then order_amt else 0 end) as bill_finish_m8,
  sum(case when substr(sdt,1,6)='202510' and nvl(customer_bill_date,'')='' then order_amt else 0 end) as bill_finish_m9,
  sum(case when substr(sdt,1,6)='202511' and nvl(customer_bill_date,'')='' then order_amt else 0 end) as bill_finish_m10,
  
  sum(case when substr(sdt,1,6)='202508' and invoice_status_code=10 then order_amt else 0 end) as invoice_finish_m7,
  sum(case when substr(sdt,1,6)='202509' and invoice_status_code=10 then order_amt else 0 end) as invoice_finish_m8,
  sum(case when substr(sdt,1,6)='202510' and invoice_status_code=10 then order_amt else 0 end) as invoice_finish_m9,
  sum(case when substr(sdt,1,6)='202511' and invoice_status_code=10 then order_amt else 0 end) as invoice_finish_m10,
  
  substr(min(case when nvl(customer_bill_date,'')='' then sdt end),1,6) as bill_minsdt,
  substr(min(case when invoice_status_code=10 then sdt end),1,6) as invoice_minsdt,
  
  substr(max(case when nvl(customer_bill_date,'')<>'' then sdt end),1,6) as bill_maxsdt,
  substr(max(case when invoice_status_code=20 then sdt end),1,6) as invoice_maxsdt
  from csx_dwd.csx_dwd_sss_source_bill_di
  where sdt>=regexp_replace(add_months(trunc(current_date,'MM'),-23),'-','')
  and shipper_code='YHCSX'
  -- and order_status='SUCCESS' 字段无效
  -- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单
  -- and bill_type=10 
  and delete_flag=0
  -- and source_bill_no not like 'OC%'
  -- and source_bill_no not like 'R%'
  and customer_code in(select customer_code from tj_cust_list)
  group by customer_code 
  )a
)

select
    b.performance_region_name,
    b.performance_province_name,
    b.performance_city_name,
    a1.customer_code,
    b.customer_name,
	f.first_business_sale_date,
	f.last_business_sale_date,
	g.sale_amt8,
	g.profit8/abs(g.sale_amt8) as profit_rate8,
	g.sale_amt9,
	g.profit9/abs(g.sale_amt9) as profit_rate9,
	g.sale_amt10,
	g.profit10/abs(g.sale_amt10) as profit_rate10,	
	g.sale_amt11,
	g.profit11/abs(g.sale_amt11) as profit_rate11,
	
	a.sale_amt_tj,
	a.sale_amt8_tjps,
	a.sale_amt9_tjps,	
	a.sale_amt10_tjps,
	a.sale_amt11_tjps,
    d.price_mode, -- 报价周期 2:按周 3:按月 4:按半月 6:自定义周期
	if(d.price_mode=6,d.cycle_days,d.price_day) as price_day,  -- 价格周期日
    -- d.price_day,  -- 价格周期日
    -- d.cycle_days,  -- 自定义周期天数
    d.`next_day`,  -- 价格周期日后天数	
	-- 调价
	a.adjust_reason,
	if(d.is_post_position=1,'是','否') as is_hz,

	e.bill_finish_m7,
	e.bill_finish_m8,
	e.bill_finish_m9,
	e.bill_finish_m10,

	e.invoice_finish_m7,
	e.invoice_finish_m8,
	e.invoice_finish_m9,
	e.invoice_finish_m10,
	
	e.bill_minsdt,	
	e.invoice_minsdt,
	e.bill_maxsdt,
	e.invoice_maxsdt	
from tj_cust_list a1
left join adjust_detail a on a.customer_code=a1.customer_code
left join 
  (
  select 
    customer_code,
    customer_name,
	dev_source_code,dev_source_name,
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    sales_user_number         sale_work_no,
    sales_user_name           sale_name
  from csx_dim.csx_dim_crm_customer_info
  where sdt = 'current'
  and shipper_code='YHCSX'
  ) b on a1.customer_code = b.customer_code 
  left join 
  (
--  select 
--    customer_code,
--    statement_user_code,
--	statement_user_name,
--	is_post_position
--  from csx_dim.csx_dim_sss_customer_statement_user_config
--  where sdt = regexp_replace(date_sub(current_date,1),'-','')
--  and delete_status=0  -- 删除状态：0:正常、1:删除
--  -- and statement_user_name not in ('','-','赵刚诚')
--  and shipper_code='YHCSX'
--  -- and is_post_position=1 -- 是否后置报价 0-否 1-是
  
  select 
    customer_code,
    -- statement_user_code,
	statement_user_name,
	is_post_offer as is_post_position,
	-- price_mode,  -- 报价周期 2:按周 3:按月 4:按半月 6:自定义周期
	case 
	when price_mode=2 then '按周'
	when price_mode=3 then '按月'
	when price_mode=4 then '按半月'
	when price_mode=6 then '自定义周期'
	else price_mode end as price_mode,
    price_day,  -- 价格周期日
    cycle_days,  -- 自定义周期天数
    `next_day`  -- 价格周期日后天数	
  from dev.csx_ods_csx_b2b_sss_sss_customer_config_ss_df
  where is_deleted=0  -- 删除状态：0:正常、1:删除
  and shipper_code='YHCSX'  
  
  ) d on a1.customer_code = d.customer_code
-- 对账、开票
  left join customer_bill_invoice e on a1.customer_code = e.customer_code
left join  
(
select customer_code,
  min(first_business_sale_date) as first_business_sale_date,
  max(last_business_sale_date) as last_business_sale_date
from 
(
  select customer_code,business_type_code,
  	first_business_sale_date,last_business_sale_date
  from csx_dws.csx_dws_crm_customer_business_active_di
  where sdt = 'current'
  -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
  and business_type_code in('1')
)a 
group by customer_code
)f on a1.customer_code=f.customer_code 
left join cust_sale g on a1.customer_code=g.customer_code
-- where a.sale_amt_tj<>0
;



-- 后置定价配置表 hive
select 
	b.performance_region_name,     --  销售大区名称(业绩划分)
	b.performance_province_name,     --  销售归属省区名称
	b.performance_city_name,     --  城市组名称(业绩划分)  
    a.customer_code,
	b.customer_name,
	c.first_sale_date,c.last_sale_date,
    -- statement_user_code,
	-- statement_user_name,
	-- is_post_offer as is_post_position,
	-- price_mode,  -- 报价周期 2:按周 3:按月 4:按半月 6:自定义周期
	case 
	when a.price_mode=2 then '按周'
	when a.price_mode=3 then '按月'
	when a.price_mode=4 then '按半月'
	when a.price_mode=6 then '自定义周期'
	else a.price_mode end as price_mode,
	if(a.price_mode=6,a.cycle_days,a.price_day) as price_day,  -- 价格周期日
    -- a.price_day,  -- 价格周期日
    -- a.cycle_days,  -- 自定义周期天数
    a.`next_day`,  -- 价格周期日后天数
	e.price_type_name -- 报价类型 1:下单前报价  2:发货前报价  3:发货后报价'	
from
(
select *
  from dev.csx_ods_csx_b2b_sss_sss_customer_config_ss_df
  where is_deleted=0  -- 删除状态：0:正常、1:删除
  and shipper_code='YHCSX'
  and is_post_offer=1  -- 是否后置报价 0-否 1-是
)a 
left join 
(
select dev_source_name,
	performance_region_name,     --  销售大区名称(业绩划分)
	performance_province_name,     --  销售归属省区名称
	performance_city_name,     --  城市组名称(业绩划分)
	customer_code,
	customer_name,     --  客户名称
	-- first_category_code,     --  一级客户分类编码
	first_category_name,     --  一级客户分类名称
	-- second_category_code,     --  二级客户分类编码
	second_category_name,     --  二级客户分类名称
	-- third_category_code,     --  三级客户分类编码
	third_category_name     --  三级客户分类名称
from csx_dim.csx_dim_crm_customer_info
where sdt='current'
and shipper_code='YHCSX'
-- and customer_type_code=4
)b on a.customer_code=b.customer_code   
left join 
(
  select 
    customer_code,first_sale_date,last_sale_date,sale_total_amt
  from csx_dws.csx_dws_crm_customer_active_di
  where sdt ='current' 
  and shipper_code='YHCSX'
)c on c.customer_code=a.customer_code 
-- 最新一次会签信息
left join 
(
select *
from 
(
  select *,
  case business_attribute
  when '日配客户' then 1
  when '福利客户' then 2
  when '大宗贸易' then 5
  when 'M端' then 9
  when 'BBC' then 6
  when '内购' then 3
   end as business_attribute_code,
  row_number() over(partition by customer_number order by create_time desc) as ranks 
  from csx_analyse.csx_analyse_crm_countersign_info_df
  where countersign_type='新客会签'
  -- and execute_time>=date_sub(current_date,31)
  -- and execute_time<=date_sub(current_date,1)
  and status='已完成'
  and is_valid='是'
  and business_attribute='日配客户'
  )a where ranks=1
)d on a.customer_code=d.customer_number 
-- 新客会签填写报价类型是否后置
left join 
(
select *,
case 
when price_type=1 then '下单前报价'
when price_type=2 then '发货前报价'
when price_type=3 then '发货后报价'
else price_type end as price_type_name -- 报价类型 1:下单前报价  2:发货前报价  3:发货后报价'
from csx_ods.csx_ods_csx_crm_prod_customer_countersign_details_df
where shipper_code='YHCSX'
)e on d.id=e.countersign_id  




  

select 
source_bill_no,  -- 来源单号
customer_code,  -- 客户编码
customer_bill_date,  -- 客户对账日期
invoice_status_code,  -- 开票状态 10 待开票20 开票完成30 部分开票
sdt
customer_bill_date,
invoice_status_code,order_amt
from csx_dwd.csx_dwd_sss_source_bill_di
where sdt>='20250801'
and shipper_code='YHCSX'
-- and order_status='SUCCESS' 字段无效
-- 单据类型 10正常单 11福利单  20退货单 30返利单 40尾差调整单
-- and bill_type=10 
-- and delete_flag='0'
-- and source_bill_no not like 'OC%'
-- and source_bill_no not like 'R%'
and customer_code='116522'
and source_bill_no like'OC25101200040%'


