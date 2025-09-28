-- 日配异常周分析 
drop table csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_ky ; 
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_ky as 
select 
--	weeks as month,
	(case when a.performance_province_name='河南省' then '华北大区' 
	      when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end) as performance_region_name,
	a.performance_province_name,
	(case when a.performance_province_name in ('上海松江') then '上海松江' 
	      when a.performance_province_name in ('江苏苏州') then '江苏苏州'  
	else a.performance_city_name end) as performance_city_name,
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1) as price_type1,
	nvl(f.sec_price_type,e.price_type2) as price_type2,
	e.price_period_name,
	e.price_date_name,
	c.classify_large_name,
	c.classify_middle_name,
	-- c.classify_small_name,
	-- a.goods_code,
	-- c.goods_name,
	(case  when a.delivery_type_code=2 then '直送单' 
	      when a.order_channel_code=6 then '调价单' 
		  when a.order_channel_code=4 then '返利单' 
		  when a.refund_order_flag=1 then '退货单' 
		  when a.order_channel_detail_code=26 then '价格补救单' 
	else '其他' end) as order_type,
	case  
	    when direct_delivery_type=1 then '直送1'
	    when direct_delivery_type=2 then '直送2'
	    when direct_delivery_type=11 then '临时加单'
	    when direct_delivery_type=12 then '紧急补货'
	   else '普通' end as  direct_delivery_name,
	sum(case when weeks='本周' then a.sale_amt end ) as sale_amt,
	sum(case when weeks='本周' then a.profit   end ) as profit,
	sum(case when weeks='本周' then a.sale_qty end ) as sale_qty ,
	sum(case when weeks='上周' then a.sale_amt end ) as last_sale_amt,
	sum(case when weeks='上周' then a.profit   end ) as last_profit,
	sum(case when weeks='上周' then a.sale_qty end ) as last_sale_qty 
from 
	(select * ,case when sdt between '20231130' and '20231206' then '上周' else '本周' end weeks
	from   csx_dws.csx_dws_sale_detail_di 
	where
	((sdt>='20231130' and sdt<='20231206')
	or ( sdt>='20231207' and sdt<='20231213'))
	and business_type_code in ('1') 
	and inventory_dc_code not in  ('W0J2','W0AJ','W0G6','WB71')
	) a 
	left join 
	(select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=1 
	) b 
	on a.inventory_dc_code=b.shop_code 
	left join 
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current' 
	) c 
	on a.goods_code=c.goods_code 
	left join 
	(select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
	) d 
	on a.customer_code=d.customer_code 
	left join 
	csx_analyse_tmp.csx_analyse_tmp_customer_price_type_ky e 
	on a.customer_code=e.customer_code 
	left join 
	-- 线下表客户定价类型
	dev.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	on a.customer_code=f.customer_code 
where b.shop_code is null 
group by 
-- weeks,
	(case when a.performance_province_name='河南省' then '华北大区' 
	      when a.performance_province_name in ('安徽省','湖北省') then '华东大区' 
	else a.performance_region_name end),
	a.performance_province_name,
	(case when a.performance_province_name in ('上海松江') then '上海松江' 
	      when a.performance_province_name in ('江苏苏州') then '江苏苏州'  
	else a.performance_city_name end),
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1),
	nvl(f.sec_price_type,e.price_type2),
	e.price_period_name,
	e.price_date_name,
	c.classify_large_name,
	c.classify_middle_name,
	-- c.classify_small_name,
	-- a.goods_code,
	-- c.goods_name,
	(case  when a.delivery_type_code=2 then '直送单' 
	      when a.order_channel_code=6 then '调价单' 
		  when a.order_channel_code=4 then '返利单' 
		  when a.refund_order_flag=1 then '退货单' 
		  when a.order_channel_detail_code=26 then '价格补救单' 
	else '其他' end),
		case  
	    when direct_delivery_type=1 then '直送1'
	    when direct_delivery_type=2 then '直送2'
	    when direct_delivery_type=11 then '临时加单'
	    when direct_delivery_type=12 then '紧急补货'
	   else '普通' end
;

select performance_region_name,
	 a.performance_province_name,
	  performance_city_name,
     order_type,
     direct_delivery_name,
	 sum(sale_amt)sale_amt,
	 sum(profit)profit,
	 sum(sale_qty )sale_qty ,
	 sum(last_sale_amt)last_sale_amt,
	 sum(last_profit)last_profit,
	 sum(last_sale_qty)last_sale_qty  
from  csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_ky a
group by performance_region_name,
	 a.performance_province_name,
	 performance_city_name,
     order_type,direct_delivery_name
     ;
-- 剔除客户深圳航空
	 select performance_region_name,
	 a.performance_province_name,
	  performance_city_name,
     order_type,
     direct_delivery_name,
	 sum(if(customer_code='107592' and order_type='调价单' ,0,sale_amt))/10000 sale_amt,
	 sum(if(customer_code='107592' and order_type='调价单' ,0,profit))/10000 profit,
	 sum(sale_qty )sale_qty ,
	 sum(if(customer_code='107592' and order_type='调价单' ,0,last_sale_amt))/10000 last_sale_amt,
	 sum(if(customer_code='107592' and order_type='调价单' ,0,last_profit))/10000 last_profit,
	 sum(last_sale_qty)last_sale_qty  
from  csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_ky a
group by performance_region_name,
	 a.performance_province_name,
	 performance_city_name,
     order_type,direct_delivery_name
     ;
     
     select * from  csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_ky a

-- 直送退货 
select 
    a.sdt,
        a.performance_region_name,     --  销售大区名称(业绩划分)
        a.performance_province_name,     --  销售归属省区名称
        a.performance_city_name,     --  城市组名称(业绩划分)
        a.customer_code,
        c.customer_name,     --  客户名称        
        a.order_code,
        a.business_type_name,
        a.delivery_type_name,
        a.classify_middle_name,     --  管理中类名称
        a.goods_code,     --  商品编码
        a.goods_name,     --  商品名称
        a.sale_amt,     --  含税销售金额
        a.profit,     --  含税定价毛利额
        a.sale_qty,     --  销售数量
        b.source_biz_type_name,  -- 订单业务来源
        b.order_status_name,  -- 退货单状态
        b.has_goods_name,
        b.child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
        b.refund_order_type_name,        -- 退货单类型(0:差异单 1:退货单）
        b.first_level_reason_name,
        b.second_level_reason_name        
from 
        (select *,weekofyear(date_sub(to_date(from_unixtime(unix_timestamp(sdt,'yyyyMMdd'))),-2)) week
        from csx_dws.csx_dws_sale_detail_di 
        where sdt>=replace(date_sub(current_date,9),'-','')
        and sdt<=replace(date_sub(current_date,3),'-','') 
        and business_type_code='1' 
        and order_channel_code not in ('4','6') 
        and refund_order_flag=1 
        )a 
left join 
(
select
        inventory_dc_code,
        inventory_dc_name, 
        sdt,        
        refund_code,
        order_status_code,  -- 退货单状态: 10-差异待审(预留) 20-处理中 30-处理完成 -1-差异拒绝
        case order_status_code
        when -1 then '差异拒绝'
        when 10 then '差异待审'
        when 20 then '处理中'
        when 30 then '处理完成'
        else order_status_code end as order_status_name,  -- 退货单状态
        sale_order_code,
        customer_code,
        regexp_replace(regexp_replace(customer_name,'\n',''),'\r','') as customer_name,
        sub_customer_code,
        regexp_replace(regexp_replace(sub_customer_name,'\n',''),'\r','') as sub_customer_name,
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
        end as source_biz_type_name,  -- 订单业务来源（-1-B端订单管理退货 0-OMS物流审核 1-结算调整数量 2-OMS调整数量 3-CRM客诉退货 4-CRM订单售后退货 5-CRM预退货审核 6-CRM签收 7-司机送达时差异 8-司机发起退货 9-实物退仓收货差异 10-OMS签收）
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
        case source_type
        when 0 then '签收差异或退货'
        when 1 then '改单退货'
        end as source_type_name,  -- 订单来源(0-签收差异或退货 1-改单退货)
        case child_return_type_code
        when 0 then '父退货单'
        when 1 then '子退货单逆向'
        when 2 then '子退货单正向'
        end as child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
        case refund_order_type_code
        when 0 then '差异单'
        when 1 then '退货单'
        end as refund_order_type_name,        -- 退货单类型(0:差异单 1:退货单）
        refund_qty,
        sale_price,
        refund_total_amt,
        refund_scale_total_amt,
        first_level_reason_name,
        regexp_replace(regexp_replace(second_level_reason_name,'\n',''),'\r','') as second_level_reason_name
from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
where sdt>=replace(date_sub(current_date,40),'-','')
and child_return_type_code=1
and parent_refund_code<>''
)b on a.order_code=b.refund_code and a.goods_code=b.goods_code
left join 
(
        select  
                bloc_code,     --  集团编码
                bloc_name,     --  集团名称
                parent_id,customer_id,
                customer_code,
                customer_name,     --  客户名称
                first_category_name,     --  一级客户分类名称
                second_category_name,     --  二级客户分类名称
                performance_region_name,     --  销售大区名称(业绩划分)
                performance_province_name,     --  销售归属省区名称
                performance_city_name     --  城市组名称(业绩划分)
        from csx_dim.csx_dim_crm_customer_info
        where sdt='current'
        and customer_type_code=4
)c on a.customer_code=c.customer_code;


-- 调价影响
-- -----------------------------------------------------------------------------------
-- ---------------客户定价类型数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_price_type_01;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_customer_price_type_01 as 
select 
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
;	

-- -----------------------------------------------------------------------------------
-- -----------销售数据--- 客户品类数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_01;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_01 as 
select 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1) as price_type1, -- 定价类型1
	nvl(f.sec_price_type,e.price_type2) as price_type2, -- 定价类型2
	e.price_period_name, -- 报价周期
	e.price_date_name, -- 报价日
	c.classify_middle_code,
	c.classify_middle_name,
	(case when a.delivery_type_code=2 then '直送单'
		  when a.order_channel_code=6 then '调价单' 
		  when a.order_channel_code=4 then '返利单' 
		  when a.refund_order_flag=1 then '退货单' 
		  -- when a.order_channel_detail_code=26 then '价格补救单' 
	else '其他' end) as order_type,
	if(a.delivery_type_code=2 ,'是','否') zhisong_type,
	if(a.order_channel_code=6 and a.customer_code !='107592','是','否') tiaojia_type,
	if(a.order_channel_code=4 ,'是','否') fanli_type,
	if(a.refund_order_flag=1,'是','否') tuihuo_type,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
from 
	(select *   -- 销售额
	from csx_dws.csx_dws_sale_detail_di 
	where sdt>='20231201' and sdt<='20231227'
	-- and channel_code in('1','7','9')
	and business_type_code in ('1') 
	and inventory_dc_code not in ('W0J2','W0AJ','W0G6','WB71')
	) a 
	left join  -- 直送仓数据dc
	(select * 
	from csx_dim.csx_dim_shop  
	where sdt='current' 
	and shop_low_profit_flag=1 
	) b 
	on a.inventory_dc_code=b.shop_code 
	left join  -- 商品信息
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current' 
	) c 
	on a.goods_code=c.goods_code 
	left join  -- 客户表
	(select * 
	from csx_dim.csx_dim_crm_customer_info 
	where sdt='current'
	) d 
	on a.customer_code=d.customer_code 
	left join  -- 线上客户定价类型
	csx_analyse_tmp.csx_analyse_tmp_customer_price_type_ky e 
	on a.customer_code=e.customer_code 
	left join  -- 线下客户定价类型
	dev.csx_ods_data_analysis_prd_cus_price_type_231206_df f  
	on a.customer_code=f.customer_code 
where b.shop_code is null 
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	d.first_category_name,
	d.second_category_name,
	d.third_category_name,
	a.customer_code,
	d.customer_name,
	nvl(f.fir_price_type,e.price_type1), -- 定价类型1
	nvl(f.sec_price_type,e.price_type2), -- 定价类型2
	e.price_period_name, -- 报价周期
	e.price_date_name, -- 报价日
	c.classify_middle_code,c.classify_middle_name,
	(case when a.delivery_type_code=2 then '直送单'
		  when a.order_channel_code=6 then '调价单' 
		  when a.order_channel_code=4 then '返利单' 
		  when a.refund_order_flag=1 then '退货单' 
		  -- when a.order_channel_detail_code=26 then '价格补救单' 
	else '其他' end),
	if(a.delivery_type_code=2 ,'是','否'),
if(a.order_channel_code=6 and a.customer_code !='107592','是','否'),
	if(a.order_channel_code=4 ,'是','否'),
	if(a.refund_order_flag=1,'是','否');
	
---销售数据(客户数据)--- 异常因素 TOP5
-- 客户品类数据 
select middle_ranks_asc,middle_ranks_desc,customer_ranks_asc,customer_ranks_desc,
	order_type,
	performance_region_name,
	performance_province_name,			
	customer_code,customer_name,
	second_category_name,
	price_type1,price_type2,
	price_period_name,price_date_name,
	customer_sale_amt,customer_profit,customer_profit_rate,
	province_sale_amt,province_profit,province_profit_rate,
	classify_middle_name,
	sale_amt,profit,profit_rate	
from 
(select 
	a.*,	
	row_number() over(partition by order_type,customer_code order by  sale_amt  asc) as middle_ranks_asc,-- 1234 客户下品类影响排名
	row_number() over(partition by order_type,customer_code order by  sale_amt  desc) as middle_ranks_desc,
	dense_rank() over(partition by order_type,performance_province_name order by  customer_sale_amt  asc) as customer_ranks_asc,
	dense_rank() over(partition by order_type,performance_province_name order by  customer_sale_amt  desc) as customer_ranks_desc -- 1122 省区下客户影响排名
from
	(select 
		a.*,
		sum(sale_amt)over(partition by order_type,customer_code) customer_sale_amt, -- 客户异常业绩合计
		sum(profit)over(partition by order_type,customer_code) customer_profit,
		sum(profit)over(partition by order_type,customer_code) /abs(sum(sale_amt)over(partition by order_type,customer_code)) customer_profit_rate,sum(sale_amt)over(partition by order_type,performance_province_name) province_sale_amt, -- 省区异常业绩合计
		sum(profit)over(partition by order_type,performance_province_name) province_profit,	
		sum(profit)over(partition by order_type,performance_province_name)/abs(sum(sale_amt)over(partition by order_type,performance_province_name))province_profit_rate
	from 
		(select 
			order_type,
			performance_region_name,
			performance_province_name,			
			customer_code,customer_name,
			classify_middle_code,classify_middle_name,
			second_category_name,
			price_type1,price_type2,
			price_period_name,price_date_name,
			sum(sale_amt) sale_amt,
			sum(profit) profit,
			sum(profit)/abs(sum(sale_amt)) profit_rate		
		from csx_analyse_tmp.csx_analyse_tmp_sale_detail_di_01
		where order_type != '其他'
		group by 
			order_type,
			performance_region_name,
			performance_province_name,			
			customer_code,customer_name,
			classify_middle_code,classify_middle_name,
			second_category_name,
			price_type1,price_type2,
			price_period_name,price_date_name
		
		)a	
	)a
)a
 where if (order_type = '直送单',middle_ranks_desc=1 and customer_ranks_desc<=5,middle_ranks_asc=1 and customer_ranks_asc<=5)

 