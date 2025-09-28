--昨日
set i_sdate =date_sub(current_date,1);
--昨日分区样式
set i_sdate_1 =regexp_replace(date_sub(current_date,1),'-','');
-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
--set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
--set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
--set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
--set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');


select
	case when coalesce(a.division_code,b.division_code) in ('10','11') then '生鲜' when coalesce(a.division_code,b.division_code) in ('12','13') then '食百' else '易耗品' end as division,
	case coalesce(a.product_level,b.product_level)
		when 1 then '特级'
		when 2 then '一级'
		when 3 then '正常'
		when 4 then 'B'
		when 5 then '定制'
		when 6 then '小'
		else coalesce(a.product_level,b.product_level)
	end as product_level,
	--coalesce(sum(sales_sku),0) as sales_sku,
	--coalesce(sum(sales_value),0) as sales_value,
	--coalesce(sum(profit)/sum(sales_value),0) as profit_rate,
	--coalesce(sum(remark_sku),0) as remark_sku,
	coalesce(sum(sales_level_sku),0) as sales_level_sku,
	coalesce(sum(sales_level_value),0) as sales_level_value,
	coalesce(sum(sales_level_profit)/sum(sales_level_value),0) as sales_level_profit_rate,
	coalesce(sum(remark_level_sku),0) as remark_level_sku,
	coalesce(sum(goods_level_sku),0) as goods_level_sku,
	${hiveconf:i_sdate_1} as update_time,
	substr(${hiveconf:i_sdate_1},1,6) as sdt
from
	(
	select
		c.division_code,
		c.product_level,
		count(distinct a.goods_code) as sales_sku,
		sum(a.sales_value) as sales_value,
		sum(a.profit) as profit,
		count(distinct if(b.goods_code is not null,a.goods_code,null)) as remark_sku,
		count(distinct if(c.product_level <> '-1',a.goods_code,null)) as sales_level_sku,
		sum(if(c.product_level <> '-1',a.sales_value,null)) as sales_level_value,
		sum(if(c.product_level <> '-1',a.profit,null)) as sales_level_profit,
		count(distinct if(b.goods_code is not null and c.product_level <> '-1',a.goods_code,null)) as remark_level_sku
	from
		(
		select
			order_no,
			goods_code,
			sales_value,
			profit
		from 
			csx_dw.dws_sale_r_d_customer_sale
		where 
			sdt between ${hiveconf:i_sdate_21} and ${hiveconf:i_sdate_1}
			and channel in ('1','7','9') --1-大客户 2-商超 4-大宗 5-供应链（食百） 6-供应链（生鲜） 7-企业购 9-业务代理
			and is_self_sale = 1 --自营，联营(非自营)，1自营，0联营(非自营) 202007及之后的可以这样限制
		) a
		left join
			(
			select
				order_no, 
				refund_no, 
				goods_code, 
				spec_remarks, 
				buyer_remarks
			from 
				csx_dw.dwd_csms_r_d_yszx_order_detail_new
			where 
				(sdt >= ${hiveconf:i_sdate_21} or sdt = '19990101')
				and (spec_remarks <> '' or buyer_remarks <> '') 
				and (item_status is null or item_status <> 0)
			group by
				order_no, 
				refund_no, 
				goods_code, 
				spec_remarks, 
				buyer_remarks					
			) b on a.order_no = coalesce(b.refund_no, b.order_no) and a.goods_code = b.goods_code
		left join
			(
			select
				goods_id,
				division_code,
				product_level
			from 
				csx_dw.dws_basic_w_a_csx_product_m
			where 
				sdt = 'current'
			) c on a.goods_code = c.goods_id
	where
		c.division_code in (10,11,12,13) --10-加工部 11-生鲜部 12-食品类 13-用品类 14-服装 15-易耗品
		and c.product_level <> -1
	group by
		c.division_code,c.product_level
	) as a	
	full join
		(
		select
			division_code,
			product_level,
			count(distinct goods_id) as goods_level_sku
		from 
			csx_dw.dws_basic_w_a_csx_product_m
		where 
			sdt = 'current' 
			and division_code in (10,11,12,13) --10-加工部 11-生鲜部 12-食品类 13-用品类 14-服装 15-易耗品
			and product_level <> '-1'  --商品等级,1-特级,2-一级,3-正常,4-B,5-定制,6-小,-1-其它
		group by
			division_code,product_level
		) as b on b.division_code=a.division_code and b.product_level=a.product_level
group by
	case when coalesce(a.division_code,b.division_code) in ('10','11') then '生鲜' when coalesce(a.division_code,b.division_code) in ('12','13') then '食百' else '易耗品' end,
	case coalesce(a.product_level,b.product_level)
		when 1 then '特级'
		when 2 then '一级'
		when 3 then '正常'
		when 4 then 'B'
		when 5 then '定制'
		when 6 then '小'
		else coalesce(a.product_level,b.product_level)
	end	
;
		