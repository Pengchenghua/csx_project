-- ******************************************************************** 
-- @功能描述：
-- @创建者： 孔云 
-- @创建者日期：2025-04-08 16:00:02 
-- @修改者日期：
-- @修改人：
-- @修改内容：
-- ******************************************************************** 
-- 调整am内存
SET tez.am.resource.memory.mb=4096;
-- 调整container内存
SET hive.tez.container.size=8192;

-- 查询日配历史3个月销售情况 
select substr(a.sdt,1,6) as smonth, 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	(case when b.classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end) as sx_or_sb,
	extra,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit
-- 	sum(a.profit)/abs(sum(a.sale_amt)) as profitlv
from 
	(select 
		* 
	from csx_analyse.csx_analyse_bi_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-3),'-','') 
	and sdt<=regexp_replace('${yes_date}','-','')  
	and business_type_code in ('1') 
	) a 
	left join 
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) b 
	on a.goods_code=b.goods_code 
	left join 
	(select
	    code as type,
	    max(name) as name,
	    max(extra) as extra 
	from csx_dim.csx_dim_basic_topic_dict_df
	where parent_code = 'direct_delivery_type' 
	group by code 
	) g 
	on a.direct_delivery_type_code=g.type 
-- where g.extra='采购参与'
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	(case when b.classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end),
	extra,
	substr(a.sdt,1,6) 
;

-- 财务目标


select
month as smonth,
channel_name,
region_name,
province_name,
city_group_name,
business_type_code,
business_type_name,
sum(cast(sales_value as decimal(26,0))) total_sale_amt,
sum(cast(profit as decimal(26,0))) total_profit
from    csx_ods.csx_ods_csx_data_market_dws_basic_w_a_business_target_manage_df ---kpi目标
where month=substr(regexp_replace('${sdt_yes_date}','-',''),1,6)
-- and province_name not like '平台%'
and business_type_code in ('1')
group by month,
channel_name,
region_name,
province_name,
city_group_name,
business_type_name,
business_type_code

;


-- -----------------恩平总定的目标
drop table if exists csx_analyse_tmp.csx_analyse_tmp_target_next_month;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_target_next_month as 
union all select '合肥市' as city_name,11887200 as sale_amt_target,2216489 as profit_target 
union all select '阜阳市' as city_name,3318000 as sale_amt_target,608598 as profit_target 
union all select '台州市' as city_name,2322000 as sale_amt_target,352383 as profit_target 
union all select '杭州市' as city_name,6450000 as sale_amt_target,1165647 as profit_target 
union all select '宁波市' as city_name,2531070 as sale_amt_target,525339 as profit_target 
union all select '武汉市' as city_name,1625000 as sale_amt_target,309219 as profit_target 
union all select '江苏盐城' as city_name,1368000 as sale_amt_target,210915 as profit_target 
union all select '南京主城' as city_name,14175000 as sale_amt_target,2887983 as profit_target 
union all select '上海松江' as city_name,8091000 as sale_amt_target,1589749 as profit_target 
union all select '江苏苏州' as city_name,7700000 as sale_amt_target,1528485 as profit_target 
union all select '南昌市' as city_name,4638576 as sale_amt_target,665296 as profit_target 
union all select '泉州市' as city_name,4758000 as sale_amt_target,966025 as profit_target 
union all select '莆田市' as city_name,4949717 as sale_amt_target,895690 as profit_target 
union all select '龙岩市' as city_name,900508 as sale_amt_target,196486 as profit_target 
union all select '宁德市' as city_name,0 as sale_amt_target,0 as profit_target 
union all select '三明市' as city_name,2838000 as sale_amt_target,439940 as profit_target 
union all select '南平市' as city_name,4327500 as sale_amt_target,726830 as profit_target 
union all select '厦门市' as city_name,5600000 as sale_amt_target,1149734 as profit_target 
union all select '福州市' as city_name,19942000 as sale_amt_target,5198126 as profit_target 
union all select '深圳市' as city_name,22725000 as sale_amt_target,3369691 as profit_target 
union all select '广东广州' as city_name,9000000 as sale_amt_target,1411302 as profit_target 
union all select '贵阳市' as city_name,5011207 as sale_amt_target,1202516 as profit_target 
union all select '成都市' as city_name,20750000 as sale_amt_target,4584221 as profit_target 
union all select '宜宾' as city_name,2542000 as sale_amt_target,425424 as profit_target 
union all select '重庆主城' as city_name,34875500 as sale_amt_target,6840711 as profit_target 
union all select '黔江区' as city_name,1952572 as sale_amt_target,421764 as profit_target 
union all select '万州区' as city_name,1379955 as sale_amt_target,348307 as profit_target 
union all select '石柱县' as city_name,0 as sale_amt_target,0 as profit_target 
union all select '北京市' as city_name,50249600 as sale_amt_target,10060987 as profit_target 
union all select '郑州市' as city_name,12802773 as sale_amt_target,1789143 as profit_target 
union all select '西安市' as city_name,14784000 as sale_amt_target,1962900 as profit_target 
union all select '石家庄市' as city_name,18349382 as sale_amt_target,2777371 as profit_target 
union all select '哈尔滨市' as city_name,7559488 as sale_amt_target,1085568 as profit_target 
union all select'大连'as city_name,7500000 as sale_amt_target,1247334.3 as profit_target 

;

-- -----------------近4个月历史数据情况
drop table if exists csx_analyse_tmp.csx_analyse_tmp_last_4_months_sale_detail;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_last_4_months_sale_detail as 
select 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	(case when b.classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end) as sx_or_sb,
	b.classify_large_code,
	b.classify_large_name,
	b.classify_middle_code,
	b.classify_middle_name,
	sum(a.sale_amt) as sale_amt,
	sum(a.profit) as profit,
	sum(a.profit)/abs(sum(a.sale_amt)) as profitlv,
	sum(case when a.order_channel_code=6 then a.profit end) as tj_profit,
	sum(case when a.order_channel_code=6 then a.sale_amt end) as tj_sale_amt,
	sum(case when a.order_channel_code<>6 and a.order_channel_code<>4 and a.order_channel_code<>5 and a.refund_order_flag=1 then a.profit end) as th_profit,
	sum(case when a.order_channel_code<>6 and a.order_channel_code<>4 and a.order_channel_code<>5 and a.refund_order_flag=1 then a.sale_amt end) as th_sale_amt,
	sum(case when a.order_channel_code<>6 and a.order_channel_code<>4 and a.order_channel_code<>5 and a.refund_order_flag<>1 and a.delivery_type_code=2 then a.profit end) as zs_profit,
	sum(case when a.order_channel_code<>6 and a.order_channel_code<>4 and a.order_channel_code<>5 and a.refund_order_flag<>1 and a.delivery_type_code=2 then a.sale_amt end) as zs_sale_amt 
from 
	(select 
		* 
	from csx_analyse.csx_analyse_bi_sale_detail_di 
	where sdt>=regexp_replace(add_months(trunc('${yes_date}','MM'),-3),'-','') 
	and sdt<regexp_replace(trunc('${yes_date}','MM'),'-','')  
	and business_type_code in ('1') 
	) a 
	left join 
	(select * 
	from csx_dim.csx_dim_basic_goods 
	where sdt='current'
	) b 
	on a.goods_code=b.goods_code 
	left join 
	(select
	    code as type,
	    max(name) as name,
	    max(extra) as extra 
	from csx_dim.csx_dim_basic_topic_dict_df
	where parent_code = 'direct_delivery_type' 
	group by code 
	) g 
	on a.direct_delivery_type_code=g.type 
where g.extra='采购参与'
group by 
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	b.classify_large_code,
	b.classify_large_name,
	b.classify_middle_code,
	b.classify_middle_name,
	(case when b.classify_large_code in ('B01','B02','B03') then '生鲜' else '食百' end)
;

-- --------------------------------------------------------------------------------------------------市调趋势数据
-- -------------------------------------
-- ------商品状态异常的商品
drop table if exists csx_analyse_tmp.csx_analyse_tmp_abnormal_goods_ky_target;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_abnormal_goods_ky_target as 
select 
    b.performance_region_name,
    b.performance_province_name,
    b.performance_city_name,
    a.goods_code 
from 
(select * 
from csx_dim.csx_dim_basic_dc_goods 
where sdt='current'  
and goods_status_name not like 'B%'
) a 
left join 
(select * 
from csx_dim.csx_dim_shop  
where sdt='current' 
) b 
on a.dc_code=b.shop_code 
group by 
    b.performance_region_name,
    b.performance_province_name,
    b.performance_city_name,
    a.goods_code
;

-- -------------------------------------
-- ------各城市销量数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target as 
select 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		d.customer_name,
		f.rp_service_user_work_no_new,
		f.rp_service_user_name_new,
		e.classify_large_code,
		e.classify_large_name,
		e.classify_middle_code,
		e.classify_middle_name,
		e.classify_small_code,
		e.classify_small_name,
		a.goods_code,
		e.goods_name,
		e.unit_name,
		sum(a.sale_amt) as sale_amt,
		sum(a.sale_qty) as sale_qty,
		sum(a.sale_amt)/sum(a.sale_qty) as sale_price,
		sum(a.sale_amt-a.profit)/sum(a.sale_qty) as cost_price 
from 
		(select * 
		from csx_analyse.csx_analyse_bi_sale_detail_di  
		where sdt>=regexp_replace(date_add('${yes_date}',-29),'-','') 
		and sdt<=regexp_replace(date_add('${yes_date}',-0),'-','') 
		and business_type_code=1  
		and order_channel_code not in ('4','6','5') -- 剔除所有异常
		and refund_order_flag<>1 
		and delivery_type_code<>2 
		-- and shipper_code='YHCSX' 
		) a 
		left join 
		csx_analyse_tmp.csx_analyse_tmp_abnormal_goods_ky_target c 
		on a.performance_region_name=c.performance_region_name and a.performance_province_name=c.performance_province_name and a.performance_city_name=c.performance_city_name and a.goods_code=c.goods_code 
		left join 
		(select * 
		from csx_dim.csx_dim_crm_customer_info 
		where sdt='current' 
		and shipper_code='YHCSX'
		) d
		on a.customer_code=d.customer_code 
		left join 
		-- -----商品数据
		(select * 
		from csx_dim.csx_dim_basic_goods 
		where sdt='current' 
		) e 
		on a.goods_code=e.goods_code 
		left join 
		-- -----服务管家
		(select * 
		from csx_analyse.csx_analyse_report_crm_customer_sale_service_manager_info_df    
		where sdt='${yes_sdt}' 
		) f 
		on a.customer_code=f.customer_no 
		left join 
		-- 价格补救原单数据
		(select 
			original_order_code,
			customer_code,
			sub_customer_code,
			goods_code 
		from csx_analyse.csx_analyse_bi_sale_detail_di  
		where sdt>=regexp_replace(date_add('${yes_date}',-29),'-','') 
		and sdt<=regexp_replace(date_add('${yes_date}',-0),'-','') 
		and business_type_code=1  
		and order_channel_code='5' 
		group by 
			original_order_code,
			customer_code,
			sub_customer_code,
			goods_code 
		) g 
		on a.order_code=g.original_order_code and a.customer_code=g.customer_code and a.sub_customer_code=g.sub_customer_code and a.goods_code=g.goods_code 
		left join 
        (select
            code as type,
            max(name) as name,
            max(extra) as extra 
        from csx_dim.csx_dim_basic_topic_dict_df
        where parent_code = 'direct_delivery_type' 
        group by code 
        ) h 
        on a.direct_delivery_type_code=h.type 
	where c.goods_code is null and g.original_order_code is null and h.extra='采购参与' 
	group by 
		a.performance_region_name,
		a.performance_province_name,
		a.performance_city_name,
		a.inventory_dc_code,
		a.customer_code,
		d.customer_name,
		e.classify_large_code,
		e.classify_large_name,
		e.classify_middle_code,
		e.classify_middle_name,
		e.classify_small_code,
		e.classify_small_name,
		a.goods_code,
		e.goods_name,
		e.unit_name,
		f.rp_service_user_work_no_new,
		f.rp_service_user_name_new 
;

-- -- -------------------------------------
-- 各城市对标市调地点
drop table csx_analyse_tmp.market_code_tmp;
create  table csx_analyse_tmp.market_code_tmp
as
select '北京市' as city_name,'RW7' as shop_code,1 as pm 
union all select '石家庄市' as city_name,'RW440' as shop_code,1 as pm 
union all select '郑州市' as city_name,'YW137' as shop_code,1 as pm 
union all select '西安市' as city_name,'YS33' as shop_code,1 as pm 
union all select '合肥市' as city_name,'ZD273' as shop_code,1 as pm 
union all select '阜阳市' as city_name,'ZD304' as shop_code,1 as pm 
union all select '武汉市' as city_name,'ZD23' as shop_code,1 as pm 
union all select '南京主城' as city_name,'RW366' as shop_code,1 as pm 
union all select '上海松江' as city_name,'ZD615' as shop_code,1 as pm 
union all select '江苏苏州' as city_name,'RW43' as shop_code,1 as pm 
union all select '台州市' as city_name,'RS142' as shop_code,1 as pm 
union all select '宁波市' as city_name,'ZD605' as shop_code,1 as pm 
union all select '杭州市' as city_name,'YW90' as shop_code,1 as pm 
union all select '漳州市' as city_name,'ZD246' as shop_code,1 as pm 
union all select '厦门市' as city_name,'ZD212' as shop_code,1 as pm 
union all select '三明市' as city_name,'ZD361' as shop_code,1 as pm 
union all select '泉州市' as city_name,'ZD300' as shop_code,1 as pm 
union all select '莆田市' as city_name,'ZD180' as shop_code,1 as pm 
union all select '南平市' as city_name,'ZD641' as shop_code,1 as pm 
union all select '龙岩市' as city_name,'ZD202' as shop_code,1 as pm 
union all select '福州市' as city_name,'ZD478' as shop_code,1 as pm 
union all select '广东广州' as city_name,'YW173' as shop_code,1 as pm 
union all select '深圳市' as city_name,'YW173' as shop_code,1 as pm 
union all select '南昌市' as city_name,'ZD360' as shop_code,1 as pm 
union all select '贵阳市' as city_name,'ZD21' as shop_code,1 as pm 
union all select '宜宾' as city_name,'ZD588' as shop_code,1 as pm 
union all select '成都市' as city_name,'Zd92' as shop_code,1 as pm 
union all select '重庆主城' as city_name,'ZD109' as shop_code,1 as pm 
union all select '万州区' as city_name,'ZD109' as shop_code,1 as pm 
union all select '黔江区' as city_name,'ZD61' as shop_code,1 as pm 
union all select '北京市' as city_name,'ZD16' as shop_code,2 as pm 
union all select '郑州市' as city_name,'RW94' as shop_code,2 as pm 
union all select '西安市' as city_name,'ZD37' as shop_code,2 as pm 
union all select '合肥市' as city_name,'ZD375' as shop_code,2 as pm 
union all select '南京主城' as city_name,'ZD383' as shop_code,2 as pm 
union all select '上海松江' as city_name,'RW312' as shop_code,2 as pm 
union all select '江苏苏州' as city_name,'ZD277' as shop_code,2 as pm 
union all select '杭州市' as city_name,'ZD178' as shop_code,2 as pm 
union all select '南平市' as city_name,'ZD463' as shop_code,2 as pm 
union all select '福州市' as city_name,'ZD258' as shop_code,2 as pm 
union all select '广东广州' as city_name,'RW494' as shop_code,2 as pm 
union all select '深圳市' as city_name,'ZD325' as shop_code,2 as pm 
union all select '贵阳市' as city_name,'ZD287' as shop_code,2 as pm 
union all select '宜宾' as city_name,'ZD416' as shop_code,2 as pm 
union all select '成都市' as city_name,'ZD568' as shop_code,2 as pm 
union all select '重庆主城' as city_name,'ZD47' as shop_code,2 as pm 
union all select '北京市' as city_name,'YW8' as shop_code,3 as pm 
union all select '郑州市' as city_name,'ZD358' as shop_code,3 as pm 
union all select '合肥市' as city_name,'RS594' as shop_code,3 as pm 
union all select '南京主城' as city_name,'ZD413' as shop_code,3 as pm 
union all select '上海松江' as city_name,'YW381' as shop_code,3 as pm 
union all select '江苏苏州' as city_name,'ZD386' as shop_code,3 as pm 
union all select '杭州市' as city_name,'ZD293' as shop_code,3 as pm 
union all select '南平市' as city_name,'RS211' as shop_code,3 as pm 
union all select '福州市' as city_name,'ZD316' as shop_code,3 as pm 
union all select '广东广州' as city_name,'ZD571' as shop_code,3 as pm 
union all select '贵阳市' as city_name,'ZD566' as shop_code,3 as pm 
union all select '宜宾' as city_name,'ZD414' as shop_code,3 as pm 
union all select '成都市' as city_name,'ZD569' as shop_code,3 as pm 
union all select '北京市' as city_name,'RW221' as shop_code,4 as pm 
union all select '郑州市' as city_name,'ZD595' as shop_code,4 as pm 
union all select '上海松江' as city_name,'RW380' as shop_code,4 as pm 
union all select '杭州市' as city_name,'ZD179' as shop_code,4 as pm 
union all select '南平市' as city_name,'ZD396' as shop_code,4 as pm 
union all select '贵阳市' as city_name,'YW608' as shop_code,4 as pm 
union all select '成都市' as city_name,'9241' as shop_code,4 as pm 
union all select '南平市' as city_name,'ZD474' as shop_code,5 as pm 
union all select '北京市' as city_name,'RW494' as shop_code,6 as pm 
union all select '郑州市' as city_name,'RW494' as shop_code,6 as pm 
union all select '西安市' as city_name,'RW494' as shop_code,6 as pm 
union all select '合肥市' as city_name,'RW494' as shop_code,6 as pm 
union all select '阜阳市' as city_name,'RW494' as shop_code,6 as pm 
union all select '武汉市' as city_name,'RW494' as shop_code,6 as pm 
union all select '南京主城' as city_name,'RW494' as shop_code,6 as pm 
union all select '上海松江' as city_name,'RW494' as shop_code,6 as pm 
union all select '江苏苏州' as city_name,'RW494' as shop_code,6 as pm 
union all select '台州市' as city_name,'RW494' as shop_code,6 as pm 
union all select '宁波市' as city_name,'RW494' as shop_code,6 as pm 
union all select '杭州市' as city_name,'RW494' as shop_code,6 as pm 
union all select '漳州市' as city_name,'RW494' as shop_code,6 as pm 
union all select '厦门市' as city_name,'RW494' as shop_code,6 as pm 
union all select '三明市' as city_name,'RW494' as shop_code,6 as pm 
union all select '泉州市' as city_name,'RW494' as shop_code,6 as pm 
union all select '莆田市' as city_name,'RW494' as shop_code,6 as pm 
union all select '南平市' as city_name,'RW494' as shop_code,6 as pm 
union all select '龙岩市' as city_name,'RW494' as shop_code,6 as pm 
union all select '福州市' as city_name,'RW494' as shop_code,6 as pm 
union all select '深圳市' as city_name,'RW494' as shop_code,6 as pm 
union all select '南昌市' as city_name,'RW494' as shop_code,6 as pm 
union all select '贵阳市' as city_name,'RW494' as shop_code,6 as pm 
union all select '宜宾' as city_name,'RW494' as shop_code,6 as pm 
union all select '成都市' as city_name,'RW494' as shop_code,6 as pm 
union all select '重庆主城' as city_name,'RW494' as shop_code,6 as pm 
union all select '万州区' as city_name,'RW494' as shop_code,6 as pm 
union all select '黔江区' as city_name,'RW494' as shop_code,6 as pm 
union all select '石家庄市' as city_name,'RW104' as shop_code,2 as pm 
union all select '石家庄市' as city_name,'RW297' as shop_code,3 as pm 
union all select '石家庄市' as city_name,'RW299' as shop_code,4 as pm 
union all select '石家庄市' as city_name,'RW503' as shop_code,5 as pm 
union all select '石家庄市' as city_name,'RW536' as shop_code,6 as pm 
union all select '石家庄市' as city_name,'RW564' as shop_code,7 as pm 
union all select '石家庄市' as city_name,'ZD63' as shop_code,8 as pm 
union all select '石家庄市' as city_name,'ZD306' as shop_code,9 as pm 
union all select '石家庄市' as city_name,'RW494' as shop_code,10 as pm 
;

-- -- -------------------------------------
-- -- ------查看目前生效及昨天市调的通用市调中，按照市调优先级顺序每个商品应该取哪个市调地点值；
drop table if exists csx_analyse_tmp.csx_analyse_tmp_market_price_target;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_market_price_target as 
select 
	d.*,
	row_number()over(partition by d.performance_city_name,d.goods_code order by d.market_pm) as final_pm
from 
	(select 
		c.performance_city_name,
		a.shop_code,
		b.product_code as goods_code,
		d.pm as market_pm,
		row_number()over(partition by c.performance_city_name,a.shop_code,b.product_code order by a.create_time desc) as pm 
	from 
		(-- 生效区数据
		select 
			shop_code,
			cast(market_goods_id as string) as market_goods_id,
			cast(create_time as string) as create_time 
		from csx_dwd.csx_dwd_price_market_research_price_di   
		where regexp_replace(date(price_begin_time),'-','')<='${yes_sdt}' 
		and regexp_replace(date(price_end_time),'-','')>='${yes_sdt}' 
		union all 
		-- 失效区数据
		select 
			shop_code,
			cast(product_id as string) as market_goods_id,
			cast(create_time as string) as create_time   
		from csx_dwd.csx_dwd_market_research_not_yh_price_di   
		where regexp_replace(date(price_begin_time),'-','')<='${yes_sdt}' 
		and regexp_replace(date(price_end_time),'-','')>='${yes_sdt}'
		) a 
		left join 
		(select * 
	  from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
	  where sdt=regexp_replace(date_sub(current_date(),1),'-','')
	  ) b 
	  on a.market_goods_id=b.id 
	  left join 
	  (select * 
	  from csx_dim.csx_dim_shop 
	  where sdt='current'
	  ) c 
	  on b.location_code=c.shop_code 
	  left join 
	  (select 
	  		city_name,
	  		shop_code,
	  		pm 
	  from csx_analyse_tmp.market_code_tmp 
	  group by 
	  		city_name,
	  		shop_code,
	  		pm 
	  ) d 
	  on a.shop_code=d.shop_code and c.performance_city_name=d.city_name 
	where d.shop_code is not null  
	) d 
where d.pm=1 
; 

-- ---------------------------------------------------------------------------------
-- ---近6周市调价（原表数据）
drop table if exists csx_analyse_tmp.all_market_price_tmp; 
create table if not exists csx_analyse_tmp.all_market_price_tmp as   
select 
    c5.performance_region_name,
    c5.performance_province_name,
    c5.performance_city_name,
    c2.location_code,
    c4.classify_large_code,
    c4.classify_large_name,
    c4.classify_middle_code,
    c4.classify_middle_name,
    c4.classify_small_code,
    c4.classify_small_name,
    c2.product_code,
    c4.goods_name,
    c1.shop_code,
    c1.shop_name,
    (case when c1.shop_name like '%京东%' then '京东'  
          when c1.market_source_type_code=1 then '永辉' 
          when c1.market_source_type_code=4 then '一批' 
          when c1.market_source_type_code=5 then '二批' 
          when c1.market_source_type_code=6 then '终端'  
    end) as market_source_type_name,
    c1.market_research_price,
    regexp_replace(substr(c1.price_begin_time,1,10),'-','') as price_begin_date,
    regexp_replace(substr(c1.price_end_time_new,1,10),'-','') as price_end_date 
from 
    (-- 目前失效数据数据
    select 
      t1.* 
    from 
      (select 
          product_id as market_goods_id,
          source_type_code as market_source_type_code,
          shop_code,
          shop_name,
          cast(price as decimal(20,6)) as market_research_price,
          cast(price_begin_time as string) as price_begin_time,
          cast(price_end_time as string) as price_end_time,
          cast((case when status=0 and price_end_time<update_time then price_end_time else update_time end) as string) as price_end_time_new
      from csx_dwd.csx_dwd_market_research_not_yh_price_di 
      where substr((case when status=0 and price_end_time>update_time then update_time else price_end_time end),1,10)>=date_add('${yes_date}',-20)  
      and substr(price_begin_time,1,10)<='${yes_date}'   
      ) t1 

    union all 
    -- 永辉门店市调数据&目前生效的通用市调
    select 
      tt3.* 
    from 
      (select 
          market_goods_id as market_goods_id,
          cast(market_source_type_code as int) as market_source_type_code,
          shop_code,
          shop_name,
          cast(market_research_price as decimal(20,6)) as market_research_price,
          cast(price_begin_time as string) as price_begin_time,
          cast(price_end_time as string) as price_end_time,
          cast(price_end_time as string) as price_end_time_new  
      from csx_dwd.csx_dwd_price_market_research_price_di  
      where substr(price_end_time,1,10)>=date_add('${yes_date}',-20)   
      and substr(price_begin_time,1,10)<='${yes_date}'    
      and market_source_type_code<>'1' 
      ) tt3 
    ) c1 
    left join 
    (select * 
    from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
    where sdt=regexp_replace(date_sub(current_date(),1),'-','')
    ) c2 
    on c1.market_goods_id=c2.id 
    left join 
    (select * 
    from csx_dim.csx_dim_basic_goods 
    where sdt='current' 
    ) c4 
    on c2.product_code=c4.goods_code 
    left join 
    (select * 
    from csx_dim.csx_dim_shop 
    where sdt='current'
    ) c5 
    on c2.location_code=c5.shop_code 
; 

-- ---------------------------------------------------------------------------
-- 通用市调中间表2，只保留商品市调优先级最高市调数据
drop table if exists csx_analyse_tmp.final_market_price_tmp; 
create table if not exists csx_analyse_tmp.final_market_price_tmp as   
select 
	a.*,
	(last_2_week_market_research_price-last_3_week_market_research_price)/last_3_week_market_research_price as last_2_week_market_research_price_riselv, -- 近第二周市调涨幅
	(last_week_market_research_price-last_2_week_market_research_price)/last_2_week_market_research_price as last_week_market_research_price_riselv  -- 近第一周市调涨幅
from 
	(select 
		c3.performance_region_name,
		c3.performance_province_name,
		c3.performance_city_name,
		c3.shop_code,
		c3.shop_name,
		c3.product_code as goods_code,
		c3.goods_name,

		avg(case when c4.calday>=regexp_replace(date_add('${yes_date}',-20),'-','')  
		  	    and c4.calday<=regexp_replace(date_add('${yes_date}',-14),'-','') 
		then c3.market_research_price 
		end) as last_3_week_market_research_price,
		avg(case when c4.calday>=regexp_replace(date_add('${yes_date}',-13),'-','')  
		  	    and c4.calday<=regexp_replace(date_add('${yes_date}',-7),'-','') 
		then c3.market_research_price 
		end) as last_2_week_market_research_price,
		avg(case when c4.calday>=regexp_replace(date_add('${yes_date}',-6),'-','')  
		  	    and c4.calday<=regexp_replace(date_add('${yes_date}',-0),'-','') 
		then c3.market_research_price 
		end) as last_week_market_research_price 

	from 
		csx_analyse_tmp.all_market_price_tmp c3
		cross join 
		(select *  
		from csx_dim.csx_dim_basic_date 
		where calday>=regexp_replace(date_add('${yes_date}',-20),'-','')    
		and calday<='${yes_sdt}'    
		) c4 
	where c3.price_begin_date<=c4.calday and c3.price_end_date>=c4.calday 
	group by 
	    c3.performance_region_name,
		c3.performance_province_name,
		c3.performance_city_name,
		c3.shop_code,
		c3.shop_name,
		c3.product_code,
		c3.goods_name
	) a 
	left join 
	(select * 
	from csx_analyse_tmp.csx_analyse_tmp_market_price_target 
	where final_pm=1 
	) b 
	on a.performance_city_name=b.performance_city_name and a.goods_code=b.goods_code and a.shop_code=b.shop_code
where b.shop_code is not null 
;

-- ---------------------------------------------------------------------------
-- 用上面销售数据关联市调趋势及销量乘数，以便后面的聚合**********************************************************
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_market;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_market as 
select 
	a.*,

	c.shop_code,
	c.shop_name,
	c.last_3_week_market_research_price,
	c.last_2_week_market_research_price,
	c.last_week_market_research_price,

	c.last_2_week_market_research_price_riselv,-- 近第二周市调涨幅
	c.last_week_market_research_price_riselv, -- 近第一周市调涨幅

	(case when c.last_2_week_market_research_price_riselv>0 and c.last_week_market_research_price_riselv>0 then '是' else '否' end) as if_2_week_rise -- 近2周是否连续上涨
from 
	csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target a 
	left join 
	-- 关联每个商品的市调数据
	csx_analyse_tmp.final_market_price_tmp c 
	on a.performance_city_name=c.performance_city_name and a.goods_code=c.goods_code 
;

-- ---------------------------------------------------------------------------
-- 查看品类整体市调趋势(只看近2周市调趋势)
drop table if exists csx_analyse_tmp.csx_analyse_tmp_classify_market;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_classify_market as 
select 
	b.*,
	(case when last_2_week_market_research_price_classify_riselv>0 and last_week_market_research_price_classify_riselv>0 then '连续2周上涨' 
	      when last_2_week_market_research_price_classify_riselv<0 and last_week_market_research_price_classify_riselv<0 then '连续2周下跌' 
	      when market_research_price_classify_riselv>0 then '前第一周较前第三周上涨' 
	      when market_research_price_classify_riselv<0 then '前第一周较前第三周下跌' 
	end) as market_qushi_type 
from 
	(select 
		a.*,

		(a.last_2_week_market_research_price_classify-a.last_3_week_market_research_price_classify)/a.last_3_week_market_research_price_classify as last_2_week_market_research_price_classify_riselv, -- 近第二周市调涨幅
		(a.last_week_market_research_price_classify-a.last_2_week_market_research_price_classify)/a.last_2_week_market_research_price_classify as last_week_market_research_price_classify_riselv,  -- 近第一周市调涨幅
		
		(a.last_week_market_research_price_classify-a.last_3_week_market_research_price_classify)/a.last_3_week_market_research_price_classify as market_research_price_classify_riselv
	from 
		(select 
			performance_city_name,
			classify_middle_name,
			
			sum(sale_qty*last_3_week_market_research_price)/sum(sale_qty) as last_3_week_market_research_price_classify,
			sum(sale_qty*last_2_week_market_research_price)/sum(sale_qty) as last_2_week_market_research_price_classify,
			sum(sale_qty*last_week_market_research_price)/sum(sale_qty) as last_week_market_research_price_classify 
		from csx_analyse_tmp.csx_analyse_tmp_sale_detail_ky_target_market 
		where last_3_week_market_research_price>0 and last_2_week_market_research_price>0 and last_week_market_research_price>0 
		group by 
			performance_city_name,
			classify_middle_name 
		) a 
	) b 
;

-- -----------------历史数据情况匹配目标数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_and_target;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_sale_and_target as 
select 
	t3.*,

	t3.profit_target_gap+t3.first_profit_for as final_profit_for,-- 最终品类毛利额
	(t3.profit_target_gap+t3.first_profit_for)/abs(t3.sale_amt_target) as final_profitlv_for,-- 最终预估毛利率

	nvl((case when abs(t3.zs_profitlv_effect)>0 then (t3.first_profitlv_for-(t3.profit_target_gap+t3.first_profit_for)/abs(t3.sale_amt_target))/t3.zs_profitlv_effect end),0) as zs_fator_add,
	nvl((case when abs(t3.zs_profitlv_effect)=0 and abs(t3.final_market_price_riselv)>0 then (t3.first_profitlv_for-(t3.profit_target_gap+t3.first_profit_for)/abs(t3.sale_amt_target))/t3.final_market_price_riselv end),0) as market_fator_add,
	nvl((case when abs(t3.zs_profitlv_effect)=0 and abs(t3.final_market_price_riselv)=0 and abs(t3.tj_profitlv_effect)>0 then (t3.first_profitlv_for-(t3.profit_target_gap+t3.first_profit_for)/abs(t3.sale_amt_target))/t3.tj_profitlv_effect end),0) as tj_fator_add,
	nvl((case when abs(t3.zs_profitlv_effect)=0 and abs(t3.final_market_price_riselv)=0 and abs(t3.tj_profitlv_effect)=0 and abs(t3.th_profitlv_effect)>0 then (t3.first_profitlv_for-(t3.profit_target_gap+t3.first_profit_for)/abs(t3.sale_amt_target))/t3.th_profitlv_effect end),0) as th_fator_add
	
from 
	(select 
		t2.*,
		(t2.city_profit_target-t2.first_city_profit_for) as city_profit_target_gap,
		(t2.city_profit_target-t2.first_city_profit_for)*t2.city_profit_pro as profit_target_gap -- 品类毛利额缺口
	from 
		(select 
			t1.*,
			sum(t1.first_profit_for)over(partition by t1.performance_city_name) as first_city_profit_for
		from 
			(select 
				a.*,

				a.sale_amt/a.city_sale_amt as city_sale_amt_pro,-- 城市销售额占比
				a.profit/a.city_profit as city_profit_pro,-- 城市毛利额占比

				b.market_qushi_type,
				b.last_2_week_market_research_price_classify_riselv,
				b.last_week_market_research_price_classify_riselv,
				b.market_research_price_classify_riselv,
				nvl(b.final_market_price_riselv,0) as final_market_price_riselv,

				c.sale_amt_target as city_sale_amt_target,
				c.profit_target as city_profit_target,

				c.sale_amt_target*(a.sale_amt/a.city_sale_amt) as sale_amt_target,-- 品类销售额目标

				a.profitlv-a.zs_profitlv_effect*0.1-a.tj_profitlv_effect*0.1-a.th_profitlv_effect*0.1-nvl(b.final_market_price_riselv,0)*0.1 as first_profitlv_for,-- 初版预估毛利率
				(a.profitlv-a.zs_profitlv_effect*0.1-a.tj_profitlv_effect*0.1-a.th_profitlv_effect*0.1-nvl(b.final_market_price_riselv,0)*0.1)*(a.sale_amt/a.city_sale_amt)*c.sale_amt_target as first_profit_for-- 初版预估毛利额 
			from 
				-- 历史数据情况
				(select 
					*,
					cast(profit/abs(sale_amt)-nvl((profit-tj_profit)/abs(sale_amt-tj_sale_amt),0) as decimal(20,4)) as tj_profitlv_effect,
					cast(profit/abs(sale_amt)-nvl((profit-th_profit)/abs(sale_amt-th_sale_amt),0) as decimal(20,4)) as th_profitlv_effect,
					cast(profit/abs(sale_amt)-nvl((profit-zs_profit)/abs(sale_amt-zs_sale_amt),0) as decimal(20,4)) as zs_profitlv_effect,
					sum(sale_amt)over(partition by performance_city_name) as city_sale_amt,
					sum(profit)over(partition by performance_city_name) as city_profit  
				from csx_analyse_tmp.csx_analyse_tmp_last_4_months_sale_detail 
				) a 
				-- 市调趋势数据
				left join 
				(select 
					*, 
					nvl((case when market_qushi_type='连续2周下跌' and last_2_week_market_research_price_classify_riselv>=last_week_market_research_price_classify_riselv then last_2_week_market_research_price_classify_riselv 
							  when market_qushi_type='连续2周下跌' and last_2_week_market_research_price_classify_riselv<last_week_market_research_price_classify_riselv then last_week_market_research_price_classify_riselv 
							  when market_qushi_type='前第一周较前第三周下跌' then market_research_price_classify_riselv   
						end),0) as final_market_price_riselv 
				from csx_analyse_tmp.csx_analyse_tmp_classify_market 
				) b 
				on a.performance_city_name=b.performance_city_name and a.classify_middle_name=b.classify_middle_name 
				-- 目标数据
				left join 
				csx_analyse_tmp.csx_analyse_tmp_target_next_month c 
				on a.performance_city_name=c.city_name 
			where c.city_name is not null 
			) t1 
		) t2 
	) t3 
;

-- -----------------全关联数据
drop table if exists csx_analyse_tmp.csx_analyse_tmp_all_link;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_all_link as 
select 
    a.*,
    b.* 
from 
(select 
    performance_region_name,
	performance_province_name,
	performance_city_name,
	city_sale_amt_target,
	city_profit_target 
from csx_analyse_tmp.csx_analyse_tmp_sale_and_target 
group by 
    performance_region_name,
	performance_province_name,
	performance_city_name,
	city_sale_amt_target,
	city_profit_target 
) a 
cross join 
(select 
    sx_or_sb,
	classify_middle_name 
from csx_analyse_tmp.csx_analyse_tmp_sale_and_target 
group by 
    sx_or_sb,
	classify_middle_name 
) b 
;



drop table if exists csx_analyse_tmp.csx_analyse_tmp_final_target;
create table if not exists csx_analyse_tmp.csx_analyse_tmp_final_target as 

select 
    t1.*,
    t2.profitlv,
    t2.sale_amt_target,
    t2.final_profit_for,
    t2.final_profitlv_for,
    t2.first_profitlv_for,-- 首次预估毛利率
    
    t2.zs_profitlv_effect,
    t2.zs_fator,
    
    t2.tj_profitlv_effect,
    t2.tj_fator,
    
    t2.th_profitlv_effect,
    t2.th_fator,
    
    t2.market_qushi_type,
    t2.last_2_week_market_research_price_classify_riselv,
    t2.last_week_market_research_price_classify_riselv,
    t2.market_research_price_classify_riselv,
    t2.final_market_price_riselv,
    t2.sd_fator 
from 
    csx_analyse_tmp.csx_analyse_tmp_all_link t1 
    left join 
    (select 
    	performance_region_name,
    	performance_province_name,
    	performance_city_name,
    	city_sale_amt_target,
    	city_profit_target,
    	
    	sx_or_sb,
    	classify_middle_name,
    	profitlv,
    	sale_amt_target,
    	final_profit_for,
    	final_profitlv_for,
    	first_profitlv_for,-- 首次预估毛利率
    
    	zs_profitlv_effect,
    	0.1+zs_fator_add as zs_fator,
    
    	tj_profitlv_effect,
    	0.1+tj_fator_add as tj_fator,
    
    	th_profitlv_effect,
    	0.1+th_fator_add as th_fator,
    
    	market_qushi_type,
    	last_2_week_market_research_price_classify_riselv,
    	last_week_market_research_price_classify_riselv,
    	market_research_price_classify_riselv,
    	final_market_price_riselv,
    	0.1+market_fator_add as sd_fator 
    
    from csx_analyse_tmp.csx_analyse_tmp_sale_and_target 
    ) t2 
    on t1.performance_city_name=t2.performance_city_name and t1.sx_or_sb=t2.sx_or_sb and t1.classify_middle_name=t2.classify_middle_name 
;



select 
	performance_region_name as `大区`,
	performance_province_name as `省区`,
	performance_city_name as `城市`,
	sx_or_sb as `生鲜or食百`,
	classify_middle_name as `管理中类`,
	city_sale_amt_target as `城市目标销售额`,
	city_profit_target as `城市目标毛利额`,
	sale_amt_target as `品类目标销售额`,
	final_profit_for as `品类目标毛利额`,
	final_profitlv_for as `品类目标毛利率`,
	profitlv as `品类毛利率基准值`,
	first_profitlv_for as `首次预估毛利率`,

	'' as `徐力最终定品类毛利率目标`,
	'' as `毛利率差值`,

	zs_profitlv_effect as `品类直送影响`,
	zs_fator as `品类直送系数`,

	tj_profitlv_effect as `品类调价影响`,
	tj_fator as `品类调价系数`,

	th_profitlv_effect as `品类退货影响`,
	th_fator as `品类退货系数`,

	market_qushi_type as `品类市调趋势类型`,
	last_2_week_market_research_price_classify_riselv as `品类前第2周较前第3周市调价周环比`,
	last_week_market_research_price_classify_riselv as `品类前第1周较前第2周市调价周环比`,
	market_research_price_classify_riselv as `品类前第1周较前第3周市调价周环比`,
	final_market_price_riselv as `品类品类市调毛利率影响值`,
	sd_fator  as `品类市调系数`,

	zs_fator as `品类直送最终调整系数`,
	tj_fator as `品类调价最终调整系数`,
	th_fator as `品类退货最终调整系数`, 
	sd_fator  as `品类市调最终调整系数` 
from csx_analyse_tmp.csx_analyse_tmp_final_target 