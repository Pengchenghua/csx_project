	select distinct
		csx_week, -- 彩食鲜业务周(上周六开始本周五结束)
		concat_ws('-',csx_week_begin,csx_week_end) as csx_week_range		
	from csx_dim.csx_dim_basic_date
	where calday>='20240729' 
	and calday<=regexp_replace(cast(to_date(date_sub(now(),1)) as string),'-','')
	order by csx_week
	
	



-- drop table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi;
-- create table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi as
with
-- 通用市调价
market_research_ty as
(
	select location_code,product_code,
	concat_ws(',',collect_list(concat(shop_code,shop_name))) as shop_code_name,
	concat_ws(',',collect_list(concat(shop_code,shop_name,round(market_research_price,2)))) as market_research_price  -- 市调价格
		-- price_end_time,  -- 生效结束时间
		-- row_number() over(partition by location_code,product_code order by create_date desc) as rum
	from csx_analyse.csx_analyse_fr_price_market_research_price_detail_df
	where substr(price_end_time,1,10)>=cast(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),2) as string)  -- 生效结束时间>=T-2
	and (change_type_name<>'手动失效' or change_type_name is NULL)  -- 页面修改价格类型<>手动失效
	and shop_code not in('ZD273','ZD319','ZD41','ZD527') -- 剔除4个市调地点（ZD273、ZD319、ZD41、ZD527）
	group by location_code,product_code	
),
-- 云超价，通用市调-永辉门店
market_research_yc as
(
select *
from 
(
	select location_code,product_code,
		market_research_price,  -- 市调价格
		market_research_date,
		row_number() over(partition by location_code,product_code order by market_research_date desc) as rum
	from 
	(	
		select 
			b.location_code,
			b.product_code,
			-- regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 		
			a.market_research_date,
			min(a.market_research_price) as market_research_price
		from 
			(
				select * 
				from csx_dwd.csx_dwd_price_market_research_price_di 
				-- where sdt='${sdt_yes}'  -- regexp_replace(date_sub(current_date,1),'-','')
				-- where sdt>=regexp_replace(date_sub('${sdt_date}',dayofweek('${sdt_date}')-2+3),'-','') -- 上周五
				where sdt>=regexp_replace(date_sub('${sdt_date}',if(dayofweek('${sdt_date}')=1,9,dayofweek('${sdt_date}')-2+3)),'-','')
				and market_source_type_code='1' -- 市调来源类型编码：1-永辉门店,2-网站,3-批发市场,4-一批,5-二批,6-终端
				and shipper_code='YHCSX'
			) a 
			left join 
			(
				select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df 
				where sdt='${sdt_yes}'  -- regexp_replace(date_sub(current_date,1),'-','')
				and shipper_code='YHCSX'
			) b on a.market_goods_id=b.id 
		group by 
			b.location_code,
			b.product_code,
			-- regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n',''), 		
			a.market_research_date
	)a
)a where rum=1
),
-- 标品非标品近1个月动销情况
shidiao_goods as
(
select
	c.flag,
	e.performance_region_code,
	e.performance_region_name,
	e.performance_province_code,
	e.performance_province_name,
	e.performance_city_code,
	e.performance_city_name,
	a.inventory_dc_code as location_code,
	c.business_division_name,
	c.classify_large_code,c.classify_large_name,
	c.classify_middle_code,c.classify_middle_name,
	c.classify_small_code,c.classify_small_name,	
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n','') as regionalized_goods_name, 
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end as is_beihuo_goods,
	c.goods_bar_code,
	sum(a.sale_amt) sale_amt,
	sum(a.sale_qty) sale_qty,
	sum(a.profit)/abs(sum(a.sale_amt)) as profit_rate,
	count(distinct a.customer_code) customer_cnt,
	count(distinct a.sdt) as day_cnt,
	count(a.goods_code) as goods_cnt,
	'${sdt_yes}' as sdt_import
from 
	(
	select sdt,goods_code,customer_code,inventory_dc_code,order_code,sale_amt,sale_qty,profit
	from csx_dws.csx_dws_sale_detail_di
	-- 昨日的上月自然月整月
	where sdt between regexp_replace(trunc(add_months('${sdt_date}',-1),'MM'),'-','') and regexp_replace(last_day(add_months('${sdt_date}',-1)),'-','')
		and channel_code in ('1','7','9')
		and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and delivery_type_code in (1) -- 剔除直送和自提 配送类型编码：1-配送 2-直送 3-自提 4-直通 11-同城配送 12-快递配送 13-一件代发
		and inventory_dc_code in('W0R9','W0A5','W0N0','W0W7','W0X6','W0T1','W0N1','W0AS','W0A8','W0F4','W0L3','WB56','W0AH','W0G9','WA96','WB67','W0K6','W0BK','W0A7','W0X2','W0Z9','W0A6','W0Q2','W0A3','W0Q9','W0P8','W0A2','W0L4','W0BR','W0BH','WB95','WC53','WD57')
	) a 	
	join 
		(
		select *,
			if(((classify_large_name='干货加工' and (classify_middle_name='蛋' or unit_name='KG'))
				or (classify_large_name='肉禽水产' and classify_middle_name!='预制菜')	
				or classify_large_name='蔬菜水果'),'非标品','标品') as flag
		from csx_dim.csx_dim_basic_goods
		where sdt ='current'
		) c on a.goods_code=c.goods_code
	join
		(
		select dc_code,goods_code,shop_special_goods_status,goods_status_name,stock_attribute_code,regionalized_goods_name,stock_attribute_name --1存储 2货到即配
		from csx_dim.csx_dim_basic_dc_goods
		where sdt = 'current'
			and shop_special_goods_status in('0','7') -- 0：B 正常商品；3：H 停售；6：L 退场；7：K 永久停购；
		) d on d.dc_code=a.inventory_dc_code and d.goods_code=a.goods_code	
	left join 
		(
		select *
		from csx_dim.csx_dim_shop 
		where sdt='current'
		) e on e.shop_code=a.inventory_dc_code
group by 
	c.flag,
	e.performance_region_code,
	e.performance_region_name,
	e.performance_province_code,
	e.performance_province_name,
	e.performance_city_code,
	e.performance_city_name,
	a.inventory_dc_code,
	c.business_division_name,
	c.classify_large_code,c.classify_large_name,
	c.classify_middle_code,c.classify_middle_name,
	c.classify_small_code,c.classify_small_name,	
	c.spu_goods_name,
	c.brand_name,
	a.goods_code,
	c.goods_name,
	regexp_replace(d.regionalized_goods_name,'\n|\t|\r|\,|\"|\\\\n',''),
	c.unit_name,
	c.standard,
	d.goods_status_name,
	case when d.stock_attribute_code='1' then '是' else '否' end,
	c.goods_bar_code
having sum(a.sale_amt)>0
)

insert overwrite table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_m_wi partition(swt)
select
	concat_ws('-',a.week_of_year,a.flag,a.location_code,a.goods_code) as biz_id,
	e.performance_region_code,
	e.performance_region_name,
	e.performance_province_code,
	e.performance_province_name,
	e.performance_city_code,
	e.performance_city_name,
	a.flag,
	-- a.province_name,
	-- a.city_name,
	a.location_code,
	a.classify_large_code,a.classify_large_name,
	a.classify_middle_code,a.classify_middle_name,
	a.classify_small_code,a.classify_small_name,	
	a.goods_code,
	a.goods_name,
	a.regionalized_goods_name,
	a.unit_name,
	a.standard,
	cast(a.sale_amt as decimal(20,6)) as sale_amt,
	cast(a.sale_qty as decimal(20,6)) as sale_qty,
	cast(a.profit_rate as decimal(20,6)) as profit_rate,
	cast(a.customer_cnt as decimal(20,6)) as customer_cnt,
	a.day_cnt,
	a.goods_cnt,
	a.sdt_import,
	a.product_code,  -- 原料编码
	a.product_name,  -- 原料名称
	a.product_source,  -- 原料来源	
	c.market_research_price as market_research_price_ty,  -- 市调价格
	c2.market_research_price as market_research_price_ty_bom,  -- 市调价格
	d.market_research_price as market_research_price_yc,
	d2.market_research_price as market_research_price_yc_bom,
	if(coalesce(c.market_research_price,c2.market_research_price) is not null,'是','否') as is_market_research_price_ty,
	if(coalesce(d.market_research_price,d2.market_research_price) is not null,'是','否') as is_market_research_price_yc,
	if(coalesce(c.market_research_price,c2.market_research_price,d.market_research_price,d2.market_research_price) is not null,'是','否') as is_market_research_price_all,
	a.week_range,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	c.shop_code_name as shop_code_name_ty,  -- 通用市调地点_商品
	c2.shop_code_name as shop_code_name_bom,  -- 通用市调地点_bom
	if(f.biz_id_2 is null,'否','是') as is_qd_list,	  -- 是否清单商品
	h.base_product_status_name,
	a.week_of_year as swt
from 
(
	select
		a.*,
		b.product_code,  -- 原料编码
		b.product_name,  -- 原料名称
		b.flag as product_source  -- 原料来源
	from 
	(
	select a.*,
	-- b.csx_week, -- 彩食鲜业务周(上周六开始本周五结束)
	b.week_of_year, -- 年周(自然周)
	b.week_end,
	concat_ws('-',week_begin,week_end) as week_range
	-- from csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df a
	from shidiao_goods a
	-- 导入最大日期
	-- join (select max(sdt_import) sdt_import from csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df )a1 on a.sdt_import=a1.sdt_import	
	join csx_dim.csx_dim_basic_date b on a.sdt_import=b.calday
	-- 跨年的时候这里可能年周出现问题，需要注释掉,用导入最大日期
	where b.week_of_year=concat(substr('${sdt_yes}',1,4),lpad(weekofyear(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),-0)),2,'0'))
	)a
-- 匹配bom看是否有原料码
	left join 
	(
	select *
	from 
	(
		select *,
			row_number() over(partition by location_code,goods_code order by product_num desc) as rum
		from csx_analyse.csx_analyse_fr_ts_bom_man_made_factory_setting_df
	)a where rum=1
	)b on a.location_code=b.location_code and a.goods_code=b.goods_code
)a
-- 是否有通用市调价
left join market_research_ty c on a.location_code=c.location_code and a.goods_code=c.product_code
left join market_research_ty c2 on a.location_code=c2.location_code and a.product_code=c2.product_code
-- 是否有云超价，通用市调-永辉门店
left join market_research_yc d on a.location_code=d.location_code and a.goods_code=d.product_code
left join market_research_yc d2 on a.location_code=d2.location_code and a.product_code=d2.product_code
left join (select * from csx_dim.csx_dim_shop where sdt='current') e on e.shop_code=a.location_code
left join 
(
	select *
	from 
	(
		select 
		concat_ws('-',b.week_of_year,a.location_code,a.goods_code) as biz_id_2,  -- 年周(自然周)
		row_number() over(partition by b.week_of_year,a.location_code,a.goods_code order by a.flag desc) as rum
		from csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df a 
		join csx_dim.csx_dim_basic_date b on a.sdt_import=b.calday
	)a where rum=1
) f on f.biz_id_2=concat_ws('-',a.week_of_year,a.location_code,a.goods_code)
-- 近1月销售情况
-- left join sale_statistics f on a.location_code=f.inventory_dc_code and a.goods_code=f.goods_code
-- left join 
-- (
-- 	select 
-- 		classify_large_code,classify_large_name,
-- 		classify_middle_code,classify_middle_name,
-- 		classify_small_code,classify_small_name,
-- 		goods_code,goods_name,unit_name,brand_name,standard,category_small_name,spu_goods_name,goods_bar_code,business_division_name
-- 	from csx_dim.csx_dim_basic_goods
-- 	where sdt ='current'
-- ) g on a.goods_code=g.goods_code
left join 
(
	select inventory_dc_code,product_code,
	case base_product_status
		when 0 then '正常'
		when 3 then '停售'
		when 6 then '退场'
		when 7 then '停购'
	end as base_product_status_name	 -- 主数据商品状态
	from csx_ods.csx_ods_b2b_mall_prod_yszx_dc_product_pool_df
	where sdt='${sdt_yes}'
	and base_product_tag=1
) h on a.goods_code=h.product_code and a.location_code=h.inventory_dc_code	
;


drop table if exists csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_m_wi;
create table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_m_wi(
`biz_id` STRING  COMMENT '业务主键',
`performance_region_code` STRING  COMMENT '大区编码',
`performance_region_name` STRING  COMMENT '大区名称',
`performance_province_code` STRING  COMMENT '省区编码',
`performance_province_name` STRING  COMMENT '省区名称',
`performance_city_code` STRING  COMMENT '城市编码',
`performance_city_name` STRING  COMMENT '城市名称',
`flag` STRING  COMMENT '类别标品非标品',
`location_code` STRING  COMMENT 'dc编码',
`classify_large_code` STRING  COMMENT '管理大类编号',
`classify_large_name` STRING  COMMENT '管理大类名称',
`classify_middle_code` STRING  COMMENT '管理中类编号',
`classify_middle_name` STRING  COMMENT '管理中类名称',
`classify_small_code` STRING  COMMENT '管理小类编号',
`classify_small_name` STRING  COMMENT '管理小类名称',
`goods_code` STRING  COMMENT '商品编码',
`goods_name` STRING  COMMENT '商品名称',
`regionalized_goods_name` STRING  COMMENT '区域化名称',
`unit_name` STRING  COMMENT '单位',
`standard` STRING  COMMENT '规格',
`sale_amt` DECIMAL (20,6) COMMENT '销售额',
`sale_qty` DECIMAL (20,6) COMMENT '销售数量',
`profit_rate` DECIMAL (20,6) COMMENT '毛利率',
`customer_cnt` DECIMAL (20,6) COMMENT '下单客户数',
`day_cnt` INT  COMMENT '动销天数',
`goods_cnt` INT  COMMENT '下单次数',
`sdt_import` STRING  COMMENT '导入日期',
`product_code` STRING  COMMENT '原料编码',
`product_name` STRING  COMMENT '原料名称',
`product_source` STRING  COMMENT '原料来源',
`market_research_price_ty` STRING  COMMENT '通用市调价格_商品',
`market_research_price_ty_bom` STRING  COMMENT '通用市调价格_bom',
`market_research_price_yc` STRING  COMMENT '云超价格_商品',
`market_research_price_yc_bom` STRING  COMMENT '云超价格_bom',
`is_market_research_price_ty` STRING  COMMENT '是否有通用市调价格',
`is_market_research_price_yc` STRING  COMMENT '是否有云超价格',
`is_market_research_price_all` STRING  COMMENT '是否有市调价格',
`csx_week_range` STRING  COMMENT '周区间',
`update_time` TIMESTAMP  COMMENT '更新时间',
`shop_code_name_ty` STRING  COMMENT '通用市调地点_商品',
`shop_code_name_bom` STRING  COMMENT '通用市调地点_bom',
`is_qd_list` STRING  COMMENT '是否清单商品'
) COMMENT '市调清单与覆盖率-非清单-自然月动销'
PARTITIONED BY (swt string COMMENT '日期分区')
;




-- 查数
select 
	concat_ws('-',swt,flag,location_code,goods_code) abc,
	swt,
	flag,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,		
	goods_code,
	count(1) aa
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
group by 
	concat_ws('-',swt,flag,location_code,goods_code),
	swt,
	flag,
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,		
	goods_code
having aa>1

select *
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where concat_ws('-',swt,flag,location_code,goods_code)=''


select * from csx_analyse.csx_analyse_fr_ts_bom_man_made_factory_setting_df where location_code='W0A2' and goods_code='1028534'

	select location_code,product_code,
		market_research_price,  -- 市调价格
		price_end_time  -- 生效结束时间
	from csx_analyse.csx_analyse_fr_price_market_research_price_detail_df
	where substr(price_end_time,1,10)>=cast(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),-2) as string)  -- 生效结束时间>=T-2
	and (change_type_name<>'手动失效' or change_type_name is NULL)  -- 页面修改价格类型<>手动失效
	and shop_code not in('ZD273','ZD319','ZD41') -- 剔除3个市调地点（ZD273、ZD319、ZD41）
select * from csx_analyse.csx_analyse_fr_price_market_research_price_detail_df where location_code='W0A2' and product_code='1028534'




市调覆盖率-非清单-自然月动销
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E8%25A6%2586%25E7%259B%2596%25E7%258E%2587-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595-%25E8%2587%25AA%25E7%2584%25B6%25E6%259C%2588%25E5%258A%25A8%25E9%2594%2580.cpt&ref_t=design&ref_c=0f82490d-6286-4a62-8470-72d07b1da79c

市调清单明细-非清单-自然月动销
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E6%25B8%2585%25E5%258D%2595%25E6%2598%258E%25E7%25BB%2586-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595-%25E8%2587%25AA%25E7%2584%25B6%25E6%259C%2588%25E5%258A%25A8%25E9%2594%2580.cpt&ref_t=design&ref_c=0f82490d-6286-4a62-8470-72d07b1da79c


