	select distinct
		csx_week, -- 彩食鲜业务周(上周六开始本周五结束)
		concat_ws('-',csx_week_begin,csx_week_end) as csx_week_range		
	from csx_dim.csx_dim_basic_date
	where calday>='20240729' 
	and calday<=regexp_replace(cast(to_date(date_sub(now(),1)) as string),'-','')
	order by csx_week
	
	




-- ds1
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,
	classify_large_name,
	classify_middle_name,	
	flag,	
	swt,
	count(1) as count_sku,
	count(case when is_market_research_price_all='是' then goods_code end) as sd_count_sku,
	
	sum(sale_amt)/10000 as sale_amt,
	sum(case when is_market_research_price_all='是' then sale_amt end)/10000 as sd_sale_amt	
-- from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_shop_wi
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where swt='${EDATE}'
and coalesce(base_product_status_name,'正常') in('正常','','B 正常商品')
${if(len(sq)==0,"","and performance_city_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,	
	classify_large_name,
	classify_middle_name,	
	flag,		
	swt
	
-- ds2
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,
	classify_large_name,
	classify_middle_name,	
	flag,
	market_flag,	
	shop_code_name,   -- 市调地点编码名称
	is_market_research_price_online,  -- 是否线上市调
	swt,
	count(goods_code) as sd_count_sku_shop,
	sum(sale_amt)/10000 as sd_sale_amt_shop
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_shop_wi
-- from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where swt='${EDATE}'
and coalesce(base_product_status_name,'正常') in('正常','','B 正常商品')
and market_flag<>''
${if(len(sq)==0,"","and performance_city_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,	
	classify_large_name,
	classify_middle_name,	
	flag,	
	market_flag,
	shop_code_name,   -- 市调地点编码名称
	is_market_research_price_online,  -- 是否线上市调		
	swt
order by shop_code_name






	
-- 明细	
select *
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_shop_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
${if(len(is_market_research_price)==0,"","and is_market_research_price in( '"+is_market_research_price+"')")}


-- 周趋势 如何定位到近8周 更新时间近2个月？
select 	
	swt as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where update_time>date_sub(now(),60)
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt


substr(week_range,10,8)

regexp_replace(cast(to_date(add_months(now(),-1)) as string),'-','')




-- 周趋势
select 	
	swt as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt

union all	
select 	
	'202430' as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt

union all	
select 	
	'202429' as `周`,
	count(1) as `需市调数-sku`,
	count(case when is_market_research_price_all='是' then goods_code end)/count(1) as `市调覆盖率-sku`,
	count(case when is_market_research_price_ty='是' then goods_code end)/count(1) as `通用覆盖率-sku`,
	count(case when is_market_research_price_yc='是' then goods_code end)/count(1) as `云超覆盖率-sku`,
	
	sum(sale_amt) as `需市调数-销售额`,
	sum(case when is_market_research_price_all='是' then sale_amt end)/sum(sale_amt) as `市调覆盖率-销售额`,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/sum(sale_amt) as `通用覆盖率-销售额`,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/sum(sale_amt) as `云超覆盖率-销售额`
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where swt='${EDATE}'
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by swt
	


【新增市调覆盖率报表开发】
1、单个省区的品类各市调地点的覆盖率看板与明细报表
-- 总的需市调的SKU 总计sku覆盖率&金额覆盖率、横向各市调地点的SKU覆盖率&金额覆盖率、横向各市调地点的SKU覆盖率 
-- 对应明细数据


-- 增大内存容量
set hive.tez.container.size = 8192;
set tez.am.resource.memory.mb=4096;

-- drop table csx_analyse_tmp.csx_analyse_price_market_research_cover_rate_nqd_shop_wi;
-- create table csx_analyse_tmp.csx_analyse_price_market_research_cover_rate_nqd_shop_wi as
with
-- 通用市调价
market_research_ty as
(
	select location_code,product_code,
	-- if(sum(if(substr(shop_code,2,1)='W',1,0))>0,'是','否') as is_ty_online,  --线上通用市调
	-- if(sum(if(substr(shop_code,2,1)<>'W',1,0))>0,'是','否') as is_ty_offline,  --线下通用市调
	if(substr(shop_code,2,1)='W','是','否') as is_ty_online,  --线上通用市调
	shop_code,shop_name,
	concat(shop_code,'_',shop_name) as shop_code_name,
	market_research_price  -- 市调价格
		-- price_end_time,  -- 生效结束时间
		-- row_number() over(partition by location_code,product_code order by create_date desc) as rum
	from csx_analyse.csx_analyse_fr_price_market_research_price_detail_df
	where substr(price_end_time,1,10)>=cast(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),2) as string)  -- 生效结束时间>=T-2
	and (change_type_name<>'手动失效' or change_type_name is NULL)  -- 页面修改价格类型<>手动失效
	and shop_code not in('ZD273','ZD319','ZD41','ZD527') -- 剔除4个市调地点（ZD273、ZD319、ZD41、ZD527）	
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
				where sdt>=regexp_replace(date_sub('${sdt_date}',if(dayofweek('${sdt_date}')=1,9,dayofweek('${sdt_date}')-2+3)),'-','') -- 上周五
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

-- 通用市调价+云超价
market_research_ty_yc as
(
select location_code,product_code,
	'通用' as market_flag,
	is_ty_online,  --线上通用市调
	shop_code,
	shop_name,
	shop_code_name,
	market_research_price  -- 市调价格
from market_research_ty

union 
select location_code,product_code,
	'云超' as  market_flag,
	'' as is_ty_online,  --线上通用市调
	'云超' as shop_code,
	'云超' as shop_name,
	'云超' as shop_code_name,
	market_research_price  -- 市调价格
from market_research_yc
),
-- 非清单商品明细表
price_market_research_cover_rate_nqd_wi as
(
select
	performance_region_code,
	performance_region_name,
	performance_province_code,
	performance_province_name,
	performance_city_code,
	performance_city_name,
	flag,
	location_code,
	classify_large_code,classify_large_name,
	classify_middle_code,classify_middle_name,
	classify_small_code,classify_small_name,	
	goods_code,
	goods_name,
	regionalized_goods_name,
	unit_name,
	standard,
	sale_amt,
	sale_qty,
	profit_rate,
	customer_cnt,
	day_cnt,
	goods_cnt,
	sdt_import,
	product_code,  -- 原料编码
	product_name,  -- 原料名称
	product_source,  -- 原料来源	
	csx_week_range as week_range,
	is_qd_list,	  -- 是否清单商品
	base_product_status_name,
	update_time,
	swt	as week_of_year
from csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi
where sdt_import='${sdt_yes}'
),

-- 非清单商品明细表+所有市调地点
price_market_research_cover_rate_nqd_wi_shop1 as
(
select a.*,
b.market_flag,
b.is_ty_online,
b.shop_code,
b.shop_name,
b.shop_code_name
from price_market_research_cover_rate_nqd_wi a
join
(
	select distinct market_flag,is_ty_online,shop_code,shop_name,shop_code_name
	from market_research_ty_yc
) b on 1 = 1
),
-- 市调非清单商品明细-商品关联
price_market_research_cover_rate_nqd_wi_shop2 as
(
select a.*,
c.market_research_price
from price_market_research_cover_rate_nqd_wi_shop1 a
-- 是否有通用市调价+云超价
left join market_research_ty_yc c on a.location_code=c.location_code and a.goods_code=c.product_code and a.shop_code=c.shop_code
),

-- 市调非清单商品明细-原料关联
price_market_research_cover_rate_nqd_wi_shop3 as
(
select
	concat_ws('-',a.week_of_year,a.flag,a.location_code,coalesce(a.shop_code,c2.shop_code),a.goods_code) as biz_id,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
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
	a.market_flag,  --市调类型 通用/云超
	if(a.is_ty_online='是','是',if(a.market_flag='云超','','否')) as is_market_research_price_online,	
	a.market_research_price as market_research_price_goods,  -- 市调价格
	c2.market_research_price as market_research_price_bom,  -- 市调价格
	if(coalesce(a.market_research_price,c2.market_research_price) is not null,'是','否') as is_market_research_price,
	a.week_range,
	a.shop_code,
	a.shop_name,
	a.shop_code_name,  -- 市调地点
	-- c2.shop_code_name as shop_code_name_bom,  -- 市调地点_bom
	a.is_qd_list,	  -- 是否清单商品
	a.base_product_status_name,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	a.week_of_year as swt
from price_market_research_cover_rate_nqd_wi_shop2 a
left join market_research_ty_yc c2 on a.location_code=c2.location_code and a.product_code=c2.product_code and a.shop_code=c2.shop_code
)

insert overwrite table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_shop_wi partition(swt)
select *
from price_market_research_cover_rate_nqd_wi_shop3
where is_market_research_price='是'

union all 
select
	concat_ws('-',a.swt,a.flag,a.location_code,a.is_market_research_price,a.goods_code) as biz_id,
	a.performance_region_code,
	a.performance_region_name,
	a.performance_province_code,
	a.performance_province_name,
	a.performance_city_code,
	a.performance_city_name,
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
	a.sale_amt,
	a.sale_qty,
	a.profit_rate,
	a.customer_cnt,
	a.day_cnt,
	a.goods_cnt,
	a.sdt_import,
	a.product_code,  -- 原料编码
	a.product_name,  -- 原料名称
	a.product_source,  -- 原料来源	
	'' as market_flag,  --市调类型 通用/云超
	'' as is_market_research_price_online,	
	null as market_research_price_goods,  -- 市调价格
	null as market_research_price_bom,  -- 市调价格
	'' as is_market_research_price,
	a.week_range,
	'' as shop_code,
	'' as shop_name,
	'' as shop_code_name,  -- 市调地点
	-- c2.shop_code_name as shop_code_name_bom,  -- 市调地点_bom
	a.is_qd_list,	  -- 是否清单商品
	a.base_product_status_name,
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	a.swt
from 
(
select *,
row_number() over(partition by location_code,goods_code order by shop_code desc) as rum
from price_market_research_cover_rate_nqd_wi_shop3
where is_market_research_price='否'
)a where rum=1
;

-- left join (select * from csx_dim.csx_dim_shop where sdt='current') e on e.shop_code=a.location_code
-- left join 
-- (
-- 	select *
-- 	from 
-- 	(
-- 		select 
-- 		concat_ws('-',b.week_of_year,a.location_code,a.goods_code) as biz_id_2,  -- 年周(自然周)
-- 		row_number() over(partition by b.week_of_year,a.location_code,a.goods_code order by a.flag desc) as rum
-- 		from csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df a 
-- 		join csx_dim.csx_dim_basic_date b on a.sdt_import=b.calday
-- 	)a where rum=1
-- ) f on f.biz_id_2=concat_ws('-',a.week_of_year,a.location_code,a.goods_code)
-- left join 
-- (
-- 	select dc_code,goods_code,goods_status_name,
--  	goods_status_name as base_product_status_name	 -- 商品状态
-- from csx_dim.csx_dim_basic_dc_goods 
-- where sdt='current' 
-- ) h on a.goods_code=h.goods_code and a.location_code=h.dc_code 	




	
drop table if exists csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_shop_wi;
create table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_shop_wi(
`biz_id`	string	COMMENT	'业务主键',
`performance_region_code` string COMMENT '大区编码',
`performance_region_name` string COMMENT '大区名称',
`performance_province_code` string COMMENT '省区编码',
`performance_province_name` string COMMENT '省区名称',
`performance_city_code` string COMMENT '城市编码',
`performance_city_name` string COMMENT '城市名称',
`flag` string COMMENT '类别标品非标品',
`location_code` string COMMENT 'DC编码',
-- `classify_large_name_import` string COMMENT '一级分类',
-- `classify_middle_name_import` string COMMENT '二级分类',
`classify_large_code` string COMMENT '管理大类编号',
`classify_large_name` string COMMENT '管理大类名称',
`classify_middle_code` string COMMENT '管理中类编号',
`classify_middle_name` string COMMENT '管理中类名称',
`classify_small_code` string COMMENT '管理小类编号',
`classify_small_name` string COMMENT '管理小类名称',
`goods_code` string COMMENT '商品编码',
`goods_name` string COMMENT '商品名称',
`regionalized_goods_name` string COMMENT '区域化名称',
`unit_name` string COMMENT '单位',
`standard` string COMMENT '规格',
`sale_amt` decimal(20,6) COMMENT '销售额',
`sale_qty` decimal(20,6) COMMENT '销售数量',
`profit_rate` decimal(20,6) COMMENT '毛利率',
`customer_cnt` decimal(20,6) COMMENT '下单客户数',
`day_cnt` int COMMENT '动销天数',
`goods_cnt` int COMMENT '下单次数',
`sdt_import` string COMMENT '导入日期',
`product_code` string COMMENT '原料编码',
`product_name` string COMMENT '原料名称',
`product_source` string COMMENT '原料来源',
`market_flag` string COMMENT '市调类型',
`is_market_research_price_online` string COMMENT '是否线上市调',
`market_research_price_goods` string COMMENT '市调价格_商品',
`market_research_price_bom` string COMMENT '市调价格_bom',
`is_market_research_price` string COMMENT '是否有市调价格',
`week_range` string COMMENT '周区间',
`shop_code` string COMMENT '市调地点编码',
`shop_name` string COMMENT '市调地点',
`shop_code_name` string COMMENT '市调地点编码名称',
`is_qd_list` string COMMENT '是否清单商品',
`base_product_status_name` string COMMENT '主数据商品状态',
`update_time`	timestamp	COMMENT    '更新时间'
) COMMENT '市调清单与覆盖率明细-市调地点-非清单'
PARTITIONED BY (swt string COMMENT '日期分区')
;


市调覆盖率-非清单（市调地点）
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E8%25A6%2586%25E7%259B%2596%25E7%258E%2587-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595-%25E5%259C%25B0%25E7%2582%25B9.cpt&ref_t=design&ref_c=e6ac11f7-5d9a-4f2f-9bfe-4eb6d0733d12

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



csx_ods_csx_b2b_oms_agreement_order_di(履约订单)




市调清单明细
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E6%25B8%2585%25E5%258D%2595%25E6%2598%258E%25E7%25BB%2586.cpt&ref_t=design&ref_c=8adff7dd-0a77-4918-9810-d2c99758d52c


市调清单明细-非清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E6%25B8%2585%25E5%258D%2595%25E6%2598%258E%25E7%25BB%2586-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7

市调覆盖率-非清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E8%25A6%2586%25E7%259B%2596%25E7%258E%2587-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7








