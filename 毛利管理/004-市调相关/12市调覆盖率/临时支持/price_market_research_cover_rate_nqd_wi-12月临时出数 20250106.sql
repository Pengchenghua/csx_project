	select distinct
		csx_week, -- 彩食鲜业务周(上周六开始本周五结束)
		concat_ws('-',csx_week_begin,csx_week_end) as csx_week_range		
	from csx_dim.csx_dim_basic_date
	where calday>='20240729' 
	and calday<=regexp_replace(cast(to_date(date_sub(now(),1)) as string),'-','')
	order by csx_week
	
	


import_table_price_market_research_list
-- drop table if exists import_table_price_market_research_list;
CREATE TABLE `import_table_price_market_research_list` (
`id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '主键',
`flag` varchar(32) DEFAULT NULL COMMENT '类别',
`province_name` varchar(32) DEFAULT NULL COMMENT '省区',
`city_name` varchar(32) DEFAULT NULL COMMENT '城市',
`location_code` varchar(32) DEFAULT NULL COMMENT 'DC编码',
`classify_large_name` varchar(32) DEFAULT NULL COMMENT '一级分类',
`classify_middle_name` varchar(32) DEFAULT NULL COMMENT '二级分类',
`goods_code` varchar(32) DEFAULT NULL COMMENT '商品编码',
`goods_name` varchar(256) DEFAULT NULL COMMENT '商品名称',
`regionalized_goods_name` varchar(256) DEFAULT NULL COMMENT '区域化名称',
`unit_name` varchar(32) DEFAULT NULL COMMENT '单位',
`standard` varchar(64) DEFAULT NULL COMMENT '规格',
`sale_amt` decimal(20,6) DEFAULT NULL COMMENT '销售额',
`sale_qty` decimal(20,6)  DEFAULT NULL COMMENT '销售数量',
`profit_rate` decimal(20,6)  DEFAULT NULL COMMENT '毛利率',
`customer_cnt` decimal(20,6)  DEFAULT NULL COMMENT '下单客户数',
`day_cnt` int(4) DEFAULT NULL COMMENT '动销天数',
`goods_cnt` int(4) DEFAULT NULL COMMENT '下单次数',
`sdt_import` varchar(32) DEFAULT NULL COMMENT '导入日期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 COMMENT='导入表-市调清单-标品非标品';

create table csx_ods.csx_ods_data_analysis_prd_import_table_price_market_research_list_df(
`id` bigint COMMENT '主键',
`flag` string COMMENT '类别',
`province_name` string COMMENT '省区',
`city_name` string COMMENT '城市',
`location_code` string COMMENT 'DC编码',
`classify_large_name` string COMMENT '一级分类',
`classify_middle_name` string COMMENT '二级分类',
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
`sdt_import` string COMMENT '导入日期'
) COMMENT '导入表-市调清单-标品非标品'
;




-- ds1
select 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,	
	flag,	
	count(1) as count_sku,
	count(case when is_market_research_price_all='是' then goods_code end) as sd_count_sku,
	count(case when is_market_research_price_ty='是' then goods_code end) as sd_count_sku_ty,
	count(case when is_market_research_price_yc='是' then goods_code end) as sd_count_sku_yc,
	
	sum(sale_amt)/10000 as sale_amt,
	sum(case when is_market_research_price_all='是' then sale_amt end)/10000 as sd_sale_amt,
	sum(case when is_market_research_price_ty='是' then sale_amt end)/10000 as sd_sale_amt_ty,
	sum(case when is_market_research_price_yc='是' then sale_amt end)/10000 as sd_sale_amt_yc
from csx_analyse_tmp.tmp_1
where 1=1
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
group by 
	performance_region_name,
	performance_province_name,
	performance_city_name,
	location_code,	
	flag
	
-- 明细	
select *
from csx_analyse_tmp.tmp_1
where 1=1
${if(len(sq)==0,"","and performance_province_name in('"+replace(sq,",","','")+"') ")}
${if(len(flag)==0,"","and flag in( '"+flag+"')")}
${if(len(is_market_research_price_all)==0,"","and is_market_research_price_all in( '"+is_market_research_price_all+"')")}


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
	


-- 通用市调临时表
drop table csx_analyse_tmp.csx_analyse_fr_price_market_research_price_detail_df;
create  table csx_analyse_tmp.csx_analyse_fr_price_market_research_price_detail_df
as
select 
	a.performance_province_name,a.performance_city_name,a.location_code,a.market_source_type_name,a.shop_code,a.shop_name,a.product_code,a.product_name,a.market_research_price,
	a.min_price,max_price,a.price_begin_time,a.price_end_time,a.create_date,a.create_by,a.remark,a.unit_name,a.estimated_pricing_gross_margin,
	a.one_product_category_code,a.one_product_category_name,a.two_product_category_code,a.two_product_category_name,a.update_time,
	a.source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
	a.product_status, -- 商品状态:1促销 0正常
	a.bom, -- bom配置
	a.bom_type,  -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)

	from_utc_timestamp(current_timestamp(),'GMT') update_time_b,

	a.link_addr,  -- 链接地址
	a.status,  -- 状态(1-有效 0-无效)
	a.update_by,
	a.change_type_name		-- 页面修改价格类型		
from 
(
	select
		performance_province_name,performance_city_name,location_code,market_source_type_name,shop_code,shop_name,product_code,product_name,price as market_research_price,
		min_price,max_price,price_begin_time,price_end_time,create_date,a.create_by,a.remark,a.unit_name,a.estimated_pricing_gross_margin,
		a.one_product_category_code,a.one_product_category_name,a.two_product_category_code,a.two_product_category_name,a.update_time,
		a.source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
		a.product_status, -- 商品状态:1促销 0正常
		a.bom, -- bom配置
		a.bom_type,  -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)
		a.link_addr,  -- 链接地址
		case a.status when 0 then '无效' when 1 then '有效' end as status,  -- 状态(1-有效 0-无效)
		a.update_by,
		a.change_type_name		-- 页面修改价格			
	from
		(
		select 
			c.performance_province_name,
			c.performance_city_name,
			b.location_code,
			(case when a.source_type_code=2 then '网站' 
				when a.source_type_code=3 then '批发市场' 
				when a.source_type_code=4 then '一批' 
				when a.source_type_code=5 then '二批' 
				when a.source_type_code=6 then '终端' end) as market_source_type_name,
			a.shop_code,
			--a.shop_name,
			regexp_replace(a.shop_name,'\n|\t|\r|\,|\"|\\\\n','') as shop_name, 
			b.product_code,
			--b.product_name,
			regexp_replace(b.product_name,'\n|\t|\r|\,|\"|\\\\n','') as product_name, 
			a.price,
			a.min_price,
			a.max_price,
			a.price_begin_time,
			a.price_end_time,
			to_date(a.create_time) as create_date,
			row_number()over(partition by b.location_code,b.product_code,a.shop_code order by a.create_time desc) as rn,
			a.create_by,
			a.remark,
			d.unit_name,
			a.estimated_pricing_gross_margin,
			b.one_product_category_code,
			b.one_product_category_name,
			b.two_product_category_code,
			b.two_product_category_name,
			a.update_time,
			a.source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
			a.product_status, -- 商品状态:1促销 0正常
			a.bom, -- bom配置
			a.bom_type, -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)	
			a.link_addr,  -- 链接地址
			a.status,  -- 状态(1-有效 0-无效)
			a1.update_by,
			a1.change_type_name		-- 页面修改价格			
		from 
			(
			select 
				* 
			from 
				(
				select
					case 
					when source=0 then '市调导入'
					when source=1 then '小程序'
					when source=2 then '小程序-pc端'
					when source=3 then '线上网站市调'
					when source=4 then '通用市调系统生成'
					when source=5 then '市调图片导入'
					when source=6 then '系统自动爬取'					
					else source end as source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
					case product_status when 0 then '正常' when 1 then '促销' end as product_status, -- 商品状态:1促销 0正常
					bom, -- bom配置
					case 
					when bom_type=1 then '人工bom'
					when bom_type=2 then '报价策略人工bom'
					when bom_type=3 then '工厂bom'
					when bom_type=4 then '报价策略工厂bom'
					else bom_type end as bom_type, -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)				
					source_type_code,shop_code,shop_name,price,min_price,max_price,price_begin_time,price_end_time,create_time,create_by,remark,
					estimated_pricing_gross_margin,product_id,update_time,
					link_addr,  -- 链接地址
					status  -- 状态(1-有效 0-无效)
				from csx_ods.csx_ods_csx_price_prod_market_research_not_yh_price_effective_df  -- 非永辉 生效
				where regexp_replace(split(create_time,' ')[0],'-','')>='20240101'
				and regexp_replace(split(create_time,' ')[0],'-','')<='20241231'
				and source_type_code!=1
				and shipper_code='YHCSX'
				-- from csx_dwd.csx_dwd_price_market_research_not_yh_price_effective_di -- 非永辉 生效
				-- and source_type_code!=1	
					
				union all
				select
					case 
					when source=0 then '市调导入'
					when source=1 then '小程序'
					when source=2 then '小程序-pc端'
					when source=3 then '线上网站市调'
					when source=4 then '通用市调系统生成'
					when source=5 then '市调图片导入'
					when source=6 then '系统自动爬取'					
					else source end as source, -- 来源: 0:市调导入；1:小程序;2:小程序-pc端 3: 线上网站市调
					case product_status when 0 then '正常' when 1 then '促销' end as product_status, -- 商品状态:1促销 0正常
					bom, -- bom配置
					case 
					when bom_type=1 then '人工bom'
					when bom_type=2 then '报价策略人工bom'
					when bom_type=3 then '工厂bom'
					when bom_type=4 then '报价策略工厂bom'
					else bom_type end as bom_type, -- bom类型(1:人工bom;2:报价策略人工bom;3:工厂bom;4:报价策略工厂bom)					
					source_type_code,shop_code,shop_name,price,min_price,max_price,price_begin_time,price_end_time,create_time,create_by,remark,
					estimated_pricing_gross_margin,product_id,update_time,
					link_addr,  -- 链接地址
					status  -- 状态(1-有效 0-无效)
				from csx_dwd.csx_dwd_market_research_not_yh_price_di -- 非永辉 失效
				where sdt>='20240101'
				and sdt<='20241231'
				and source_type_code!=1	
				and shipper_code='YHCSX'
				) tmp
			) a 
			left join (select * from csx_ods.csx_ods_csx_price_prod_market_research_product_df where sdt='${sdt_yes}') b on a.product_id=b.id 
			left join (select * from csx_dim.csx_dim_shop where sdt='current') c on c.shop_code=b.location_code
			left join (select * from csx_dim.csx_dim_basic_goods where sdt='current')d on d.goods_code=b.product_code
			-- 市调价格更新日志表--价格失效原因
			left join 
			(
				select *
				from 
				(
				select 
					shop_code,product_id,
					update_by,update_time,
					case 
						when change_type=1 then '页面修改价格'
						when change_type=2 then '导入修改价格'
						when change_type=3 then '导入修改时间'
						when change_type=4 then '导入失效'  --有冲突截断时间后失效旧数据生效新数据
						when change_type=5 then '手动失效'
						when change_type=6 then '导入引用%s客户市调'
						when change_type=7 then '添加商品'
						when change_type=8 then '添加修改价格'
						when change_type=9 then '添加修改时间'
						when change_type=10 then '添加失效'
						when change_type=11 then '添加引用%s客户市调'
						when change_type=12 then '冲突'	
						else change_type end as change_type_name		-- 页面修改价格		
					-- row_number()over(partition by shop_code,product_id order by update_time desc) as rno
				from csx_ods.csx_ods_csx_price_prod_market_research_price_log_df  
				where sdt='${sdt_yes}' 
				-- and shipper_code='YHCSX'
				)a	
				-- where rno=1			
			) a1 on a.shop_code=a1.shop_code and a.product_id=a1.product_id and substr(a.update_time,1,16)=substr(a1.update_time,1,16) and status=0
			
		) a 
	where rn=1
)a 
;




drop table csx_analyse_tmp.tmp_1;
create table csx_analyse_tmp.tmp_1 as
with
-- 通用市调价
market_research_ty as
(
	select location_code,product_code,
	concat_ws(',',collect_list(concat(shop_code,shop_name))) as shop_code_name,
	concat_ws(',',collect_list(concat(shop_code,shop_name,round(market_research_price,2)))) as market_research_price  -- 市调价格
		-- price_end_time,  -- 生效结束时间
		-- row_number() over(partition by location_code,product_code order by create_date desc) as rum
	from csx_analyse_tmp.csx_analyse_fr_price_market_research_price_detail_df
	-- where substr(price_end_time,1,10)>=cast(date_sub(to_date(from_unixtime(unix_timestamp('${sdt_yes}','yyyyMMdd'))),2) as string)  -- 生效结束时间>=T-2
	where regexp_replace(substr(price_end_time,1,10),'-','')>='20241201'
	and regexp_replace(substr(price_begin_time,1,10),'-','')<='20241231'
	and (change_type_name<>'手动失效' or change_type_name is NULL)  -- 页面修改价格类型<>手动失效
	and shop_code not in('ZD273','ZD319','ZD41') -- 剔除3个市调地点（ZD273、ZD319、ZD41）
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
				where sdt between '20241201' and '20241231'
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
	-- where sdt between regexp_replace(add_months(current_date,-1),'-','') and regexp_replace(date_sub(current_date,1),'-','')   -- hive
	where sdt between '20241101' and '20241130'
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

-- insert overwrite table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi partition(swt)
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
;


drop table if exists csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi;
create table csx_analyse.csx_analyse_price_market_research_cover_rate_nqd_wi(
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
`market_research_price_ty` string COMMENT '通用市调价格_商品',
`market_research_price_ty_bom` string COMMENT '通用市调价格_bom',
`market_research_price_yc` string COMMENT '云超价格_商品',
`market_research_price_yc_bom` string COMMENT '云超价格_bom',
`is_market_research_price_ty` string COMMENT '是否有通用市调价格',
`is_market_research_price_yc` string COMMENT '是否有云超价格',
`is_market_research_price_all` string COMMENT '是否有市调价格',
`csx_week_range` string COMMENT '周区间',
`update_time`	timestamp	COMMENT    '更新时间'
) COMMENT '市调清单与覆盖率-非清单'
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



csx_ods_csx_b2b_oms_agreement_order_di(履约订单)




市调清单明细
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E6%25B8%2585%25E5%258D%2595%25E6%2598%258E%25E7%25BB%2586.cpt&ref_t=design&ref_c=8adff7dd-0a77-4918-9810-d2c99758d52c


市调清单明细-非清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E6%25B8%2585%25E5%258D%2595%25E6%2598%258E%25E7%25BB%2586-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7

市调覆盖率-非清单
http://fr.csxdata.cn/webroot/decision/view/report?viewlet=%25E5%2585%25B1%25E4%25BA%25AB%252F%25E5%25B8%2582%25E8%25B0%2583%252F%25E5%25B8%2582%25E8%25B0%2583%25E8%25A6%2586%25E7%259B%2596%25E7%258E%2587-%25E9%259D%259E%25E6%25B8%2585%25E5%258D%2595.cpt&ref_t=design&ref_c=a7d05a63-1ec9-4346-951b-86fa3a3d18b7








