-- ①1、对抵负库存成本调整
-- ①2、采购退货金额差异调整
-- ①3、工厂月末分摊-调整销售
-- ①4、工厂月末分摊-调整跨公司调拨
-- ①5、工厂月末分摊-调整其他
-- ①6、手工调整销售成本
--  7、价量差工厂未使用的商品
--  8、工厂分摊后成本小于0，未分摊金额
--  9、报损
-- ★10、盘盈(盘盈用负数表示，表示减成本）
-- ★11、盘亏
--  12、后台收入
--  13、后台支出
   
-- 20200705 6月增加
-- ①14、采购入库价格补救-调整销售
-- ①15、采购入库价格补救-调整跨公司调拨
-- ①16、采购入库价格补救-调整其他

--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;


--销售订单所在省区渠道
with tmp_sale_order_flag as 
(
select 
	case when channel_name='业务代理' then '大客户' else channel_name end channel_name,
	performance_province_code,performance_province_name,original_order_code,split(id, '&')[0] as credential_no,goods_code,
	sum(sale_amt)sale_amt,
	sum(sale_amt_no_tax)sale_amt_no_tax
from 
	-- csx_dw.dws_sale_r_d_detail
	csx_dws.csx_dws_sale_detail_di
where 
	sdt>=add_months(trunc('${ytd_date}','MM'),-6)
group by 
	case when channel_name='业务代理' then '大客户' else channel_name end,
	performance_province_code,performance_province_name,original_order_code,split(id, '&')[0],goods_code
),

--1 2 3 4 5 6  成本调整 adjustment_amt_no_tax,adjustment_amt
tmp_cbgb_tz_v11 as 
(
select case when a.location_code='W0H4' then '-' else b.performance_province_code end performance_province_code,
	case when a.location_code='W0H4' then '供应链' else b.performance_province_name end performance_province_name,
	case when a.location_code='W0H4' then '-' else b.performance_city_code end performance_city_code,
	case when a.location_code='W0H4' then '供应链' else b.performance_city_name end performance_city_name,
	a.location_code,a.location_name,
	c.channel_name,d.dept_id,d.dept_name,
	sum(adj_ddfkc_no) adj_ddfkc_no,sum(adj_ddfkc) adj_ddfkc,
	sum(adj_cgth_no) adj_cgth_no,sum(adj_cgth) adj_cgth,
	sum(adj_gc_xs_no) adj_gc_xs_no,sum(adj_gc_xs) adj_gc_xs,
	sum(adj_gc_db_no) adj_gc_db_no,sum(adj_gc_db) adj_gc_db,
	sum(adj_gc_qt_no) adj_gc_qt_no,sum(adj_gc_qt) adj_gc_qt,
	sum(adj_sg_no) adj_sg_no,sum(adj_sg) adj_sg,
	sum(adj_bj_xs_no) adj_bj_xs_no,sum(adj_bj_xs) adj_bj_xs,
	sum(adj_bj_db_no) adj_bj_db_no,sum(adj_bj_db) adj_bj_db,
	sum(adj_bj_qt_no) adj_bj_qt_no,sum(adj_bj_qt) adj_bj_qt
from
	(
	select 
		source_credential_no,goods_code,location_code,location_name,
		--对抵负库存的成本调整
		case when adjustment_remark='in_remark' then adjustment_amt_no_tax end adj_ddfkc_no,
		case when adjustment_remark='in_remark' then adjustment_amt end adj_ddfkc,
		--采购退货金额差异的成本调整
		case when adjustment_remark='out_remark' then adjustment_amt_no_tax end adj_cgth_no,
		case when adjustment_remark='out_remark' then adjustment_amt end adj_cgth,
		--工厂月末分摊-调整销售订单
		case when (adjustment_remark in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and wms_biz_type_code in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
				then adjustment_amt_no_tax end adj_gc_xs_no,
		case when (adjustment_remark in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and wms_biz_type_code in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
				then adjustment_amt end adj_gc_xs,		
		--工厂月末分摊-调整跨公司调拨订单
		case when (adjustment_remark in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and wms_biz_type_code in('06','07','08','09','12','15','17') )
				then adjustment_amt_no_tax end adj_gc_db_no,
		case when (adjustment_remark in('fac_remark_sale','fac_remark_span') 
					and adjustment_type='sale'
					and wms_biz_type_code in('06','07','08','09','12','15','17') )
				then adjustment_amt end adj_gc_db,		
		--工厂月末分摊-调整其他
		case when adjustment_remark in('fac_remark_sale','fac_remark_span')		
				and adjustment_type='sale'
				and wms_biz_type_code not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_gc_qt_no,
		case when adjustment_remark in('fac_remark_sale','fac_remark_span')  
				and adjustment_type='sale'
				and wms_biz_type_code not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt end adj_gc_qt,		
		--手工调整销售成本
		case when adjustment_remark='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt_no_tax,adjustment_amt_no_tax) end adj_sg_no,
		case when adjustment_remark='manual_remark' then if(adjustment_type='stock',-1*adjustment_amt,adjustment_amt) end adj_sg,
		--采购入库价格补救-调整销售
		case when adjustment_remark = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and wms_biz_type_code in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
				then adjustment_amt_no_tax end adj_bj_xs_no,
		case when adjustment_remark = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and wms_biz_type_code in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82')
				then adjustment_amt end adj_bj_xs,				
		--采购入库价格补救-调整跨公司调拨	
		case when adjustment_remark = 'pur_remark_remedy'
				and adjustment_type='sale'
				and wms_biz_type_code in ('06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_bj_db_no,
		case when adjustment_remark = 'pur_remark_remedy'
				and adjustment_type='sale'
				and wms_biz_type_code in ('06','07','08','09','12','15','17')
				then adjustment_amt end adj_bj_db,				
		--采购入库价格补救-调整其他
		case when adjustment_remark = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and wms_biz_type_code not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt_no_tax end adj_bj_qt_no,	
		case when adjustment_remark = 'pur_remark_remedy' 
				and adjustment_type='sale'
				and wms_biz_type_code not in ('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
				then adjustment_amt end adj_bj_qt
	from 
		(
		select 
			* 
		from 
			-- csx_dw.dws_cas_r_d_account_adjustment_detail
			csx_dws.csx_dws_cas_adjustment_detail_view_di
		where 
			sdt>=regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-','') and sdt<regexp_replace(trunc('${ytd_date}','MM'),'-','')		
		)a
	)a
	left join tmp_sale_order_flag c on a.source_credential_no=c.credential_no and a.goods_code=c.goods_code
	join 
		(
		select 
			shop_code,shop_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current'
			and purpose<>'09'
		) b on b.shop_code=a.location_code
	left join 
		(
		select 
			regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') goods_name,
			goods_code,purchase_group_code dept_id,purchase_group_name dept_name
		from 
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current' 
		) d on a.goods_code=d.goods_code
group by 
	case when a.location_code='W0H4' then '-' else b.performance_province_code end,
	case when a.location_code='W0H4' then '供应链' else b.performance_province_name end,
	case when a.location_code='W0H4' then '-' else b.performance_city_code end,
	case when a.location_code='W0H4' then '供应链' else b.performance_city_name end,
	a.location_code,a.location_name,
	c.channel_name,d.dept_id,d.dept_name
)

insert overwrite table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_xstz_mi partition(sdt)

select
	concat_ws('&',performance_city_code,location_code,channel_name,substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6)) as biz_id,
	performance_province_code,performance_province_name,performance_city_code,performance_city_name,
	location_code,location_name,channel_name,dept_id,dept_name,
	adj_ddfkc_no,adj_ddfkc,
	adj_cgth_no,adj_cgth,
	adj_gc_xs_no,adj_gc_xs,
	adj_gc_db_no,adj_gc_db,
	adj_gc_qt_no,adj_gc_qt,
	adj_sg_no,adj_sg,
	adj_bj_xs_no,adj_bj_xs,
	adj_bj_db_no,adj_bj_db,
	adj_bj_qt_no,adj_bj_qt,
	substr(regexp_replace(add_months(trunc('${ytd_date}','MM'),-1),'-',''),1,6) as sdt
from tmp_cbgb_tz_v11;


/*
create table csx_analyse.csx_analyse_report_cbgb_tz_m_cbgb_xstz_mi(
`biz_id`                         string              COMMENT    '业务唯一id',
`performance_province_code`      string              COMMENT    '省区编码',
`performance_province_name`      string              COMMENT    '省区名称',
`performance_city_code`          string              COMMENT    '城市组',
`performance_city_name`          string              COMMENT    '城市组名称',
`location_code`                  string              COMMENT    '地点编码',
`location_name`                  string              COMMENT    '地点名称',
`channel_name`                   string              COMMENT    '渠道名称',
`dept_id`                        string              COMMENT    '课组编号',
`dept_name`                      string              COMMENT    '课组名称',
`adj_ddfkc_no`                   decimal(26,6)       COMMENT    '对抵负库存的成本调整不含税',
`adj_ddfkc`                      decimal(26,6)       COMMENT    '对抵负库存的成本调整含税',
`adj_cgth_no`                    decimal(26,6)       COMMENT    '采购退货金额差异的成本调整不含税',
`adj_cgth`                       decimal(26,6)       COMMENT    '采购退货金额差异的成本调整含税',
`adj_gc_xs_no`                   decimal(26,6)       COMMENT    '调整销售订单不含税',
`adj_gc_xs`                      decimal(26,6)       COMMENT    '调整销售订单含税',
`adj_gc_db_no`                   decimal(26,6)       COMMENT    '调整跨公司调拨订单不含税',
`adj_gc_db`                      decimal(26,6)       COMMENT    '调整跨公司调拨订单含税',
`adj_gc_qt_no`                   decimal(26,6)       COMMENT    '调整其他不含税',
`adj_gc_qt`                      decimal(26,6)       COMMENT    '调整其他含税',
`adj_sg_no`                      decimal(26,6)       COMMENT    '手工调整销售成本不含税',
`adj_sg`                         decimal(26,6)       COMMENT    '手工调整销售成本含税',
`adj_bj_xs_no`                   decimal(26,6)       COMMENT    '采购入库价格补救-调整销售不含税',
`adj_bj_xs`                      decimal(26,6)       COMMENT    '采购入库价格补救-调整销售含税',
`adj_bj_db_no`                   decimal(26,6)       COMMENT    '采购入库价格补救-调整跨公司调拨不含税',
`adj_bj_db`                      decimal(26,6)       COMMENT    '采购入库价格补救-调整跨公司调拨含税',
`adj_bj_qt_no`                   decimal(26,6)       COMMENT    '采购入库价格补救-调整其他不含税',
`adj_bj_qt`                      decimal(26,6)       COMMENT    '采购入库价格补救-调整其他含税'

) COMMENT '财报管报-调整项-销售调整'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS PARQUET;

*/	