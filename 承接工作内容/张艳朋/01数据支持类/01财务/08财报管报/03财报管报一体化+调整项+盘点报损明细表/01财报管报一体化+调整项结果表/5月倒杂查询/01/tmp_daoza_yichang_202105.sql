--主要是说 调整明细是  能找到它不  有可能是对冲的  就是倒杂到销售表是错的  但是后面在成本调整里冲回来了
--成本调整那个表里找不到
--成本调整里没查到浙江有这个商品，4—6月都没有
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select a.*,b.adj_gc_xs_no,b.adj_gc_xs,b.adj_gc_db_no,b.adj_gc_db,b.adj_gc_qt_no,b.adj_gc_qt
from
(
select *
from csx_tmp.tmp_daoza_yichang_202105
)a
left join
(
select *,
--工厂月末分摊-调整销售订单
case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
			and adjustment_type='sale'
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
		then adjustment_amt_no_tax end adj_gc_xs_no,
case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
			and adjustment_type='sale'
			and item_wms_biz_type in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82') )
		then adjustment_amt end adj_gc_xs,
--工厂月末分摊-调整跨公司调拨订单
case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
			and adjustment_type='sale'
			and item_wms_biz_type in('06','07','08','09','12','15','17') )
		then adjustment_amt_no_tax end adj_gc_db_no,
case when (adjustment_reason in('fac_remark_sale','fac_remark_span') 
			and adjustment_type='sale'
			and item_wms_biz_type in('06','07','08','09','12','15','17') )
		then adjustment_amt end adj_gc_db,		
--工厂月末分摊-调整其他
case when adjustment_reason in('fac_remark_sale','fac_remark_span')		
		and adjustment_type='sale'
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
		then adjustment_amt_no_tax end adj_gc_qt_no,
case when adjustment_reason in('fac_remark_sale','fac_remark_span')  
		and adjustment_type='sale'
		and item_wms_biz_type not in('18','19','20','21','22','23','24','25','26','27','55','58','68','72','73','82','06','07','08','09','12','15','17')
		then adjustment_amt end adj_gc_qt		
from csx_ods.source_sync_r_d_data_relation_cas_sale_adjustment
where sdt = '19990101'
and posting_time >= '2021-05-01'
--and posting_time < '2021-06-01'
and (adjustment_reason in('fac_remark_sale','fac_remark_span') 
  and adjustment_type='sale')
)b on a.credential_no=b.credential_no and a.goods_code=b.product_code;  
  
--5月销售数据
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select id,business_type_name,channel_name,province_name,
dc_code,dc_name,customer_no,customer_name,channel_name,
first_category_name,sales_name,work_no,
order_time,goods_code,goods_name,origin_order_no,order_no,
purchase_price_flag,cost_price,purchase_price,middle_office_price,sales_price,
order_qty,sales_qty,sales_value,sales_cost,profit,front_profit,return_flag,logistics_mode_name,order_category_desc,sdt,sales_type
from csx_dw.dws_sale_r_d_detail
where sdt>='20210401'
and sdt<'20210601'
and concat(goods_code,'&',dc_code) in(
'3600&W0P8',
'10525&W0L3',
'5990&W0A3',
'1298477&W0H2',
'1351011&W0P8',
'791060&W0H4',
'324028&W0A5',
'1116803&W0N1',
'608&W0P8',
'404704&W0N0',
'554&W0A6',
'1141117&W0N9',
'721&W0A6',
'1359018&W0A2',
'317120&W0P8',
'1141117&W0A5',
'200&W0A6',
'707&W0H4',
'1239857&W0A7',
'474&W0A6',
'620&W0P8',
'471&W0P8',
'312915&W0M9',
'51&W0A3',
'1279445&W0A5',
'2152&W0H4',
'618&W0P8'
);  


---------------------------------------------hive 建表语句-----------------------------------------------
--5月倒杂金额_异常数据 csx_tmp.tmp_daoza_yichang_202105

drop table if exists csx_tmp.tmp_daoza_yichang_202105;
CREATE TABLE `csx_tmp.tmp_daoza_yichang_202105`(
  `credential_no` string COMMENT '凭证号',
  `order_code` string COMMENT '订单号',
  `goods_code` string COMMENT '商品编号',  
  `origin_order_code` string COMMENT '原始单号',  
  `sign_company_code` string COMMENT '签约公司编码', 
  `perform_dc_code` string COMMENT '履约地点编码', 
  `dc_code` string COMMENT '库存地点编码', 
  `cost_price` decimal(20,6) COMMENT '成本含税单价',
  `cost_price_after` decimal(20,6) COMMENT '成本含税单价(倒杂后)',  
  `cost_amt_diff` decimal(20,6) COMMENT '发货成本含税金额差值',
  `cost_amt_no_tax_diff` decimal(20,6) COMMENT '发货成本不含税金额差值'
) COMMENT '5月倒杂金额_异常数据'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--load data inpath '/tmp/raoyanhua/tmp_daza_yichang_202105.csv' overwrite into table csx_tmp.tmp_daoza_yichang_202105;
--select * from csx_tmp.tmp_daoza_yichang_202105;







  
  

  
  
  
  
  
  
  
  