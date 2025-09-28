select count(1) `总数量`,
count(case when is_fact='是' then 1 end) `工厂加工数量`,
count(case when credential_no is not null then 1 end) `批次数量`,
count(case when credential_no is null and is_fact is null then 1 end) `工厂与批次都无的数量`
from csx_tmp.tmp_factory_order_to_scm_13;


select substr(order_code,1,2),count(1)
from csx_tmp.tmp_factory_order_to_scm_14
where credential_no is null and is_fact is null
group by substr(order_code,1,2);


select warehouse_code dc_code,
price_begin_time,
price_end_time,
product_code
from csx_ods.source_price_r_d_effective_purchase_prices
where sdt=regexp_replace(date_sub(current_date, 1), '-', '')
and effective='1';


select
  goods_code,
  credential_no,
  batch_no,
  source_order_no,*
from csx_dw.dws_wms_r_d_batch_detail
where source_order_no='POW0A2210329002175'
and goods_code in('921185','900151');
    
select 
  product_code,--原料编号
  goods_code,--成品商品编号
  order_code,*
from csx_dw.dws_mms_r_a_factory_order
where sdt >= '20210301' and mrp_prop_key in('3061','3010') 
and order_code='WO210410004839'
and goods_code in('5180','1239858');
    
select 
  target_location_code,
  order_code,*
from csx_dw.dws_scm_r_d_order_received
where order_code='POW0A2210329002175'
and goods_code in('921185','900151');

insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select *
from csx_tmp.tmp_sale_detail_abnormal_label
limit 10;

select count(1), 
count(distinct concat(goods_code,credential_no)) count1, 
count(distinct concat(batch_no,goods_code,credential_no)) count2 
from csx_tmp.tmp_sale_detail_abnormal_label; 

select count(1), 
count(distinct concat(goods_code,order_code)) count1, 
count(distinct concat(batch_no,goods_code,order_code)) count2 
from csx_tmp.tmp_factory_order_to_scm_14;



--近14天平均价
select 
    goods_code,
    DC_DC_code,
    scm_sdt,
    received_qty_ls,
    received_value_ls,
    received_value_ls/received_qty_ls received_price_ls	
  from 
  (
    select 
      goods_code,
      DC_DC_code,
	  scm_sdt,
      sum(received_qty) over(partition by goods_code,DC_DC_code order by unix_timestamp(scm_sdt,'yyyyMMdd') asc range between 1209600 preceding and 0 preceding) as received_qty_ls,
      sum(received_value) over(partition by goods_code,DC_DC_code order by unix_timestamp(scm_sdt,'yyyyMMdd') asc range between 1209600 preceding and 0 preceding) as received_value_ls,
    from csx_tmp.tmp_goods_received_1;
	
select count(1), 
count(distinct concat(order_code,goods_code)) count1, 
count(distinct concat(scm_order_code,order_code,batch_no,goods_code)) count2,
count(distinct concat(scm_order_code,order_code,batch_no,goods_code,credential_no)) count3
from csx_tmp.tmp_received_batch_detail_abnormal_label; 

select count(1)
from csx_tmp.tmp_scm_factory_order_to_batch_ls;


558442  tmp_scm_factory_order_to_batch_1  采购+工厂
558808  tmp_scm_factory_order_to_batch_2  采购+工厂+批次
558808  tmp_scm_factory_order_to_batch_3  采购+工厂+批次 关联采购标签
558808  tmp_received_batch_detail_abnormal_label 结果表


558808  531235  321101  321101  321920
558808  531235  321101  321101  321920
558808  531235  321101  321101  321920




select sum(if(sdt<>'',1,0)) scm_order_code,
sum(if(scm_order_code<>'',1,0)) scm_order_code,
sum(if(order_code<>'',1,0)) order_code,
sum(if(batch_no<>'',1,0)) batch_no,
sum(if(goods_code<>'',1,0)) goods_code,
sum(if(credential_no<>'',1,0)) credential_no,
sum(if(scm_order_code<>'' and order_code<>'' and goods_code<>'',1,0)) count
from csx_tmp.tmp_scm_factory_order_to_batch_4;

POW039210425000404&WO210521004973&CB20210521023964&319749&PZ20210521021994&20210521

select concat_ws('&',order_code,goods_code) as id,
count(1)
from csx_tmp.tmp_goods_received_d where scm_sdt>='20201001'
group by concat_ws('&',order_code,goods_code)
having id>=2;


select id,count(1) count1
from csx_tmp.tmp_received_batch_detail_abnormal_label
group by id
having count1>=2;

select *
from csx_tmp.tmp_goods_received_d1 
where order_code='PO99B1210521001956'
and goods_code='69044';




	