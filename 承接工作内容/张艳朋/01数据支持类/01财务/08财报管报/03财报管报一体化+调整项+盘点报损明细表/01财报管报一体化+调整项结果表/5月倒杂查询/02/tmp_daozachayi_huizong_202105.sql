
--insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t'
select case when channel_code in('1','9') then 'B端' 
            when channel_code ='2' then 'M端' 
			when channel_code ='7' then 'BBC' 
			when channel_code ='4' then '大宗' 
			when channel_code in('5','6') then '供应链' end channel_name,
  region_name,province_name,city_group_name,
  diff_groups,source_file,source_sheet,
  sum(use_amt_no_tax) use_amt_no_tax,
  sum(use_amt) use_amt  
from 
(
  select a.*,b.channel_code,b.channel_name,b.region_name,b.province_name,b.city_group_name
  from
  (
  select credential_no,diff_groups,source_file,source_sheet,
    sum(use_amt_no_tax) use_amt_no_tax,
    sum(use_amt) use_amt
  from csx_dw.daozachayi_huizong_202105
  group by credential_no,diff_groups,source_file,source_sheet
  )a
  left join
  (
  select distinct split(id, '&')[0] as credential_no,business_type_name,channel_code,channel_name,region_name,province_name,city_group_name,
  dc_code,dc_name,customer_no,customer_name
  from csx_dw.dws_sale_r_d_detail
  where sdt>='20210501'
  --and sdt<'20210601'
  )b on a.credential_no=b.credential_no
)a
group by case when channel_code in('1','9') then 'B端' 
            when channel_code ='2' then 'M端' 
			when channel_code ='7' then 'BBC' 
			when channel_code ='4' then '大宗' 
			when channel_code in('5','6') then '供应链' end,
  region_name,province_name,city_group_name,
  diff_groups,source_file,source_sheet; 

--区分生鲜食百
select case when channel_code in('1','9') then 'B端' 
            when channel_code ='2' then 'M端' 
			when channel_code ='7' then 'BBC' 
			when channel_code ='4' then '大宗' 
			when channel_code in('5','6') then '供应链' end channel_name,
  region_name,province_name,city_group_name,division_name,
  diff_groups,source_file,source_sheet,
  sum(use_amt_no_tax) use_amt_no_tax,
  sum(use_amt) use_amt  
from 
(
  select a.*,b.channel_code,b.channel_name,b.region_name,b.province_name,b.city_group_name,
    if(substr(c.department_id,1,1) in('U','H'),'生鲜','食百') division_name
  from
  (
  select credential_no,goods_code,diff_groups,source_file,source_sheet,
    sum(use_amt_no_tax) use_amt_no_tax,
    sum(use_amt) use_amt
  from csx_dw.daozachayi_huizong_202105
  group by credential_no,goods_code,diff_groups,source_file,source_sheet
  )a
  left join
  (
  select distinct split(id, '&')[0] as credential_no,business_type_name,channel_code,channel_name,region_name,province_name,city_group_name,
  dc_code,dc_name,customer_no,customer_name
  from csx_dw.dws_sale_r_d_detail
  where sdt>='20210501'
  --and sdt<'20210601'
  )b on a.credential_no=b.credential_no
  left join
  (
  select goods_id,goods_name,department_id,department_name
  from csx_dw.dws_basic_w_a_csx_product_m where sdt = 'current' 
  )c on a.goods_code=c.goods_id 
)a
group by case when channel_code in('1','9') then 'B端' 
            when channel_code ='2' then 'M端' 
			when channel_code ='7' then 'BBC' 
			when channel_code ='4' then '大宗' 
			when channel_code in('5','6') then '供应链' end,
  region_name,province_name,city_group_name,division_name,
  diff_groups,source_file,source_sheet; 


---------------------------------------------hive 建表语句-----------------------------------------------
--5月倒杂差异_汇总导入数据20210728 csx_tmp.daozachayi_huizong_202105

drop table if exists csx_tmp.daozachayi_huizong_202105;
CREATE TABLE `csx_tmp.daozachayi_huizong_202105`(
  `credential_no` string COMMENT '凭证号',
  `order_code` string COMMENT '订单号',
  `goods_code` string COMMENT '商品编号',  
  `origin_order_code` string COMMENT '原始单号',  
  `sign_company_code` string COMMENT '签约公司编码', 
  `perform_dc_code` string COMMENT '履约地点编码', 
  `dc_code` string COMMENT '库存地点编码',
  `is_factory` string COMMENT '是否工厂地点',  
  `cost_price` decimal(20,6) COMMENT '成本含税单价',
  `cost_price_after` decimal(20,6) COMMENT '成本含税单价(倒杂后)',  
  `cost_amt_diff` decimal(20,6) COMMENT '发货成本含税金额差值',
  `cost_amt_no_tax_diff` decimal(20,6) COMMENT '发货成本不含税金额差值',
  `daoza_before_after_diff` decimal(20,6) COMMENT '倒杂前_倒杂后',
  `adjustment_amt_no_tax` decimal(20,6) COMMENT '成本调整金额不含税',
  `adjustment_amt` decimal(20,6) COMMENT '成本调整金额',
  `wubiaoti` decimal(20,6) COMMENT '无标题',
  `other_sub` decimal(20,6) COMMENT '刨减成本',
  `other_add` decimal(20,6) COMMENT '需增加成本',
  `use_amt_no_tax` decimal(20,6) COMMENT '最终使用金额_不含税', 
  `use_amt` decimal(20,6) COMMENT '最终使用金额_含税',   
  `diff_groups` string COMMENT '调定价成本or调整销售', 
  `source_file` string COMMENT '来源文件',
  `source_sheet` string COMMENT '来源表'  
) COMMENT '5月倒杂差异_汇总导入数据20210728(重要)'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE;

--load data inpath '/tmp/raoyanhua/daozachayi_huizong_202105.csv' overwrite into table csx_tmp.daozachayi_huizong_202105;
--select * from csx_tmp.daozachayi_huizong_202105;








  
  

  
  
  
  
  
  
  
  