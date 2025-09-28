-- 附件3 大客户 大宗 bbc 供应链   order_kind	string	订单类型：NORMAL-普通单，WELFARE-福利单
--attribute_code	int	客户属性 1:日配客户 2:福利客户 3:贸易客户 4:战略客户 5:合伙人客户
--set mapreduce.job.queuename=caishixian;
set i_sdate =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-',''); -- 当前日n-1月第一天
set i_sdate_2=regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');--上月第一天

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.dws_sale_r_m_finance_bb partition(sdt)
select
  if(a.channel_name='业务代理','大客户',a.channel_name) channel_name,
  a.province_code,
  a.province_name,
   case when a.channel_name='大宗'  then '-'
       when a.province_name='安徽省' then '合肥市'
	   else a.city_name end city_name,
  a.attribute,
  a.first_category, 
  a.is_factory_goods_name,
  case when substr(a.department_code,1,1) IN ('H','U') THEN '生鲜' 
  when substr(a.department_code,1,1) IN ('A','1') THEN '食百' else '其他' end as is_department,
  a.order_kind,
  if(a.attribute='合伙人客户','是','否') as is_copemate_order,
  if(a.attribute='战略客户','是','否') as is_attribute_value, 
  CASE WHEN b.shop_id IS NULL THEN '外部客户'
    WHEN d.table_type = 1 THEN '彩食鲜-内部'
    WHEN d.table_type = 2 THEN '云超云创' END AS cust_cage,  ---供应链 供应链(生鲜) 供应链(食百)
  sum(sales_value) as sales_value,
  sum(excluding_tax_sales) as excluding_tax_sales,
  sum(sales_cost) as sales_cost,
  sum(excluding_tax_cost) as excluding_tax_cost,
  sum(profit) as profit,
  sum(excluding_tax_profit) as excluding_tax_profit,
  if(g.code is null, round(0.01*sum(excluding_tax_sales), 2), '') as excluding_tax_channel_value,
  substr(a.sdt,1,6) as sdt
from 
	(select 
          sign_company_code company_code,
          sales_date,
          customer_no,
          channel_name,
          province_code,
          province_name,
          case when province_name in ('浙江省','江苏省','福建省') then substr(city_group_name,1,3)
		       when province_name='安徽省' then '合肥市'
			   when province_name='河北省' then '石家庄'
			   when province_name='陕西省' then '西安市'
			   when province_name='广东省' then '深圳市'
			   when province_name='贵州省' then '贵阳市'
			   when province_name='四川省' then '成都市'
			   when province_name='重庆市' then '重庆市'
			   when province_name='北京市' then '北京市'
			   when province_name='上海市' then '上海市'
			  else city_group_name
		   end  city_name,
          attribute,
          first_category, 
          is_factory_goods_name,
          department_code,
          order_kind,
          sum(sales_value) as sales_value,
          sum(excluding_tax_sales) as excluding_tax_sales,
          sum(sales_cost) as sales_cost,
          sum(excluding_tax_cost) as excluding_tax_cost,
          sum(profit) as profit,
          sum(excluding_tax_profit) as excluding_tax_profit,
          substr(sdt,1,6) as sdt
          from csx_dw.dws_sale_r_d_customer_sale
        	where sdt >= ${hiveconf:i_sdate_2} and sdt < ${hiveconf:i_sdate}  and channel <> '2'
        	and order_no not in ('OC20111000000022','OC20111000000023','OC20111000000021','OC20111000000024','OC20111000000025')
        group by sign_company_code,customer_no,sales_date,
          channel_name,
          province_code,
          province_name,
          case when province_name in ('浙江省','江苏省','福建省') then substr(city_group_name,1,3)
		       when province_name='安徽省' then '合肥市'
			   when province_name='河北省' then '石家庄'
			   when province_name='陕西省' then '西安市'
			   when province_name='广东省' then '深圳市'
			   when province_name='贵州省' then '贵阳市'
			   when province_name='四川省' then '成都市'
			   when province_name='重庆市' then '重庆市'
			   when province_name='北京市' then '北京市'
			   when province_name='上海市' then '上海市'
			  else city_group_name
		   end ,
          attribute,
          first_category, 
          is_factory_goods_name,
          department_code,
          order_kind,substr(sdt,1,6)
        union all
        -- 返利 收款和退款
        select
        
          d.company_code
          ,a.sdt sales_date
          ,a.sap_cus_code customer_no
          ,b.channel channel_name
          ,b.sales_province_code  province_code
          ,b.sales_province  province_name
          ,b.sales_city  city_name
          ,b.attribute
          ,b.first_category
          ,if(f.workshop_code is null, '不是工厂商品', '是工厂商品') as is_factory_goods_name
          ,c.department_id department_code
          ,'NORMAL'  order_kind
          ,sum(a.total_price) as sales_value,
          sum(a.total_price_no_tax) as excluding_tax_sales,
          0.0 as sales_cost,
          0.0 as excluding_tax_cost,
          0.0 as profit,
          0.0 as excluding_tax_profit,
          substr(a.sdt,1,6) as sdt
        from
        (
          select *
          from csx_dw.dwd_csms_r_d_yszx_customer_rebate_detail_new
          where sdt >= ${hiveconf:i_sdate_2} and sdt < ${hiveconf:i_sdate}   and type in (0, 1)
        ) a left join
        (
          select * from csx_dw.dws_crm_w_a_customer_m_v1 --新表
          where sdt = regexp_replace(date_sub(current_date, 1), '-', '') and source = 'crm'
        ) b on a.sap_cus_code = b.customer_no
        left join
        (
          select *
          from csx_dw.dws_basic_w_a_csx_product_m
          where sdt = 'current'
        ) c on a.product_code = c.goods_id
        left join
        (
          select
            shop_id, company_code
          from csx_dw.dws_basic_w_a_csx_shop_m
          where sdt = 'current'
        ) d on a.agreement_dc_code = d.shop_id
        left join
(
  select
    shop_id, company_code, province_code
  from csx_dw.dws_basic_w_a_csx_shop_m
  where sdt = 'current'
) e on a.inventory_dc_code = e.shop_id
        left outer join
        (
          select *
          from csx_dw.dws_mms_w_a_factory_setting_craft_once_all
          where sdt = 'current' and new_or_old = 1
        )f on e.province_code = f.province_code and a.product_code = f.goods_code
        
        group by 
          d.company_code,a.sdt,a.sap_cus_code
          ,b.channel 
          ,b.sales_province_code  
          ,b.sales_province  
          ,b.sales_city  
          ,b.attribute
          ,b.first_category
          ,if(f.workshop_code is null, '不是工厂商品', '是工厂商品')
          ,c.department_id,substr(a.sdt,1,6)
        ) a 
LEFT JOIN
	(
	SELECT shop_id, company_code FROM csx_dw.dws_basic_w_a_csx_shop_m
	WHERE sdt = 'current'
	) b ON a.customer_no = concat('S', b.shop_id)
LEFT JOIN
	(
	SELECT code, table_type FROM csx_dw.dws_basic_w_a_company_code
	WHERE sdt = 'current'
	) d ON b.company_code = d.code
left join  (
   SELECT code FROM csx_ods.source_basic_w_a_md_company_code
   WHERE sdt = regexp_replace(date_sub(current_date, 1), '-', '')
   ) g on a.company_code = g.code	
group by 
   if(a.channel_name='业务代理','大客户',a.channel_name), a.province_code, a.province_name,g.code,
  a.attribute, a.first_category,
   case when a.channel_name='大宗'  then '-'
       when a.province_name='安徽省' then '合肥市'
	   else a.city_name end ,
  CASE WHEN b.shop_id IS NULL THEN '外部客户'
    WHEN d.table_type = 1 THEN '彩食鲜-内部'
    WHEN d.table_type = 2 THEN '云超云创' END,
  a.is_factory_goods_name,if(a.attribute='合伙人客户','是','否') 
  ,case when substr(a.department_code,1,1) IN ('H','U') THEN '生鲜' 
  when substr(a.department_code,1,1) IN ('A','1') THEN '食百' else '其他' end
  ,a.order_kind,substr(a.sdt,1,6);


-- 附件4

set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table csx_dw.dws_sale_r_m_finance_m partition(sdt)
select
  dc_code,
  dc_name,
  dc_company_code,
  channel_name,
  customer_no,
  customer_name,
  a.province_name,
  case when a.province_name='福建省' and a.sales_province='福建省' and a.sales_city in ('泉州市','南平市','莆田市','厦门市') then a.sales_city
       when a.province_name='福建省' and a.sales_province='福建省' and a.sales_city not in ('泉州市','南平市','莆田市','厦门市')  then '福州市' 
	   when a.province_name='福建省' and a.sales_province<>'福建省' then substr(a.city_real,1,3)
	   when a.province_name='江苏省' and a.dc_code='W0A5' then '昆山市'
	   when a.province_name='江苏省' and a.dc_code='W0K5' then '南京市'
	   when a.province_name='江苏省' and a.dc_code not in ('W0A5','W0K5') then ''
       else substr(a.city_real,1,2) end city_name,
  sales_belong_flag,
  case when substr(department_code,1,1) IN ('H','U') THEN '生鲜' 
  when substr(department_code,1,1) IN ('A','1') THEN '食百' else '其他' end as is_department,
  is_factory_goods_name,
  sum(sales_value) as sales_value,
  sum(excluding_tax_sales) as excluding_tax_sales,
  sum(sales_cost) as sales_cost,
  sum(excluding_tax_cost) as excluding_tax_cost,
  sum(profit) as profit,
  sum(excluding_tax_profit) as excluding_tax_profit,
  substr(a.sdt,1,6) as sdt
from 
 (
  select * from csx_dw.dws_sale_r_d_customer_sale
  where sdt >= ${hiveconf:i_sdate_2} and sdt < ${hiveconf:i_sdate}  and channel = '2' and customer_no not like 'S99%'
 and order_no not in ('OC20111000000022','OC20111000000023','OC20111000000021','OC20111000000024','OC20111000000025')
  ) a 
left join
  (
  select shop_id, sales_belong_flag from csx_dw.shop_m where sdt = 'current'
  ) b on a.customer_no = concat('S', b.shop_id)
group by dc_code, dc_name, dc_company_code,channel_name, customer_no, 
  customer_name,sales_belong_flag,is_factory_goods_name
  ,case when substr(department_code,1,1) IN ('H','U') THEN '生鲜' 
  when substr(department_code,1,1) IN ('A','1') THEN '食百' else '其他' 
  end,
  a.province_name,
  case when a.province_name='福建省' and a.sales_province='福建省' and a.sales_city in ('泉州市','南平市','莆田市','厦门市') then a.sales_city
       when a.province_name='福建省' and a.sales_province='福建省' and a.sales_city not in ('泉州市','南平市','莆田市','厦门市')  then '福州市' 
	   when a.province_name='福建省' and a.sales_province<>'福建省' then substr(a.city_real,1,3)
	   when a.province_name='江苏省' and a.dc_code='W0A5' then '昆山市'
	   when a.province_name='江苏省' and a.dc_code='W0K5' then '南京市'
	   when a.province_name='江苏省' and a.dc_code not in ('W0A5','W0K5') then ''
       else substr(a.city_real,1,2) end
  ,substr(a.sdt,1,6);
	
/* 删除分区
 alter table csx_dw.dws_sale_r_m_finance_m  drop partition (sdt='202005')
 同步数据
 INVALIDATE METADATA  csx_dw.dws_sale
 
 _r_m_finance_m;
 改表结构
 ALTER TABLE csx_dw.dws_sale_r_m_finance_m REPLACE COLUMNS 
 (`dc_code` string COMMENT '库存DC',
`dc_name` string COMMENT '库存DC名称',
`dc_company_code` string COMMENT 'DC所属公司编码',
`channel_name` string COMMENT '渠道',
`customer_no` string COMMENT '客户编码',
`customer_name` string COMMENT '客户名',
`province_name` string COMMENT '省区',
`city_name` string COMMENT '战报城市',
`sales_belong_flag` string COMMENT '客户类型（外部/关联交易/绿标/mini/超级物种/永辉生活/永辉到家/小店)',
`is_department` string COMMENT '生鲜/食百',	
`is_factory_goods_name` string COMMENT '是否加工商品',
`sales_value` decimal(26,6) COMMENT '含税销售额',
`excluding_tax_sales` decimal(26,6) COMMENT '不含税销售额',
`sales_cost` decimal(26,6) COMMENT '含税成本',
`excluding_tax_cost` decimal(26,6) COMMENT '不含税成本',
`profit` decimal(26,6) COMMENT '含税毛利',
`excluding_tax_profit` decimal(26,6) COMMENT '不含税毛利'
)
alter table csx_dw.dws_sale_r_m_finance_bb ADD COLUMNS 
(excluding_tax_channel_value decimal(26,6) COMMENT '通道费（不含税）');
 ALTER TABLE csx_dw.dws_sale_r_m_finance_bb REPLACE COLUMNS 
 (
`channel_name` string COMMENT '渠道',
`province_code` string COMMENT '省区编码',
`province_name` string COMMENT '省区',
`city_name` string COMMENT '战报城市',
`attribute` string COMMENT '客户属性',
`first_category` string COMMENT '企业一级分类',
`is_factory_goods_name` string COMMENT '加工商品/非加工商品',
`is_department` string COMMENT '生鲜/食百',	
`order_kind` string COMMENT '订单类型：NORMAL-普通单，WELFARE-福利单',
`is_copemate_order` string COMMENT '是否合伙人',
`is_attribute_value` string COMMENT '是否为战略客户',
`cust_cage` string COMMENT '供应链客户分类',
`sales_value` decimal(26,6) COMMENT '销售额',
`excluding_tax_sales` decimal(26,6) COMMENT '不含税销售额',
`sales_cost` decimal(26,6) COMMENT '定价成本',
`excluding_tax_cost` decimal(26,6) COMMENT '不含税定价成本',
`profit` decimal(26,6) COMMENT '定价毛利',
`excluding_tax_profit` decimal(26,6) COMMENT '不含税定价毛利',
 )*/


