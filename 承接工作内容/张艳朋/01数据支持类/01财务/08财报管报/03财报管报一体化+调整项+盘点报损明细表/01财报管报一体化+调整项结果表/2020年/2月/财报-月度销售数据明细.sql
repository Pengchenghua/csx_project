
--销售端明细数据
insert overwrite directory '/tmp/raoyanhua/linshi01' row format delimited fields terminated by '\t' 
select a.AA,a.sdt,a.customer_no,a.customer_name,a.channel_name,a.province_code,a.province_name,a.shop_id,
	a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
	a.department_code,a.department_name,
	sum(a.sales_value) sales_value,sum(a.sales_cost) sales_cost,sum(a.profit) profit	
from 
(select 
a.AA,a.sdt,a.customer_no,b.customer_name,a.channel_name,a.province_code,a.province_name,a.shop_id,
	a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
	a.order_no,a.credential_no,a.goods_code,a.goods_name,a.department_code,a.department_name,
    a.sales_value,a.sales_cost,a.profit	
from b2b_tmp.cust_sales_m2 a 
left join 
( select
a.customer_no,
a.customer_name,
a.attribute,
a.channel,
a.sales_id,
a.sales_name,
a.work_no,
a.first_supervisor_name,
a.second_supervisor_name,
a.third_supervisor_name,
a.fourth_supervisor_name,
a.sales_province,
a.sales_city,
a.first_category,
a.second_category,
a.third_category,
regexp_replace(split(a.sign_time, ' ')[0], '-', '') as sign_date
from csx_dw.customer_m a
where a.sdt=regexp_replace(date_sub(current_date,1),'-','')
and length(a.customer_no) > 0 )b on b.customer_no=a.customer_no) a
group by a.AA,a.sdt,a.customer_no,a.customer_name,a.channel_name,a.province_code,a.province_name,a.shop_id,
	a.attribute,a.first_category,a.second_category,a.sales_name,a.work_no,
	a.department_code,a.department_name;
	
	
	
	



--数据源，处理省区、渠道信息
drop table b2b_tmp.cust_sales_detail_m;
create table b2b_tmp.cust_sales_detail_m
as
select a.AA,a.sdt,coalesce(b.channel,a.channel) channel,a.dc_code,a.origin_shop_id,a.division_code,a.customer_no,a.sales_value,a.sales_cost,a.profit,b.sales_province,
b.attribute,b.first_category,b.second_category,b.third_category,b.sales_name,b.work_no,
b.supervisor_id,b.supervisor_name,b.sales_manager_id,b.sales_manager_name,b.city_manager_id,b.city_manager_name,b.item_province_manager_id,b.item_province_manager_name,
b.item_province_name,b.item_province_code,b.item_city_name,b.item_city_code,
a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name
from
(select '成本'AA,credential_no,product_code goods_code,product_name goods_name,source_order_no order_no,purchase_group_code department_code,purchase_group_name department_name,
regexp_replace(split(create_time, ' ')[0], '-', '') as sdt,
case when source_sys='1' then '大客户'
	when source_sys='2' then '企业购'
	when source_sys='3' then '商超'
	else '其他' end channel,
location_code dc_code,  
'' origin_shop_id,
root_category	division_code, 
customer_no,
sale_amt sales_value,
cost_amt sales_cost,
profit_amt profit
from csx_dw.dwd_sync_r_d_data_relation_cas_sale_detail
where create_time>='2020-01-01'
and create_time<'2020-03-1'
 )a
left join 
( select
a.customer_no,
a.customer_name,
a.attribute,
a.channel,
a.sales_id,
a.sales_name,
a.work_no,
a.first_supervisor_code as supervisor_id,--销售主管
a.first_supervisor_name as supervisor_name,
a.first_supervisor_work_no as supervisor_work_no,
a.second_supervisor_code as sales_manager_id,-- 销售经理
a.second_supervisor_name as sales_manager_name, 
a.second_supervisor_work_no as  sales_manager_work_no,
a.third_supervisor_code as city_manager_id,
a.third_supervisor_name as city_manager_name, -- 城市经理
a.third_supervisor_work_no as city_manager_work_no,
a.fourth_supervisor_code as  item_province_manager_id,
a.fourth_supervisor_name as item_province_manager_name,-- 省区经理
a.sales_province,
a.sales_city,
a.province_name as item_province_name,
a.province_code as item_province_code,
a.city_name as item_city_name, 
a.city_code as item_city_code,
a.first_category,
a.second_category,
a.third_category,
regexp_replace(split(a.sign_time, ' ')[0], '-', '') as sign_date
from csx_dw.customer_m a
where a.sdt=regexp_replace(date_sub(current_date,2),'-','')
and length(a.customer_no) > 0 )b on b.customer_no=a.customer_no;


drop table b2b_tmp.cust_sales_detail_m1;
create table b2b_tmp.cust_sales_detail_m1
as
select
AA,sdt,shop_id,customer_no,sales_value,sales_cost,profit,channel,
attribute,first_category,second_category,third_category,sales_name,work_no,
supervisor_id,supervisor_name,sales_manager_id,sales_manager_name,city_manager_id,city_manager_name,item_province_manager_id,item_province_manager_name,
item_province_name,item_province_code,item_city_name,item_city_code,
credential_no,goods_code,goods_name,order_no,department_code,department_name,
  case
    when channel = '1'  OR channel = '' then '大客户'
    when channel = '2' then '商超'
    when channel = '3' then '商超(对外)'
    when channel = '4' then '大宗'
    when channel = '5' then '供应链(食百)'
    when channel = '6' then '供应链(生鲜)'
    when channel = '7' then '企业购 '
    when channel = '8' then '其他'
  end as channel_name,


  case
    when channel = '5' then '平台-食百采购'
    when channel = '6' then '平台-生鲜采购'
    when channel = '4' then '平台-大宗'
    else a.province_name
  end as province_name

from
(
select
a.AA,a.sdt,a.shop_id,a.customer_no,a.sales_value,a.sales_cost,a.profit, 
a.attribute,a.first_category,a.second_category,a.third_category,a.sales_name,a.work_no,
a.supervisor_id,a.supervisor_name,a.sales_manager_id,a.sales_manager_name,a.city_manager_id,a.city_manager_name,a.item_province_manager_id,a.item_province_manager_name,
a.item_province_name,a.item_province_code,a.item_city_name,a.item_city_code,
a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name,
  case 
    when  a.shop_id like 'E%' then '2'
    --when a.origin_shop_id = 'W0B6' or a.channel like '%企业购%' or a.sales_type='bbc' then '7'
    when a.origin_shop_id = 'W0B6' or a.channel like '%企业购%' then '7'	
    when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and a.category_code in ('12','13','14') ) 
      or (a.channel like '供应链%' and a.category_code in ('12','13','14'))then '5'
    when (a.shop_id = 'W0H4' and a.customer_no like 'S%' and a.category_code in ('10','11'))
      or (a.channel like '供应链%' and a.category_code in ('10', '11'))then '6'  
    when a.channel = '大客户' or a.channel = 'B端' then '1'
    when a.channel ='M端'  or a.channel like '%对内%' or a.channel='商超' then '2'
    when a.channel like '%对外%' then '3'
    when a.channel = '大宗' then '4'  
    when a.channel='其他' then '8'
    else ''
    end as channel,

   case
     when a.shop_id in ('W0M1','W0M4','W0J6','W0M6') then '商超平台' 
     when a.customer_no is not null and a.sales_province='BBC' then '福建省'
     when a.sales_province is not null and a.channel <> 'M端' and a.channel not like '商超%'  then a.sales_province
     when a.customer_no like 'S%' and substr(c.province_name, 1, 2) 
       in ('重庆','四川','北京','福建','上海','浙江','江苏','安徽','广东','贵州','陕西') 
       then c.province_name
     else d.province_name end as province_name
from 
(select AA,sdt,channel,dc_code shop_id,origin_shop_id,division_code as category_code,customer_no,sales_value,sales_cost,profit,sales_province,
attribute,first_category,second_category,third_category,sales_name,work_no,
supervisor_id,supervisor_name,sales_manager_id,sales_manager_name,city_manager_id,city_manager_name,item_province_manager_id,item_province_manager_name,
item_province_name,item_province_code,item_city_name,item_city_code,
credential_no,goods_code,goods_name,order_no,department_code,department_name,
case when dc_code like 'E%' then concat('9',substr(dc_code,2,3)) else dc_code end shop_no 
from b2b_tmp.cust_sales_detail_m )a
left outer join 
(
  select
    shop_id,
    case
      when shop_id in ('W055') then '上海市'  else province_name
      end province_name,
    case
      when province_name like '%市' then province_name
      else city_name
      end city_name     
  from csx_dw.shop_m 
  where sdt = 'current'
)c 
on a.customer_no = concat('S',c.shop_id)
left outer join
(
  select
    shop_id,
    shop_name,
    province_name
  from
    csx_dw.shop_m
  where
    sdt = 'current' 
)d 
on a.shop_no = d.shop_id )a;


drop table b2b_tmp.cust_sales_detail_m2;
create table b2b_tmp.cust_sales_detail_m2
as
select a.AA,a.sdt,a.shop_id,a.customer_no,a.sales_value,a.sales_cost,a.profit,
	a.attribute,a.first_category,a.second_category,a.third_category,a.sales_name,a.work_no,
	a.supervisor_id,a.supervisor_name,a.sales_manager_id,a.sales_manager_name,a.city_manager_id,a.city_manager_name,a.item_province_manager_id,a.item_province_manager_name,
	a.item_province_name,a.item_province_code,a.item_city_name,a.item_city_code,
	a.credential_no,a.goods_code,a.goods_name,a.order_no,a.department_code,a.department_name,
    a.channel,
    a.channel_name,
    case when a.province_name='商超平台'  then '-100' else g.province_code end province_code,
    case when a.province_name='平台-B' then '大客户平台' else a.province_name end province_name,
	 a.city_name,

  case
    when a.province_name = '福建省' then coalesce(b.city_real,'福州、宁德、三明')  
    when a.province_name = '江苏省' then coalesce(b.city_real,'苏州')  
    when a.province_name = '浙江省' and  supervisor_id='1000000211087' then '宁波'
    when a.province_name = '浙江省' and  supervisor_id<>'1000000211087' then '杭州'
    else '-' end city_real,
  case
    when a.province_name = '福建省' then coalesce(b.cityjob,'沈锋')
    when a.province_name = '江苏省' then coalesce(b.cityjob,'部桦')    
    when a.province_name = '浙江省' and  supervisor_id='1000000211087' then '林艳'
    when a.province_name = '浙江省' and  supervisor_id<>'1000000211087' then '王海燕'
    else '-' end cityjob
	
from
	(select
	AA,sdt,shop_id,customer_no,sales_value,sales_cost,profit,
	attribute,first_category,second_category,third_category,sales_name,work_no,
	supervisor_id,supervisor_name,sales_manager_id,sales_manager_name,city_manager_id,city_manager_name,item_province_manager_id,item_province_manager_name,
	item_province_name,item_province_code,item_city_name,item_city_code,
	credential_no,goods_code,goods_name,order_no,department_code,department_name,
		case when channel is null or channel='' then '1' when province_name='平台-B' and channel='1' then '1' else channel end channel,
		case when channel is null or channel='' then '大客户' when province_name='平台-B' and channel='1' then '大客户' else channel_name end channel_name,
		case when province_name ='成都省' then '四川省'   else province_name end province_name,
    case when channel='2'  then city_name 
        --when channel='7' then '福州' 
        when (channel<>'2')  then region_city else '-' 
        end city_name		
	from b2b_tmp.cust_sales_detail_m1	)a
  left outer join
  (
    select '泉州'city,'泉州'city_real,'张铮'cityjob
    union all 
    select '莆田'city,'莆田'city_real,'倪薇红'cityjob
    union all 
    select '南平'city,'南平'city_real,'林挺'cityjob
    union all 
    select '厦门'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
    union all 
    select '漳州'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
    union all 
    select '龙岩'city,'厦门、龙岩、漳州'city_real,'崔丽'cityjob
    union all 
    select '福州'city,'福州、宁德、三明'city_real,'沈锋'cityjob
    union all 
    select '宁德'city,'福州、宁德、三明'city_real,'沈锋'cityjob
    union all 
    select '三明'city,'福州、宁德、三明'city_real,'沈锋'cityjob
    union all 
    select '南京'city,'南京'city_real,'黄巍'cityjob
  )b on substr(a.city_name,1,2)=b.city	
  left outer join 
  (
    select
      province_code,
      province
    from csx_ods.sys_province_ods
  )g on a.province_name=g.province;

