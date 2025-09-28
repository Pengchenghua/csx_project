
insert overwrite directory '/tmp/zhangyanpeng/20220412_01' row format delimited fields terminated by '\t'

select
	region_name,province_name,city_group_name,a.order_no,to_date(order_time) as order_date,substr(order_time,12,8) as order_time,
	business_type_name,order_channel_name,logistics_mode_name,
	if(is_patch_order=1,'是','否') as is_patch_order,
	customer_no,customer_name,child_customer_no,child_customer_name,dc_code,dc_name,
	count(distinct goods_code) as goods_cnt,sum(sales_value)
from 
	(select * from csx_dw.dws_sale_r_d_detail where to_date(order_time)>='2022-03-01' and to_date(order_time)<='2022-03-31' 
		and channel_code in('1','7','9') and business_type_code !='4') a 
	left join(select order_no,is_patch_order from csx_dw.dws_csms_r_d_yszx_order_m_new group by order_no,is_patch_order) b on b.order_no=a.order_no
group by 
	region_name,province_name,city_group_name,a.order_no,to_date(order_time),substr(order_time,12,8),
	business_type_name,order_channel_name,logistics_mode_name,if(is_patch_order=1,'是','否'),customer_no,customer_name,child_customer_no,child_customer_name,dc_code,dc_name
			
