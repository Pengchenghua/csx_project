--====================================================================================================================================
-- 福建BBC&福利单数据需求
insert overwrite directory '/tmp/zhangyanpeng/20210115_fuli_fujian' row format delimited fields terminated by '\t'

select
	substr(sdt,1,6) as smonth,
	channel_name,
	business_type_name,
	city_group_name,
	customer_no,
	customer_name,
	goods_code,
	goods_name,
	department_code,
	department_name,
	classify_large_name,
	classify_middle_name,
	classify_small_name,
	sum(sales_qty) as sales_qty,
	sum(sales_value)as sales_value,	
	sum(profit)as profit,	
	sum(front_profit) as front_profit
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20200101' and '20201231'
	and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code in ('2','6') -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	and province_name='福建省'
group by 
	substr(sdt,1,6),
	channel_name,
	business_type_name,
	city_group_name,
	customer_no,
	customer_name,
	goods_code,
	goods_name,
	department_code,
	department_name,
	classify_large_name,
	classify_middle_name,
	classify_small_name
;

