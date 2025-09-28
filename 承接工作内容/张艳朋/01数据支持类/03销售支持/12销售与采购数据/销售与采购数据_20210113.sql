--====================================================================================================================================
-- 干货加工
insert overwrite directory '/tmp/zhangyanpeng/20210113_classify_large_B01' row format delimited fields terminated by '\t'

select 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6) as smonth,goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20200101' and '20201231'
	and channel_code in('1','2','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !='4' -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	and classify_large_code='B01'
group by 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6),goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name
;


--====================================================================================================================================
-- 日配食品
insert overwrite directory '/tmp/zhangyanpeng/20210113_classify_large_B07' row format delimited fields terminated by '\t'

select 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6) as smonth,goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20200101' and '20201231'
	and channel_code in('1','2','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !='4' -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	and classify_large_code='B07'
group by 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6),goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name
;


--====================================================================================================================================
-- 调味杂货
insert overwrite directory '/tmp/zhangyanpeng/20210113_classify_large_B06' row format delimited fields terminated by '\t'

select 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6) as smonth,goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20200101' and '20201231'
	and channel_code in('1','2','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !='4' -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	and classify_large_code='B06'
group by 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6),goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name
;


--====================================================================================================================================
-- 烟酒饮料
insert overwrite directory '/tmp/zhangyanpeng/20210113_classify_large_B04' row format delimited fields terminated by '\t'

select 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6) as smonth,goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20200101' and '20201231'
	and channel_code in('1','2','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !='4' -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	and classify_large_code='B04'
group by 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6),goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name
;


--====================================================================================================================================
-- 休闲食品
insert overwrite directory '/tmp/zhangyanpeng/20210113_classify_large_B05' row format delimited fields terminated by '\t'

select 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6) as smonth,goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name,
	sum(sales_value)as sales_value,
	sum(profit)as profit,
	sum(profit)/abs(sum(sales_value)) as profit_rate,
	sum(excluding_tax_sales) as excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit,
	sum(excluding_tax_profit)/abs(sum(excluding_tax_sales)) as excluding_tax_profit_rate
from 
	csx_dw.dws_sale_r_d_detail 
where 
	sdt between '20200101' and '20201231'
	and channel_code in('1','2','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	and business_type_code !='4' -- 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
	and classify_large_code='B05'
group by 
	region_code,region_name,province_code,province_name,city_group_code,city_group_name,substr(sdt,1,6),goods_code,goods_name,
	classify_large_code,classify_large_name,classify_middle_code,classify_middle_name,classify_small_code,classify_small_name,
	first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
	attribute_name,business_type_name,channel_code,channel_name
;
