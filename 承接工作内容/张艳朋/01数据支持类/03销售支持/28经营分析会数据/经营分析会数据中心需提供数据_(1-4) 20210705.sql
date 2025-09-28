-- 断约客户数
select
	region_name,province_name,city_group_name,s_sdt,count(distinct customer_no) as sub_customer_cnt
from
	(
	select 
		region_name,province_name,city_group_name,customer_no,
		substr(regexp_replace(date_add(from_unixtime(unix_timestamp(max(sdt),'yyyyMMdd')),90),'-',''),1,6) as s_sdt
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20190101' and '20210630'
		and business_type_code in ('1') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and sales_type !='fanli'
	group by 
		region_name,province_name,city_group_name,customer_no
	) tmp1
where
	s_sdt>='202101'
	and s_sdt<='202106'
group by 
	region_name,province_name,city_group_name,s_sdt
	
	
	
	
	
-- 含税福利收入
	select 
		region_name,province_name,city_group_name,substr(sdt,1,6) as s_sdt,
		sum(sales_value) as sales_value
	from 
		csx_dw.dws_sale_r_d_detail 
	where 
		sdt between '20200101' and '20200630'
		and business_type_code in ('2') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
	group by 
		region_name,province_name,city_group_name,substr(sdt,1,6)