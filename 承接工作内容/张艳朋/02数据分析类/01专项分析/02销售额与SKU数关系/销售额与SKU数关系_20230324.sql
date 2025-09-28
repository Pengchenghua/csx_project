-- ================================================================================================================	
-- 日配/福利销售额和SKU数增长的关系 就是我们销售额增长同时SKU数的情况关系如何，给出个结论

drop table if exists csx_analyse_tmp.csx_analyse_tmp_sale_sku_relation;
create table csx_analyse_tmp.csx_analyse_tmp_sale_sku_relation
as
select
	syear,smonth,performance_region_name,performance_province_name,business_type_name,
	sum(sale_amt) as sale_amt,count(distinct order_code) as order_cnt,count(goods_code) as sku_cnt
from
	(
	select
		substr(sdt,1,4) as syear,substr(sdt,1,6) as smonth,performance_region_name,performance_province_name,business_type_name,order_code,goods_code,sum(sale_amt) as sale_amt
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20190101' and '20230228'
		and channel_code in('1','7','9') -- 渠道编码(1:大客户 2:商超 4:大宗 5:供应链(食百) 6:供应链(生鲜) 7:bbc 8:其他 9:业务代理)
		and business_type_code in (1,2) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
		and performance_region_name not in ('BBC','其他')
	group by 
		substr(sdt,1,4),substr(sdt,1,6),performance_region_name,performance_province_name,business_type_name,order_code,goods_code
	) a 
group by 
	syear,smonth,performance_region_name,performance_province_name,business_type_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_sale_sku_relation