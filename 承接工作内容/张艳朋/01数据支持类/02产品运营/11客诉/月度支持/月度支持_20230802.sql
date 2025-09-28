

-- 签收SKU数 只取渠道为大客户、业务代理，业务类型剔除BBC，城市服务商，不包含退货和调价返利
drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01;
create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01
as
select 
	substr(a.sdt,1,6) as smonth,a.performance_region_name,a.performance_province_name,a.performance_city_name,
	b.classify_large_name,b.classify_middle_name,b.business_division_name,
	count(a.goods_code) as sku_cnt
from
	( 
	select
		sdt,customer_code,order_code,goods_code,sale_amt,profit,performance_region_name,performance_province_name,performance_city_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230101' and sdt<='20230731'
		and channel_code in('1','7','9')
		and business_type_code not in(4,6) -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.bbc 7.大宗一部 8.大宗二部 9.商超)
		and order_channel_code =1 -- 1-b端 2-m端 3-bbc 4-客户返利管理 5-价格补救 6-客户调价管理 -1-sap
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and performance_province_name !='平台-B'
	) a 
	left join
		(
		select
			goods_code,goods_name,classify_large_name,classify_middle_name,classify_small_name,business_division_code,business_division_name
		from
			csx_dim.csx_dim_basic_goods
		where
			sdt='current'
		) b on b.goods_code=a.goods_code	
group by 
	substr(a.sdt,1,6),a.performance_region_name,a.performance_province_name,a.performance_city_name,b.classify_large_name,b.classify_middle_name,b.business_division_name
;
select * from csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01;	



-- 客户
drop table if exists csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_02;
create table csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_02
as	
select
	substr(a.sdt,1,6) as smonth,
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	f.customer_large_level,
	sum(a.sale_amt) sale_amt
from 
	(
	select
		sdt,customer_code,business_type_code,business_type_name,sale_amt,sale_amt_no_tax,profit,profit_no_tax,credit_code,
		performance_region_name,performance_province_name,performance_city_name
	from
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt>='20230701' and sdt<='20230731' 
		and channel_code in ('1', '7', '9') 
		and business_type_code in (1)
	) a 
	left join
		(
		select
			customer_no,month,customer_large_level
		from 
			csx_analyse.csx_analyse_report_sale_customer_level_mf
		where
			month>='202101' and month<='202307'
			and tag=1 --数据标识：1：全量数据；2：剔除不统计业绩仓数据
		group by 
			customer_no,month,customer_large_level
		) f on f.customer_no=a.customer_code and f.month=substr(a.sdt,1,6)
group by 
	substr(a.sdt,1,6),
	a.performance_region_name,
	a.performance_province_name,
	a.performance_city_name,
	a.customer_code,
	f.customer_large_level
;
select * from csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_02;