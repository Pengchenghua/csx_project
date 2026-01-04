--物流客户率
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
		sdt>='20250101' and sdt<='20251225'
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


select
  a.*,
  b.ks_cnt,
  b.ks_cnt/sku_cnt as ks_lv
from
  (
    select
      smonth,
      performance_region_name,
      performance_province_name,
      performance_city_name,
      sum(sku_cnt) sku_cnt
    from
      csx_analyse_tmp.csx_analyse_tmp_oms_complaint_report_01
    group by
      smonth,
      performance_region_name,
      performance_province_name,
      performance_city_name
  ) a
  left join (
    select
      months,
      region_name,
      province_name,
      city_name,
      count(distinct order_code) as ks_cnt 
	  -- 手工导入临时表
    from csx_analyse_tmp.csx_analyse_tmp_oms_complaint_pch a 
    join 
    (select  complaint_code from  csx_analyse.csx_analyse_fr_oms_complaint_detail_new_di
        where sdt>='20250101'
            and first_level_department_name='物流'
        group by complaint_code
    )b on a.order_code=b.complaint_code
    group by
      months,
      region_name,
      province_name,
      city_name
  ) b on a.smonth = b.months
  and a.performance_city_name = b.city_name
  and a.performance_province_name = b.province_name