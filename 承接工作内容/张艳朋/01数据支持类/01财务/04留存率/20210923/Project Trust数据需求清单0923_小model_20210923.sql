--===================================================================================================================================================================================
--判断客户联系俩月下单日期
--日配业务

drop table csx_tmp.tmp_project_trust_customer_normal;
create table csx_tmp.tmp_project_trust_customer_normal
as
select
	customer_no,next_month
from
	(
	select
		customer_no,
		s_month,
		lead(s_month,1,0) over (partition by customer_no order by s_month) as next_month,
		row_number() over(partition by customer_no order by s_month) as rn
	from
		(
		select
			customer_no,substr(sdt,1,6) as s_month,sum(sales_value) as sales_value
		from
			csx_dw.dws_sale_r_d_detail 
		where 
			sdt>='20190101' and sdt<='20210731' 
			and business_type_code='1'
			and sales_type !='fanli'
		group by 
			customer_no,substr(sdt,1,6)
		) t1
	) t1
where
	rn=1
	and substr(regexp_replace(add_months(from_unixtime(unix_timestamp(concat(s_month,'01'),'yyyyMMdd')),1),'-',''),1,6)=next_month
group by 
	customer_no,next_month
;

insert overwrite directory '/tmp/zhangyanpeng/20210805_linshi_3' row format delimited fields terminated by '\t' 
--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select
	sales_province_name,
	city_group_name,
	new_smonth,
	smonth,
	diff_month,
	case when diff_month=0 then '新客' when diff_month>0 then '老客' when diff_month='-1' then '首月' else '其他' end as type,
	cooperation_mode_name,
	count(distinct customer_no) counts,
	sum(excluding_tax_sales) excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit
from
	(
	select 
		c.sales_province_name,
		c.city_group_name,
		a.new_smonth,
		b.smonth,
		floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
		b.customer_no,
		c.cooperation_mode_name,
		b.excluding_tax_sales,
		b.excluding_tax_profit
	from
		(
		select 
			customer_no,substr(next_month,1,6) new_smonth
		from
			csx_tmp.tmp_project_trust_customer_normal
		--where 
			--substr(next_month,1,6)>='202001'
		)a
		right join -- 每月日配业务履约金额大于0	
			(
			select 
				customer_no,substr(sdt,1,6) smonth,
				sum(excluding_tax_sales) excluding_tax_sales,
				sum(excluding_tax_profit) as excluding_tax_profit
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20190101' and sdt<='20210731'
				and channel_code in('1','7','9')
				and business_type_code in ('1') -- 日配
				--and sales_type !='fanli'
			group by 
				customer_no,substr(sdt,1,6)
			)b on a.customer_no=b.customer_no
		join
			(
			select 
				customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,cooperation_mode_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
				and channel_code in('1','7','9')
				--and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
				and (sales_province_name in ('福建省','安徽省') or city_group_name='重庆主城')
			)c on c.customer_no=b.customer_no
	) as a 
group by
	sales_province_name,
	city_group_name,
	new_smonth,
	smonth,
	diff_month,
	case when diff_month=0 then '新客' when diff_month>0 then '老客' when diff_month='-1' then '首月' else '其他' end,
	cooperation_mode_name
;


--===================================================================================================================================================================================
--判断客户首月下单日期
--福利业务

drop table csx_tmp.tmp_project_trust_customer_fuli;
create table csx_tmp.tmp_project_trust_customer_fuli
as
select
	customer_no,substr(min(sdt),1,6) as first_month
from
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20190101' and sdt<='20210731' 
	and business_type_code='2' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
group by 
	customer_no

;


insert overwrite directory '/tmp/zhangyanpeng/20210805_fuli_1' row format delimited fields terminated by '\t' 
--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select
	sales_province_name,
	city_group_name,
	new_smonth,
	smonth,
	diff_month,
	case when diff_month=0 then '新客' when diff_month>0 then '老客' when diff_month='-1' then '首月' else '其他' end as type,
	cooperation_mode_name,
	count(distinct customer_no) counts,
	sum(excluding_tax_sales) excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit
from
	(
	select 
		c.sales_province_name,
		c.city_group_name,
		a.new_smonth,
		b.smonth,
		floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
		b.customer_no,
		c.cooperation_mode_name,
		b.excluding_tax_sales,
		b.excluding_tax_profit
	from
		(
		select 
			customer_no,substr(first_month,1,6) new_smonth
		from
			csx_tmp.tmp_project_trust_customer_fuli
		--where 
		--	substr(first_month,1,6)>='202001'
		)a
		right join -- 每月日配业务履约金额大于0	
			(
			select 
				customer_no,substr(sdt,1,6) smonth,
				sum(excluding_tax_sales) excluding_tax_sales,
				sum(excluding_tax_profit) as excluding_tax_profit
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20190101' and sdt<='20210731'
				and channel_code in('1','7','9')
				and business_type_code='2' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_no,substr(sdt,1,6)
			)b on a.customer_no=b.customer_no
		join
			(
			select 
				customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,cooperation_mode_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
				and channel_code in('1','7','9')
				--and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
				and (sales_province_name in ('福建省','安徽省') or city_group_name='重庆主城')
			)c on c.customer_no=b.customer_no
	) as a 
group by
	sales_province_name,
	city_group_name,
	new_smonth,
	smonth,
	diff_month,
	case when diff_month=0 then '新客' when diff_month>0 then '老客' when diff_month='-1' then '首月' else '其他' end,
	cooperation_mode_name
;

--===================================================================================================================================================================================
--城市服务商收入
select 
	province_name,
	city_group_name,
	substr(sdt,1,6) smonth,
	sum(excluding_tax_sales) excluding_tax_sales
from 
	csx_dw.dws_sale_r_d_detail
where 
	sdt>='20200101' and sdt<='20210731'
	and channel_code in('1','7','9')
	and business_type_code='4' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
	and (province_name in ('福建省','安徽省') or city_group_name='重庆主城')
group by 
	province_name,
	city_group_name,
	substr(sdt,1,6)
;

--===================================================================================================================================================================================
--城市服务商数量
select 
	a.s_month,a.province_name,city_group_name,
	count(distinct case when a.s_month=d.create_month then c.supplier_code else null end) cnt,
	count(distinct case when a.s_month!=d.create_month then c.supplier_code else null end) cnt_2,
	sum(a.excluding_tax_sales) excluding_tax_sales,   --含税销售额
	sum(a.excluding_tax_profit) excluding_tax_profit  --含税毛利
from 
	(
    select 
		sdt,split(id, '&')[0] as credential_no,region_code,region_name,province_code,province_name,city_group_code,city_group_name,business_type_name,dc_code,customer_no,
		customer_name,goods_code,goods_name,is_factory_goods_desc,excluding_tax_sales,sales_cost,excluding_tax_profit,front_profit,
		substr(sdt,1,6) as s_month
    from 
		csx_dw.dws_sale_r_d_detail 
    where 
		sdt>='20200101' and sdt<='20210731'
		and channel_code in ('1', '7', '9')
		and business_type_code ='4'
		and sales_type<>'fanli'
		and (province_name in ('福建省','安徽省') or city_group_name='重庆主城')
	)a 
--批次操作明细表
	left join 
		(
		select
			credential_no,
			wms_order_no,
			goods_code
		from 
			csx_dw.dws_wms_r_d_batch_detail
		where 
			sdt >= '20190101'
		group by 
			credential_no, wms_order_no, goods_code
		)b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
	left join 
		(
		select distinct
			supplier_code,
			supplier_name,
			order_code,
			goods_code
		from 
			csx_dw.dws_wms_r_d_entry_detail
		where 
			sdt >= '20190101' or sdt = '19990101'
		)c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
	left join
		(
		select 
			vendor_id,vendor_name,substr(create_date,1,6) as create_month
		from 
			csx_dw.dws_basic_w_a_csx_supplier_m
		where 
			sdt='current'
		) d on d.vendor_id=c.supplier_code
group by 
	a.s_month,a.province_name,a.city_group_name
;			

--===================================================================================================================================================================================
--判断客户俩月下单日期

drop table csx_tmp.tmp_project_trust_customer_bbc;
create table csx_tmp.tmp_project_trust_customer_bbc
as
select
	customer_no,substr(min(sdt),1,6) as first_month
from
	csx_dw.dws_sale_r_d_detail 
where 
	sdt>='20190101' and sdt<='20210731' 
	and business_type_code='6' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
group by 
	customer_no
;


insert overwrite directory '/tmp/zhangyanpeng/20210805_bbc_1' row format delimited fields terminated by '\t' 
--结果表  
--判断各月新客，计算每月新客在之后各月是否有销售
select
	sales_province_name,
	city_group_name,
	new_smonth,
	smonth,
	diff_month,
	case when diff_month=0 then '新客' when diff_month>0 then '老客' when diff_month='-1' then '首月' else '其他' end as type,
	cooperation_mode_name,
	type_2,
	count(distinct customer_no) counts,
	sum(excluding_tax_sales) excluding_tax_sales,
	sum(excluding_tax_profit) as excluding_tax_profit
from
	(
	select 
		c.sales_province_name,
		c.city_group_name,
		a.new_smonth,
		b.smonth,
		floor(months_between(concat(substr(b.smonth,1,4),'-',substr(b.smonth,5,2),'-','01'),concat(substr(a.new_smonth,1,4),'-',substr(a.new_smonth,5,2),'-','01'))) diff_month,
		b.customer_no,
		c.cooperation_mode_name,
		case when a.new_smonth>=d.first_month then 'B端转化' else '非B端转化' end as type_2,
		b.excluding_tax_sales,
		b.excluding_tax_profit
	from
		(
		select 
			customer_no,substr(first_month,1,6) new_smonth
		from
			csx_tmp.tmp_project_trust_customer_bbc
		--where 
		--	substr(first_month,1,6)>='202001'
		)a
		right join -- 每月日配业务履约金额大于0	
			(
			select 
				customer_no,substr(sdt,1,6) smonth,
				sum(excluding_tax_sales) excluding_tax_sales,
				sum(excluding_tax_profit) as excluding_tax_profit
			from 
				csx_dw.dws_sale_r_d_detail
			where 
				sdt>='20190101' and sdt<='20210731'
				and channel_code in('1','7','9')
				and business_type_code='6' -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
				--and sales_type !='fanli'
			group by 
				customer_no,substr(sdt,1,6)
			)b on a.customer_no=b.customer_no
		join
			(
			select 
				customer_no,customer_name,work_no,sales_name,first_category_code,first_category_name,second_category_code,second_category_name,third_category_code,third_category_name,
				sales_region_code,sales_region_name,sales_province_code,sales_province_name,city_group_code,city_group_name,cooperation_mode_name
			from 
				csx_dw.dws_crm_w_a_customer
			where 
				sdt = 'current'
				and channel_code in('1','7','9')
				--and cooperation_mode_code='01' -- 非一次性客户  合作模式编码(01长期客户,02一次性客户)
				and (sales_province_name in ('福建省','安徽省') or city_group_name='重庆主城')
			)c on c.customer_no=b.customer_no
		left join
			(
			select
				customer_no,substr(min(sdt),1,6) as first_month
			from
				csx_dw.dws_sale_r_d_detail 
			where 
				sdt>='20190101' and sdt<='20210731' 
				and channel_code in('1','7','9')
				and business_type_code in ('1','2') -- 业务类型编码(1.日配业务 2.福利业务 3.批发内购 4.城市服务商 5.省区大宗 6.BBC 7.大宗一部 8.大宗二部 9.商超)
			group by 
				customer_no
			) d on d.customer_no=a.customer_no
	) as a 
group by
	sales_province_name,
	city_group_name,
	new_smonth,
	smonth,
	diff_month,
	case when diff_month=0 then '新客' when diff_month>0 then '老客' when diff_month='-1' then '首月' else '其他' end,
	cooperation_mode_name,
	type_2
;


-- 城市服务商业务 缺少供应商

insert overwrite directory '/tmp/zhangyanpeng/20210806_data_1' row format delimited fields terminated by '\t' 

select 
	*
from 
	(
    select 
		sdt,split(id, '&')[0] as credential_no,region_code,region_name,province_code,province_name,city_group_code,city_group_name,business_type_name,dc_code,customer_no,
		customer_name,goods_code,goods_name,is_factory_goods_desc,sales_value,sales_cost,profit,front_profit,
		substr(sdt,1,6) as s_month
    from 
		csx_dw.dws_sale_r_d_detail 
    where 
		sdt>='20200101' and sdt<'20200301'
		and channel_code in ('1', '7', '9')
		and business_type_code ='4'
		and sales_type !='fanli'
		--and region_name in ('华北大区') 
		and province_name='福建省'
	)a 
--批次操作明细表
	left join 
		(
		select
			credential_no,
			wms_order_no,
			goods_code
		from 
			csx_dw.dws_wms_r_d_batch_detail
		where 
			sdt >= '20190101'
		group by 
			credential_no, wms_order_no, goods_code
		)b on b.credential_no = a.credential_no and b.goods_code = a.goods_code
--入库明细
	left join 
		(
		select distinct
			supplier_code,
			supplier_name,
			order_code,
			goods_code
		from 
			csx_dw.dws_wms_r_d_entry_detail
		where 
			sdt >= '20190101' or sdt = '19990101'
		)c on c.order_code = b.wms_order_no and b.goods_code = c.goods_code
	left join
		(
		select 
			vendor_id,vendor_name,substr(create_date,1,6) as create_month
		from 
			csx_dw.dws_basic_w_a_csx_supplier_m
		where 
			sdt='current'
		) d on d.vendor_id=c.supplier_code
where
	c.supplier_code is null
