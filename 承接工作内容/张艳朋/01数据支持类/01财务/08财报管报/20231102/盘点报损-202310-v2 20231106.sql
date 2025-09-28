--  自动同步近2月过账时间数据，无需单独同步，月初需告知同步
-- 无需等关账，月初同步3张表csx_ods.source_cas_r_d_accounting_credential_item,csx_ods.source_cas_r_d_accounting_credential_header，csx_ods.source_sync_r_d_data_sync_broken_item
--以上前两张表默认同步近两个月，第三张表同步全量


-- /*业务盘点*/
-- 盘点未过账
drop table if exists csx_analyse_tmp.csx_analyse_tmp_pandian_post_no; 
create table csx_analyse_tmp.csx_analyse_tmp_pandian_post_no
as
select
	a.credential_no,
	b.province_name,   --省区
	b.city_name,   --城市
	if(substr(a.purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(a.purchase_group_code,1,1)='A','食百','易耗品')) division_name,   --部类
	a.create_time,   --创建时间
	a.wms_order_time,   --订单时间
	a.posting_time,   --记账日期
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	a.location_name,   --DC名称
	a.reservoir_area_code,   --库区编码
	a.reservoir_area_name,   --库区名称
	a.purchase_group_code,   --课组编码
	a.purchase_group_name,   --课组名称
	a.product_code,   --商品编码
	regexp_replace(a.product_name,'\n|\t|\r','') as product_name,   --商品名称
	c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,c.classify_small_name,
	a.move_type,   --移动类型编码
	a.move_name,   --移动类型名称
	(case when a.direction='+' then -1*a.qty else a.qty end) as qty,   --盘点数量
	(case when a.direction='+' then -1*a.amt_no_tax else a.amt_no_tax end) as amt_no_tax,   --盘点不含税金额
	(case when a.direction='+' then -1*a.amt else a.amt end) as amt   --盘点含税金额
from
	(
	select 
		* 
	from 
		-- csx_ods.source_cas_r_d_accounting_credential_item 
		csx_ods.csx_ods_csx_b2b_accounting_accounting_credential_item_di
	where 
		sdt>='19990101' 
		and to_date(posting_time) >= add_months(trunc(date_sub(current_date,0),'MM'),-1)
		and to_date(posting_time) < trunc(date_sub(current_date,0),'MM')
		and substr(move_type,1,3) in ('110','111') 
		and substr(reservoir_area_code,1,2)='PD' 
		and substr(company_code,1,1)<>'C' -- 20221101新加 公司代码 限制不为城市服务商
	) a
	left join 
		(
		select 
			shop_code,shop_name,province_code,province_name,city_code,city_name
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current' 
		) b on b.shop_code=a.location_code
	left join 
		(
		select 
			*  
		from  
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where 
			sdt = 'current'
		)c on c.goods_code = a.product_code;

select * from csx_analyse_tmp.csx_analyse_tmp_pandian_post_no;
	
-- /*财务盘点*/	
-- 盘点过账	+省区字段
drop table if exists csx_analyse_tmp.csx_analyse_tmp_pandian_post_yes; 
create table csx_analyse_tmp.csx_analyse_tmp_pandian_post_yes
as
select
	a.credential_no,
	b.province_name,   --省区
	b.city_name,   --城市
	if(substr(a.purchase_group_code,1,1) in('U','H'),'生鲜',if(substr(a.purchase_group_code,1,1)='A','食百','易耗品')) division_name,   --部类	
	a.create_time,   --创建时间 
	a.wms_order_time,   --订单时间
	a.posting_time,   --记账日期
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	a.location_name,   --DC名称
	a.reservoir_area_code,   --库区编码
	a.reservoir_area_name,   --库区名称
	a.purchase_group_code,   --课组编码
	a.purchase_group_name,   --课组名称
	a.product_code,   --商品编码
	regexp_replace(a.product_name,'\n|\t|\r','') as product_name,   --商品名称
	c.classify_large_code,c.classify_large_name,c.classify_middle_code,c.classify_middle_name,c.classify_small_code,c.classify_small_name,
	a.move_type,   --移动类型编码
	a.move_name,   --移动类型名称
	(case when a.direction='-' then -1*a.qty else a.qty end) as qty,   --盘点数量
	(case when a.direction='-' then -1*a.amt_no_tax else a.amt_no_tax end) as amt_no_tax,   --盘点不含税金额
	(case when a.direction='-' then -1*a.amt else a.amt end) as amt   --盘点含税金额
from
	(
	select 
		* 
	from 
		-- csx_ods.source_cas_r_d_accounting_credential_item 
		csx_ods.csx_ods_csx_b2b_accounting_accounting_credential_item_di
	where 
		sdt>='19990101'  
		and posting_time >= add_months(trunc(date_sub(current_date,0),'MM'),-1)
		and posting_time < trunc(date_sub(current_date,0),'MM')
		and substr(move_type,1,3) in ('115','116') 
		and amt_no_tax<>'0'	
		and substr(company_code,1,1)<>'C' -- 20221101新加 公司代码 限制不为城市服务商
	) a
	left join 
		(
		select 
			shop_code,shop_name,province_code,province_name,city_code,city_name
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m
			csx_dim.csx_dim_shop
		where 
			sdt = 'current'
		) b on b.shop_code=a.location_code
	left join 
		(
		select 
			*  
		from  
			-- csx_dw.dws_basic_w_a_csx_product_m 
			csx_dim.csx_dim_basic_goods
		where sdt = 'current'
		)c on c.goods_code = a.product_code;
		
select * from csx_analyse_tmp.csx_analyse_tmp_pandian_post_yes

-- /*盘点总金额=业务盘点（食百+易耗品=课组首字母H\U开头为生鲜，其他食百和易耗品--A和数字开头）PD01（含税）+过账的（所有，不含税）*/

select 
	'业务盘点' pandian_group,*,amt amount
from 
	csx_analyse_tmp.csx_analyse_tmp_pandian_post_no
where 
	reservoir_area_code='PD01'
	and division_name in('食百','易耗品')
union all
select 
	'盘点过账' pandian_group,*,amt_no_tax amount
from 
	csx_analyse_tmp.csx_analyse_tmp_pandian_post_yes
; 


--报损
drop table if exists csx_analyse_tmp.csx_analyse_tmp_baosun; 
create table csx_analyse_tmp.csx_analyse_tmp_baosun
as
  select
	e.province_name,   --省区
	e.city_name,   --城市 
	--a.wms_order_no,
	a.credential_no,   --凭证号
	a.create_time,    --创建时间
	a.wms_order_time,   --订单时间
	a.posting_time,   --记账日期
	a.company_code,   --公司代码
	a.company_name,   --公司名称
	a.location_code,   --DC编码
	a.location_name,   --DC名称
	a.reservoir_area_code,   --库区编码
	a.reservoir_area_name,   --库区名称
	a.purchase_group_code,   --课组编码
	a.purchase_group_name,   --课组名称
	a.goods_code,   --商品编码
	regexp_replace(f.goods_name,'\n|\t|\r','') as product_name,   --商品名称
	f.classify_large_code,f.classify_large_name,f.classify_middle_code,f.classify_middle_name,f.classify_small_code,f.classify_small_name,
	a.move_type_code,   --移动类型编码
	a.move_type_name,   --移动类型名称
	wms_biz_type_code,   --报损类型
	wms_biz_type_name,   --报损名称
	a.cost_center_code,	--成本中心编码
	a.cost_center_name,	--成本中心名称	
	--c.cost_center_code,		--成本中心编码
	--c.cost_center_name,		--成本中心名称
	c.reservoir_area_prop,		--库区属性	
	(case when a.direction_flag='-' then -1*a.qty else a.qty end) as bs_qty,
	(case when a.direction_flag='-' then -1*a.amt_no_tax else a.amt_no_tax end) as bs_amt_no_tax,
	(case when a.direction_flag='-' then -1*a.amt else a.amt end) as bs_amt
from
	(
	select 
		* 
	from 
		-- csx_ods.source_cas_r_d_accounting_credential_item
		-- csx_ods.csx_ods_csx_b2b_accounting_accounting_credential_item_di
		csx_dws.csx_dws_cas_credential_detail_di
	where 
		sdt>='20231201' and sdt <='20231231'
-- 		and to_date(posting_time) >= add_months(trunc(date_sub(current_date,0),'MM'),-1)
-- 		and to_date(posting_time) < trunc(date_sub(current_date,0),'MM')
		and substr(move_type_code,1,3) in ('117')
		and substr(company_code,1,1)<>'C' -- 20221101新加 公司代码 限制不为城市服务商
	) a

	left join 
		(
        select 
            warehouse_code,reservoir_area_code,reservoir_area_name,reservoir_area_attribute reservoir_area_prop
        from 
            -- csx_ods.source_wms_w_a_wms_reservoir_area
			csx_ods.csx_ods_csx_b2b_wms_wms_reservoir_area_df
        where
			sdt=regexp_replace(last_day(add_months(current_date,-1)),'-','')
            and(reservoir_area_attribute='C' or reservoir_area_attribute='Y')
        ) c on a.location_code=c.warehouse_code and a.reservoir_area_code=c.reservoir_area_code  		
	--left join 
	--	(
	--	select 
	--		cost_center_code,cost_center_name
	--	from
	--		(
	--		select 
	--			cost_center_code,cost_center_name,row_number() over(partition by cost_center_code order by update_time desc) as rank 
	--		from 
	--			csx_ods.csx_ods_csx_basic_data_md_cost_center_df
	--		where 
	--			sdt=regexp_replace(date_sub(current_date,1),'-','')
	--		)a
	--	where 
	--		rank=1
	--	) d on d.cost_center_code=a.wms_order_no
	left join 
		(
		select 
			shop_code,shop_name,province_code,province_name,city_code,city_name
		from 
			-- csx_dw.dws_basic_w_a_csx_shop_m 
			csx_dim.csx_dim_shop
		where 
			sdt = 'current'
		) e on e.shop_code=a.location_code	
	left join 
		(
		select 
			*  
		from  
			-- csx_dw.dws_basic_w_a_csx_product_m
			csx_dim.csx_dim_basic_goods			
		where 
			sdt = 'current'
		)f on f.goods_code = a.goods_code
;
select * from csx_analyse_tmp.csx_analyse_tmp_baosun



-- select * 
-- from csx_ods.source_cas_r_d_accounting_credential_item 
--	where sdt='19990101'
--	and to_date(posting_time) >= ${hiveconf:i_sdate_12}
--	and to_date(posting_time) < ${hiveconf:i_sdate_11}
--	and substr(move_type,1,3) in ('117')
--	and credential_no='PZ20200615001449'
--	and wms_order_no='BS200615000010'
--	and product_code='1163327';




----------------------------------------------------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------------------------------------------------	
-- 查数
-- select credential_no,product_code,qty,amt_no_tax,
-- case when substr(move_type,1,3) in ('110','111') and substr(reservoir_area_code,1,2)='PD' then '未过账'
--  when substr(move_type,1,3) in ('115','116') and amt_no_tax<>'0' then '已过账'
--  end leibie,
-- case when substr(move_type,1,3) in ('110','111') and substr(reservoir_area_code,1,2)='PD' then '未过账'
--  --when substr(move_type,1,3) in ('115','116') and amt_no_tax<>'0' then '已过账'
--  when substr(move_type,1,3) in ('115','116')  then '已过账'  
--  end leibie1,* 
-- from csx_ods.source_cas_r_d_accounting_credential_item 
-- where sdt='19990101' 
-- and to_date(posting_time) >= '2021-05-01'
-- and to_date(posting_time) <'2021-06-01'
---- and credential_no in('PZ20210531041765','PZ20210508007205','PZ20210506015418')
-- and location_code='W0BC'
-- and product_code='174';


