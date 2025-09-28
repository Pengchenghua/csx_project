

--①1、对抵负库存成本调整
--①2、采购退货金额差异调整
--①3、工厂月末分摊-调整销售
--①4、工厂月末分摊-调整跨公司调拨
--①5、工厂月末分摊-调整其他
--①6、手工调整销售成本
-- 7、价量差工厂未使用的商品
-- 8、工厂分摊后成本小于0，未分摊金额
-- 9、报损
--★10、盘盈(盘盈用负数表示，表示减成本）
--★11、盘亏
-- 12、后台收入
-- 13、后台支出

------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_11 =trunc(date_sub(current_date,1),'MM');
set i_sdate_12 =add_months(trunc(date_sub(current_date,1),'MM'),-1);
set i_sdate_13 =add_months(trunc(date_sub(current_date,1),'MM'),-2);

-- 本月第一天，上月第一天，上上月第一天
set i_sdate_21 =regexp_replace(trunc(date_sub(current_date,1),'MM'),'-','');
set i_sdate_22 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_23 =regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-2),'-','');

--select ${hiveconf:i_sdate_11},${hiveconf:i_sdate_12},${hiveconf:i_sdate_13},${hiveconf:i_sdate_21},${hiveconf:i_sdate_22},${hiveconf:i_sdate_23};

drop table csx_tmp.tmp_cbgb_tz_gcfth_0;
create temporary table csx_tmp.tmp_cbgb_tz_gcfth_0 
as 
select  
	case when a.location_code='W0H4' then '-' else b.province_code end province_code,
	case when a.location_code='W0H4' then '供应链' else b.province_name end province_name,
	case when a.location_code='W0H4' then '-' else b.city_code end city_code,
	case when a.location_code='W0H4' then '供应链' else b.city_name end city_name,		
	a.location_code,a.location_name,
	d.dept_id,d.dept_name,
	sum(a.d_cost_subtotal) d_cost_subtotal
from 
	(
	select 
		* 
	from 
		csx_ods.source_mms_r_a_factory_report_diff_apportion_header
		--where sdt='20200606'
	where 
		sdt=regexp_replace(date_sub(current_date,0),'-','')
		and period = substr(${hiveconf:i_sdate_12},1,7)   --'2020-05'
		and notice_status = '3'
	)a
	left join 
		(
		select 
			shop_id,shop_name,sales_province_code province_code,sales_province_name province_name,city_group_code as city_code,city_group_name as city_name
		from 
			csx_dw.dws_basic_w_a_csx_shop_m 
		where 
			sdt = 'current'
		) b on b.shop_id=a.location_code
	left join 
		(
		select 
			goods_id,department_id dept_id,department_name dept_name
		from 
			csx_dw.dws_basic_w_a_csx_product_m 
		where 
			sdt = 'current'
		)d on a.product_code=d.goods_id
	left join 
		(
		select 
			* 
		from 
			csx_dw.dws_basic_w_a_company_code 
		where 
			sdt = 'current' 
			and owner_ship = 'CITY'
		) city_company on a.company_code = city_company.code
where 
	city_company.code is null
group by 
	case when a.location_code='W0H4' then '-' else b.province_code end,
	case when a.location_code='W0H4' then '供应链' else b.province_name end,
	case when a.location_code='W0H4' then '-' else b.city_code end,
	case when a.location_code='W0H4' then '供应链' else b.city_name end,
	a.location_code,a.location_name,
	d.dept_id,d.dept_name;


--数据导入
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--insert overwrite table csx_dw.cbgb_tz_m_cbgb_gcfth partition(sdt)
drop table csx_tmp.cbgb_tz_m_cbgb_gcfth_djg;
create table csx_tmp.cbgb_tz_m_cbgb_gcfth_djg
as 
select
    if(substr(dept_id,1,1) in('U','H'),'生鲜','食百') division_name,   --部类
	case when location_code in('W0R1','W0T6','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5') or (location_code='W0X4' and dept_id not in('H04','H06')) then '是' else '否' end is_daijiagong,	 
	province_code,case when location_code in('E097','E0A2','E104') then '重庆市' else province_name end province_name,city_code,
	case when province_name='重庆市' and (city_name='' or city_name='沙坪坝区') then '重庆主城' 
		 when province_name='福建省' then if(city_name='漳州市','福州市',city_name)
		 when province_name='河南省' then '郑州市'
		 when location_code in('E097','E0A2','E104') then '重庆主城'
	     else city_name end city_name,
	location_code,location_name,dept_id,dept_name,
	cast(d_cost_subtotal as decimal(21,6)) as amount,
	substr(${hiveconf:i_sdate_22},1,6) as sdt
from csx_tmp.tmp_cbgb_tz_gcfth_0
--where location_code in('W0R1','W0T6','W0T3','W0T7','W0M6','W0S8','W0T5','W0X5')
--or (location_code='W0X4' and dept_id not in('H04','H06'))
;




/*
--------------------------------- hive建表语句 -------------------------------
-- csx_dw.cbgb_tz_m_cbgb_gcfth  财报管报调整成本-工厂分摊后成本小于0

drop table if exists csx_dw.cbgb_tz_m_cbgb_gcfth;
create table csx_dw.cbgb_tz_m_cbgb_gcfth(
  `province_code` string COMMENT  '省区编号',
  `province_name` string COMMENT  '省区',
  `city_code` string COMMENT '城市编号',  
  `city_name` string COMMENT '城市',
  `location_code` string COMMENT 'DC编码',
  `location_name` string COMMENT 'DC名称',   
  `dept_id` string COMMENT '课组编号',
  `dept_name` string COMMENT '课组名称', 
  `amount` decimal(26,6) COMMENT '金额'  
) COMMENT '财报管报调整成本-工厂分摊后成本小于0'
PARTITIONED BY (sdt string COMMENT '日期分区')
STORED AS TEXTFILE;

--------------------------------- mysql建表语句 -------------------------------
drop table if exists `cbgb_tz_m_cbgb_gcfth`;
create table `cbgb_tz_m_cbgb_gcfth`(
  `province_code` varchar(64) DEFAULT NULL COMMENT  '省区编号',
  `province_name` varchar(64) DEFAULT NULL COMMENT  '省区',
  `city_code` varchar(64) DEFAULT NULL COMMENT '城市编号',  
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市',
  `location_code` varchar(64) DEFAULT NULL COMMENT 'DC编码',
  `location_name` varchar(64) DEFAULT NULL COMMENT 'DC名称',   
  `dept_id` varchar(64) DEFAULT NULL COMMENT '课组编号',
  `dept_name` varchar(64) DEFAULT NULL COMMENT '课组名称', 
  `amount` decimal(26,6) COMMENT '金额',  
  `sdt` varchar(64) DEFAULT NULL COMMENT '日期分区'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='财报管报调整成本-工厂分摊后成本小于0';



select province_name,
	sum(amount) amount
from csx_dw.cbgb_tz_m_cbgb_gcfth
where sdt='202005'
group by province_name;


*/


