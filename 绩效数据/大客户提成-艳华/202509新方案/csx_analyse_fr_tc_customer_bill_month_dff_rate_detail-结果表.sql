-- ******************************************************************** 
-- @功能描述：
-- @创建者： 饶艳华 
-- @创建者日期：2023-06-16 23:52:56 
-- @修改者日期：
-- @修改人：
-- @修改内容：扣减回款金额调整关联关系，-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金额
-- 20240814新增调整福利与BBC联营系数
-- 20241220 签呈新增限制最高回款系数
-- 20251013 管家系数，csx_analyse_customer_sale_service_info_rate_qc_mi 这个表的管家系数已弃用，tmp_tc_business_billmonth_profit_rate_tc判断管家系数，
-- 增加个人开发新客系数\跨业务系数（福利BD的日配提成按50%折算，3、日配BD的BBC和福利提成按50%折算 ，除了CD 类城市）
-- ******************************************************************** 

SET hive.exec.parallel=true;
-- 大幅增加内存配置
SET tez.am.resource.memory.mb = 12384;      -- AM内存16GB
SET tez.task.resource.memory.mb = 8192;     -- 任务内存8GB  
SET hive.tez.container.size = 12288;        -- 容器大小12GB


-- 允许使用正则
SET hive.support.quoted.identifiers=none;
 
-- 目标毛利系数-客户月度毛利
-- drop table if exists csx_analyse_tmp.tmp_tc_cust_profit_month;
-- create temporary table csx_analyse_tmp.tmp_tc_cust_profit_month as

drop table if exists csx_analyse_tmp.tmp_tc_person_profit_total;
create  table csx_analyse_tmp.tmp_tc_person_profit_total
as
 with tmp_tc_cust_profit_month as 
(select 
	-- b.performance_region_code,b.performance_region_name,
	-- b.performance_province_code,b.performance_province_name,
	-- b.performance_city_code,b.performance_city_name,
	a.customer_no as customer_code,
	b.customer_name,
	a.smt as smonth,
	c.sales_id,
	c.work_no,
	c.sales_name,
	c.rp_sales_id,
  	c.rp_sales_number,
  	c.rp_sales_name,
  	c.rp_sales_position,
  	c.fl_sales_id,
  	c.fl_sales_number,
  	c.fl_sales_name,
  	c.fl_sales_position,
  	c.bbc_sales_id,
  	c.bbc_sales_number,
  	c.bbc_sales_name,
  	c.bbc_sales_position,
	c.rp_service_user_id,
	c.rp_service_user_work_no,
	c.rp_service_user_name,
	c.fl_service_user_id,
	c.fl_service_user_work_no,
	c.fl_service_user_name,		
	c.bbc_service_user_id,
	c.bbc_service_user_work_no,
	c.bbc_service_user_name,
	-- 各类型销售额
	d.sale_amt,
	d.rp_sale_amt,
	(d.bbc_sale_amt_ly+d.bbc_sale_amt_zy) bbc_sale_amt,
	d.bbc_sale_amt_ly as bbc_ly_sale_amt,
	d.bbc_sale_amt_zy as bbc_zy_sale_amt,
	d.fl_sale_amt,
	-- 各类型定价毛利额
	-- 个人实际毛利额核算时福利及联营bbc业务按照1.2系数上浮
	case when e.flag='1' then d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly*e.adjust_rate+d.fl_profit*e.adjust_rate	
		when a.province_code in ('11','901') then (d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly+d.fl_profit)
		when  a.city_group_code in ('7') then (d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly+d.fl_profit)
		else (d.rp_profit+d.bbc_profit_zy+d.bbc_profit_ly*1.2+d.fl_profit*1.2) 
	end as profit,
	d.rp_profit,
	case when e.flag='1' then (d.bbc_profit_ly*e.adjust_rate+d.bbc_profit_zy)
		when a.province_code in ('11','901') then d.bbc_profit_ly+d.bbc_profit_zy
		when  a.city_group_code in ('7') then d.bbc_profit_ly+d.bbc_profit_zy
		else (d.bbc_profit_ly*1.2+d.bbc_profit_zy)
	end as bbc_profit,
	case when e.flag='1' then d.bbc_profit_ly*e.adjust_rate
		when a.province_code in ('11','901') then d.bbc_profit_ly
		when  a.city_group_code in ('7') then	 d.bbc_profit_ly
		else (d.bbc_profit_ly*1.2)
	end  as bbc_ly_profit,
	d.bbc_profit_zy as bbc_zy_profit,
	case when e.flag='1' then d.fl_profit*e.adjust_rate
		when a.province_code in ('11','901') then d.fl_profit
		when  a.city_group_code in ('7') then d.fl_profit
		else (d.fl_profit*1.2)
	end  as fl_profit
from
-- 客户对应的销售员、客服经理	
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)a
left join  
	(
	-- 本月销售额毛利额 毛利目标达成用签呈后的
	select *
	from csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business a
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	-- 算基准毛利额达成系数时，剔除特定福建监狱客户
	and	a.customer_code not in('105150','105156','105164','105165','105177','105181','105182','106423','106721','107404','119990')
	and a.customer_code not in(
			select customer_code
			from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
			where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
			and category_second like '%不参与人员基准毛利额达成计算%'	
		)	
	)d on a.customer_no=d.customer_code
left join 
	(
	select 
		distinct customer_id,customer_code,customer_name,sales_user_number,sales_user_name,
		performance_region_code,performance_region_name,performance_province_code,performance_province_name,performance_city_code,performance_city_name,
		-- case when channel_code='9' then '业务代理' end as ywdl_cust,
		case when (customer_name like '%内%购%' or customer_name like '%临保%') then '内购' end as ng_cust
	from csx_dim.csx_dim_crm_customer_info 
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	and customer_type_code=4
	and shipper_code='YHCSX'
	)b on b.customer_code=a.customer_no
left join 
	-- 判断是否调整福利与BBC联营按照系数调整
	(
	select customer_code,
		adjust_rate,
		'1' as flag -- 1代表调整福利与BBC联营
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整福利与BBC联营%'	
	) e on a.customer_no=e.customer_code
-- 关联对应各月销售员
join		
	(  
	select *
	-- from csx_analyse.csx_analyse_customer_sale_service_info_rate_use_mi
	from csx_analyse_tmp.csx_analyse_customer_sale_service_info_rate_qc_mi
	where smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''), 1, 6)
	)c on c.customer_no=a.customer_no	
where b.ng_cust is null
)

-- 毛利额汇总-销售员与客服经理：未处理多管家情况-顿号隔开

select 
	a.smonth,
	a.sales_id,
	a.work_no,
	a.sales_name,
	sum(a.sale_amt) as sale_amt, -- 客户总销售额
	sum(a.profit) as profit-- 客户总定价毛利额
from 
(
	-- 使用Lateral View和数组展开来替代多个UNION ALL
	select 
		t.smonth,
		t.customer_code,
		t.customer_name,
		sales_info.sales_id,
		sales_info.work_no,
		sales_info.sales_name,
		sales_info.sale_amt,
		sales_info.profit
	from tmp_tc_cust_profit_month t
	lateral view explode(
		array(
			-- rp service user records
			case when t.rp_service_user_work_no <> '' then 
				named_struct('sales_id', t.rp_service_user_id, 'work_no', t.rp_service_user_work_no, 'sales_name', t.rp_service_user_name, 'sale_amt', t.rp_sale_amt, 'profit', t.rp_profit)
			end,
			-- fl service user records
			case when t.fl_service_user_work_no <> '' then 
				named_struct('sales_id', t.fl_service_user_id, 'work_no', t.fl_service_user_work_no, 'sales_name', t.fl_service_user_name, 'sale_amt', t.fl_sale_amt, 'profit', t.fl_profit)
			end,
			-- bbc service user records
			case when t.bbc_service_user_work_no <> '' then 
				named_struct('sales_id', t.bbc_service_user_id, 'work_no', t.bbc_service_user_work_no, 'sales_name', t.bbc_service_user_name, 'sale_amt', t.bbc_sale_amt, 'profit', t.bbc_profit)
			end,
			-- rp sales records
			case when t.rp_sales_number <> '' then 
				named_struct('sales_id', t.rp_sales_id, 'work_no', t.rp_sales_number, 'sales_name', t.rp_sales_name, 'sale_amt', t.rp_sale_amt, 'profit', t.rp_profit)
			end,
			-- fl sales records
			case when t.fl_sales_number <> '' then 
				named_struct('sales_id', t.fl_sales_id, 'work_no', t.fl_sales_number, 'sales_name', t.fl_sales_name, 'sale_amt', t.fl_sale_amt, 'profit', t.fl_profit)
			end,
			-- bbc sales records
			case when t.bbc_sales_number <> '' then 
				named_struct('sales_id', t.bbc_sales_id, 'work_no', t.bbc_sales_number, 'sales_name', t.bbc_sales_name, 'sale_amt', t.bbc_sale_amt, 'profit', t.bbc_profit)
			end
		)
	) sales_array as sales_info
	where sales_info.sales_id is not null 
		and sales_info.work_no is not null 
		and sales_info.work_no <> ''
)a
group by 
	a.smonth,
	a.sales_id,a.work_no,a.sales_name
	
;

 
	

 -- 毛利额汇总-销售员与客服经理：多管家-拆分到单人
DROP TABLE IF EXISTS csx_analyse_tmp.tmp_tc_person_profit_total_split;
CREATE TABLE csx_analyse_tmp.tmp_tc_person_profit_total_split AS
WITH sales_data AS (
    -- 处理单个管家的情况
    SELECT 
        size(split(t.sales_id,'、')) as count_person,
        t.smonth,
        t.sales_id,
        t.work_no,
        t.sales_name,
        t.sale_amt,
        t.profit
    FROM csx_analyse_tmp.tmp_tc_person_profit_total t
    WHERE size(split(t.sales_id,'、'))=1
    
    UNION ALL
    
    -- 处理多个管家的情况
    SELECT 
        size(split(t.sales_id,'、')) as count_person,
        t.smonth,
        split_sales_id as sales_id,
        split_work_no as work_no,
        split_sales_name as sales_name,
        t.sale_amt,
        t.profit
    FROM csx_analyse_tmp.tmp_tc_person_profit_total t
    LATERAL VIEW posexplode(split(t.sales_id,'、')) s1 AS pos1, split_sales_id
    LATERAL VIEW posexplode(split(t.work_no,'、')) s2 AS pos2, split_work_no
    LATERAL VIEW posexplode(split(t.sales_name,'、')) s3 AS pos3, split_sales_name
    WHERE size(split(t.sales_id,'、'))>1
    AND pos1=pos2 AND pos2=pos3
),
profit_basic_data AS (
    SELECT *
    FROM csx_analyse.csx_analyse_tc_sales_service_profit_basic_mf
    WHERE smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
    AND smt_c=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
),
employee_data AS (
    SELECT 
        employee_code,
        begin_date,
        CASE WHEN begin_date>=regexp_replace(trunc(add_months('${sdt_yes_date}',-12),'MM'),'-','') 
             THEN '是' ELSE '否' END as begin_less_1year_flag
    FROM csx_dim.csx_dim_basic_employee
    WHERE sdt='current'
    AND card_type='0'
)
SELECT
    a.smonth,
    a.sales_id,
    a.work_no,
    a.sales_name,
    sum(a.sale_amt/a.count_person) sale_amt,
    sum(a.profit/a.count_person) profit,
    d.profit_basic,
    e.begin_date,
    e.begin_less_1year_flag,
    round(sum(a.profit/a.count_person)/d.profit_basic,6) as profit_target_rate
FROM sales_data a
LEFT JOIN profit_basic_data d ON d.work_no=a.work_no 
    AND d.smt=substr(regexp_replace(add_months('${sdt_yes_date}',-1),'-',''),1,6)
LEFT JOIN employee_data e ON a.work_no=e.employee_code
WHERE coalesce(a.sales_id,'')<>''
GROUP BY 
    a.smonth,
    a.sales_id,
    a.work_no,
    a.sales_name,
    d.profit_basic,
    e.begin_date,
    e.begin_less_1year_flag;



-- 创建人员信息表，获取销售员和客服经理的城市，因为存在一个业务员名下客户跨城市的情况
drop table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info;
create  table csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info
as
select distinct a.user_id,a.user_number,a.user_name,
	b.performance_city_code,
	b.performance_city_name,
	b.performance_province_code,
	b.performance_province_name,
	b.performance_region_code,
	b.performance_region_name
from
	(
	select 	user_id,user_number,user_name,user_position,city_name,province_name
	from csx_dim.csx_dim_uc_user
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	-- and status = 0 
	and delete_flag = '0'
	) a
	left join -- 区域表
	( 
	select distinct
		city_code,city_name,
		province_code,province_name,
		performance_city_code,
		performance_city_name,
		performance_province_code,
		performance_province_name,
		performance_region_code,
		performance_region_name
	from csx_dim.csx_dim_sales_area_belong_mapping
	where sdt=regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
	) b on b.city_name=a.city_name and b.province_name=a.province_name;
	
	
	
-- 目标毛利系数-销售员与客服经理
-- 过度期内入职时间超过1年的个人基准毛利率低于100%的按照实际率核算; 湖北8月过渡期，9月生效；其他省区都是8-10月过渡期，11月生效
drop table if exists csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc;
create  table csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc
as
select *,
	case 
		when a.work_no in('81131450','81133021','80952742','80010438') then 1
        when a.work_no in('81196948') and substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) between '202506' and '202605' then if(profit_target_rate<1,profit_target_rate,1)
	else (case 	
		when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate<1 then profit_target_rate
		when coalesce(begin_less_1year_flag,'否')='是' and profit_target_rate>=1 then 1
		when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate<1 then 0
		when coalesce(begin_less_1year_flag,'否')='否' and profit_target_rate>=1 then 1
		else 1 end) end as profit_target_rate_tc	
	-- coalesce(if(profit_target_rate>=1,1,profit_target_rate),1) as profit_target_rate_tc			
from 
(
	select
		a.smonth,
		a.sales_id,a.work_no,a.sales_name,
		-- 多管家毛利额达成排序中的最大值，就高原则
		case when size(profit_details) > 0 and arr[size(arr)-1]=profit_details[0].profit_target_rate then profit_details[0].begin_date
			 when size(profit_details) > 1 and arr[size(arr)-1]=profit_details[1].profit_target_rate then profit_details[1].begin_date
			 when size(profit_details) > 2 and arr[size(arr)-1]=profit_details[2].profit_target_rate then profit_details[2].begin_date
			 when size(profit_details) > 3 and arr[size(arr)-1]=profit_details[3].profit_target_rate then profit_details[3].begin_date
			 when size(profit_details) > 0 then profit_details[0].begin_date
			 else null
			 end as begin_date,	
		case when size(profit_details) > 0 and arr[size(arr)-1]=profit_details[0].profit_target_rate then profit_details[0].begin_less_1year_flag
			 when size(profit_details) > 1 and arr[size(arr)-1]=profit_details[1].profit_target_rate then profit_details[1].begin_less_1year_flag
			 when size(profit_details) > 2 and arr[size(arr)-1]=profit_details[2].profit_target_rate then profit_details[2].begin_less_1year_flag
			 when size(profit_details) > 3 and arr[size(arr)-1]=profit_details[3].profit_target_rate then profit_details[3].begin_less_1year_flag
			 when size(profit_details) > 0 then profit_details[0].begin_less_1year_flag
			 else null
			 end as begin_less_1year_flag,	
		
		case when size(profit_details) > 0 and arr[size(arr)-1]=profit_details[0].profit_target_rate then profit_details[0].profit_basic
			 when size(profit_details) > 1 and arr[size(arr)-1]=profit_details[1].profit_target_rate then profit_details[1].profit_basic
			 when size(profit_details) > 2 and arr[size(arr)-1]=profit_details[2].profit_target_rate then profit_details[2].profit_basic
			 when size(profit_details) > 3 and arr[size(arr)-1]=profit_details[3].profit_target_rate then profit_details[3].profit_basic
			 when size(profit_details) > 0 then profit_details[0].profit_basic
			 else null
			 end as profit_basic,
		case when size(profit_details) > 0 and arr[size(arr)-1]=profit_details[0].profit_target_rate then profit_details[0].profit
			 when size(profit_details) > 1 and arr[size(arr)-1]=profit_details[1].profit_target_rate then profit_details[1].profit
			 when size(profit_details) > 2 and arr[size(arr)-1]=profit_details[2].profit_target_rate then profit_details[2].profit
			 when size(profit_details) > 3 and arr[size(arr)-1]=profit_details[3].profit_target_rate then profit_details[3].profit
			 else 0
			 end as profit,
		case when size(profit_details) > 0 and arr[size(arr)-1]=profit_details[0].profit_target_rate then profit_details[0].profit_target_rate
			 when size(profit_details) > 1 and arr[size(arr)-1]=profit_details[1].profit_target_rate then profit_details[1].profit_target_rate
			 when size(profit_details) > 2 and arr[size(arr)-1]=profit_details[2].profit_target_rate then profit_details[2].profit_target_rate
			 when size(profit_details) > 3 and arr[size(arr)-1]=profit_details[3].profit_target_rate then profit_details[3].profit_target_rate
			 else 0
			 end as profit_target_rate
	from 
	(
	select 
		a.smonth,
		a.sales_id,a.work_no,a.sales_name,
		a.sale_amt,
		a.profit,
        -- 收集所有管家信息
        collect_list(
            named_struct(
                'sale_amt', coalesce(b.sale_amt, 0),
                'profit', coalesce(b.profit, 0),
                'profit_basic', b.profit_basic,
                'profit_target_rate', coalesce(b.profit_target_rate, 0),
                'begin_date', b.begin_date,
                'begin_less_1year_flag', b.begin_less_1year_flag
            )
        ) as profit_details,
        -- 多个管家的毛利额达成排序
        sort_array(
            collect_list(coalesce(b.profit_target_rate, 0))
        ) as arr
	from csx_analyse_tmp.tmp_tc_person_profit_total a 
    left join csx_analyse_tmp.tmp_tc_person_profit_total_split b 
        on array_contains(split(a.sales_id, '、'), b.sales_id)
	group by 
        a.smonth,
        a.sales_id,a.work_no,a.sales_name,
        a.sale_amt,
        a.profit
	)a
)a
left join csx_analyse_tmp.tmp_tc_cust_salary_detail_person_info b on split(a.sales_id,'、')[0] =b.user_id
;

 

-- 销售月金额与毛利率
drop table if exists csx_analyse_tmp.tmp_tc_customer_sale_profit_ls;
create  table csx_analyse_tmp.tmp_tc_customer_sale_profit_ls
as
WITH special_customers AS (
    SELECT customer_code, category_second
    FROM csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
    WHERE smt=SUBSTR(REGEXP_REPLACE(LAST_DAY(ADD_MONTHS('${sdt_yes_date}',-1)),'-',''),1,6)
    AND smt_date=SUBSTR(REGEXP_REPLACE(LAST_DAY(ADD_MONTHS('${sdt_yes_date}',-1)),'-',''),1,6)
    AND (category_second LIKE '%纳入大客户提成计算：项目供应商%' 
         OR category_second LIKE '%纳入大客户提成计算：前置仓%'
         OR category_second='纳入大客户提成计算')
),
sales_data AS (
    SELECT 
        a.customer_code,
        SUBSTR(sdt,1,6) AS smonth,
        -- 各类型销售额
        SUM(sale_amt) AS sale_amt,
        SUM(CASE WHEN business_type_code IN ('1','4','5') THEN sale_amt ELSE 0 END) AS rp_sale_amt,
        SUM(CASE WHEN business_type_code IN('6') AND (operation_mode_code=0 OR operation_mode_code IS NULL) THEN sale_amt ELSE 0 END) AS bbc_sale_amt_zy,
        SUM(CASE WHEN business_type_code IN('6') AND operation_mode_code=1 THEN sale_amt ELSE 0 END) AS bbc_sale_amt_ly,
        SUM(CASE WHEN business_type_code IN('2','10') THEN sale_amt ELSE 0 END) AS fl_sale_amt,
        -- 各类型定价毛利额
        SUM(profit) AS profit, 
        SUM(CASE WHEN business_type_code IN ('1','4','5') THEN profit ELSE 0 END) AS rp_profit,
        SUM(CASE WHEN business_type_code IN('6') AND (operation_mode_code=0 OR operation_mode_code IS NULL) THEN profit ELSE 0 END) AS bbc_profit_zy,
        SUM(CASE WHEN business_type_code IN('6') AND operation_mode_code=1 THEN profit ELSE 0 END) AS bbc_profit_ly,        
        SUM(CASE WHEN business_type_code IN('2','10') THEN profit ELSE 0 END) AS fl_profit
    FROM csx_dws.csx_dws_sale_detail_di a
    LEFT JOIN special_customers sc ON a.customer_code=sc.customer_code
    WHERE sdt>='20220601' 
        AND shipper_code='YHCSX'
        AND sdt<REGEXP_REPLACE(LAST_DAY(ADD_MONTHS('${sdt_yes_date}',-1)),'-','')
        AND channel_code IN('1','7','9')
        AND order_channel_detail_code NOT IN ('24','28')
        AND goods_code NOT IN ('8718','8708','8649','840509')
        AND (
            (a.business_type_code IN ('1','2','6','10') AND a.partner_type_code NOT IN (1, 3))
            OR (business_type_code IN('2','5') AND performance_province_name = '平台-B')
            OR (partner_type_code IN (1, 3) AND a.customer_code IN ('131309','178875','126690','127923','129026','129000','229290','175709','125092'))
            OR (partner_type_code IN (1, 3) AND sc.customer_code IS NOT NULL AND 
                (sc.category_second LIKE '%纳入大客户提成计算：项目供应商%' 
                 OR sc.category_second LIKE '%纳入大客户提成计算：前置仓%'))
            OR (sc.customer_code IS NOT NULL AND sc.category_second='纳入大客户提成计算')
        )
        AND (performance_province_name !='福建省' OR (performance_province_name='福建省' AND inventory_dc_name NOT LIKE '%V2DC%'))
    GROUP BY a.customer_code, SUBSTR(sdt,1,6)
)
SELECT 
    customer_code,
    smonth,
    sale_amt,
    rp_sale_amt,
    bbc_sale_amt_zy,
    bbc_sale_amt_ly,
    fl_sale_amt,
    profit,
    rp_profit,
    bbc_profit_zy,
    bbc_profit_ly,
    fl_profit,
    CASE WHEN ABS(sale_amt) > 0 THEN profit/ABS(sale_amt) ELSE 0 END AS prorate,
    CASE WHEN ABS(rp_sale_amt) > 0 THEN rp_profit/ABS(rp_sale_amt) ELSE 0 END AS rp_prorate,
    CASE WHEN ABS(bbc_sale_amt_zy) > 0 THEN bbc_profit_zy/ABS(bbc_sale_amt_zy) ELSE 0 END AS bbc_prorate_zy,
    CASE WHEN ABS(bbc_sale_amt_ly) > 0 THEN bbc_profit_ly/ABS(bbc_sale_amt_ly) ELSE 0 END AS bbc_prorate_ly,
    CASE WHEN ABS(fl_sale_amt) > 0 THEN fl_profit/ABS(fl_sale_amt) ELSE 0 END AS fl_prorate
FROM sales_data
;


-- 签呈处理：部分订单按实际回款时间系数计算（给预付款标签）
drop table if exists csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2;
create  table csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2
as
select a.*,
	case 
		 -- 按实际回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e.date_star and e.date_end) and e.category_second is not null 
		 and (substr(e.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e.adjust_business_type='全业务') then '是' 
		 -- 按实际回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f.date_star and f.date_end) and f.category_second is not null 
		 and (substr(f.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f.adjust_business_type='全业务') then '是' 

		 -- 指定回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e2.date_star and e2.date_end) and e2.category_second is not null 
		 and (substr(e2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e2.adjust_business_type='全业务') then '是' 
		 -- 指定回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f2.date_star and f2.date_end) and f2.category_second is not null 
		 and (substr(f2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f2.adjust_business_type='全业务') then '是' 		 
		 else '否' end yufu_flag,

	case 
		 -- 指定回款时间系数 发生日期
		 when (regexp_replace(substr(a.happen_date,1,10),'-','') between e2.date_star and e2.date_end) and e2.category_second is not null 
		 and (substr(e2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or e2.adjust_business_type='全业务') then e2.hk_dff_rate 
		 -- 指定回款时间系数 打款日期
		 when (regexp_replace(substr(a.paid_date,1,10),'-','') between f2.date_star and f2.date_end) and f2.category_second is not null 
		 and (substr(f2.adjust_business_type,1,2)=substr(a.business_type_name,1,2) or f2.adjust_business_type='全业务') then f2.hk_dff_rate 
		 -- 指定客户最高系数
		when ( (f3.adjust_business_type=a.business_type_name or f3.adjust_business_type='全业务' or f3.adjust_business_type=substr(a.business_type_name,1,2) )
		        and f3.adju_flag=1	and dff_rate>=1 ) then f3.hk_dff_rate
		 else dff_rate end dff_rate_new
from
(
	select *
	from csx_analyse_tmp.csx_analyse_fr_tc_customer_credit_order_detail
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and status=1
)a
-- 部分订单按实际回款时间系数
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-发生日期%'		
	)e on a.customer_code=e.customer_code 
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按实际回款时间系数-打款日期%'		
	)f on a.customer_code=f.customer_code	
-- 部分订单按指定回款时间系数	
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按指定回款时间系数-发生日期%'		
	)e2 on a.customer_code=e2.customer_code 
left join 
		(
	select customer_code,smt_date as smonth,category_first,category_second,
		adjust_business_type,date_star,date_end,cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%部分订单按指定回款时间系数-打款日期%'		
	)f2 on a.customer_code=f2.customer_code	
left join 
-- 限制回款最高系数
	(
	select customer_code,
		smt_date as smonth,
		category_first,
		category_second,
		adjust_business_type,
		date_star,
		date_end,
		'1' adju_flag,
		cast(hk_date_dff_rate as decimal(20,6)) hk_dff_rate
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整回款最高系数%'		
	)f3 on a.customer_code=f3.customer_code	
;	
	

-- 客户+结算月+回款时间系数：各业务类型毛利率提成比例
drop table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_part1;
create table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_part1 as 
with tmp_city_porfit as (
  SELECT
    substr(sdt, 1, 6) s_month,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    sum(sale_amt) sale_amt,
    sum(a.profit) profit,
    case
      when abs(sum(a.sale_amt)) = 0 then 0
      else sum(a.profit) / abs(sum(a.sale_amt))
    end as profit_rate
  from
    csx_dws.csx_dws_sale_detail_di a
  where
    sdt >= '20220101'
    and sdt <= regexp_replace(
      last_day(add_months('${sdt_yes_date}', -1)),
      '-',
      ''
    )
    and channel_code != '2'
    and business_type_code = '1'
    and partner_type_code not in (1, 3)
    and goods_code not in ('8718', '8708', '8649', '840509')
  group by
    substr(sdt, 1, 6),
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name
),
tmp_tc_customer_credit_order_detail_2 as (
  select
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_code,
    customer_name,
    credit_code,
    company_code,
    account_period_code,
    account_period_name,
    sales_id,
    work_no,
    sales_name,
    rp_sales_id,
    rp_sales_number,
    rp_sales_name,
    fl_sales_id,
    fl_sales_number,
    fl_sales_name,
    bbc_sales_id,
    bbc_sales_number,
    bbc_sales_name,
    rp_service_user_id,
    rp_service_user_work_no,
    rp_service_user_name,
    fl_service_user_id,
    fl_service_user_work_no,
    fl_service_user_name,
    bbc_service_user_id,
    bbc_service_user_work_no,
    bbc_service_user_name,
    rp_sales_fp_rate,
    fl_sales_sale_fp_rate  fl_sales_fp_rate,
    bbc_sales_sale_fp_rate bbc_sales_fp_rate,
    substr(regexp_replace(bill_date, '-', ''), 1, 6) as bill_month,
    bill_date,
    paid_date,
    yufu_flag,
    substr(regexp_replace(happen_date, '-', ''), 1, 6) as happen_month,
    dff_rate_new as dff_rate,
    sum(pay_amt) pay_amt,
    sum(
      case
        when business_type_code in (1, 4, 5) then pay_amt
        else 0
      end
    ) as rp_pay_amt,
    sum(
      case
        when business_type_name like 'BBC%' then pay_amt
        else 0
      end
    ) as bbc_pay_amt,
    sum(
      case
        when business_type_name = 'BBC联营' then pay_amt
        else 0
      end
    ) as bbc_ly_pay_amt,
    sum(
      case
        when business_type_name = 'BBC自营' then pay_amt
        else 0
      end
    ) as bbc_zy_pay_amt,
    sum(
      case
        when business_type_code in(2, 10) then pay_amt
        else 0
      end
    ) as fl_pay_amt,
    sum(sale_amt) as sale_amt,
    sum(
      case
        when business_type_code in (1, 4, 5) then sale_amt
        else 0
      end
    ) as rp_sale_amt,
    sum(
      case
        when business_type_name like 'BBC%' then sale_amt
        else 0
      end
    ) as bbc_sale_amt,
    sum(
      case
        when business_type_name = 'BBC联营' then sale_amt
        else 0
      end
    ) as bbc_ly_sale_amt,
    sum(
      case
        when business_type_name = 'BBC自营' then sale_amt
        else 0
      end
    ) as bbc_zy_sale_amt,
    sum(
      case
        when business_type_code in(2, 10) then sale_amt
        else 0
      end
    ) as fl_sale_amt,
    sum(profit) as profit,
    sum(
      case
        when business_type_code in (1, 4, 5) then profit
        else 0
      end
    ) as rp_profit,
    sum(
      case
        when business_type_name like 'BBC%' then profit
        else 0
      end
    ) as bbc_profit,
    sum(
      case
        when business_type_name = 'BBC联营' then profit
        else 0
      end
    ) as bbc_ly_profit,
    sum(
      case
        when business_type_name = 'BBC自营' then profit
        else 0
      end
    ) as bbc_zy_profit,
    sum(
      case
        when business_type_code in(2, 10) then profit
        else 0
      end
    ) as fl_profit
  from
    csx_analyse_tmp.tmp_tc_customer_credit_order_detail_2 a
  group by
    region_code,
    region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    a.customer_code,
    customer_name,
    credit_code,
    company_code,
    account_period_code,
    account_period_name,
    sales_id,
    work_no,
    sales_name,
    rp_sales_id,
    rp_sales_number,
    rp_sales_name,
    fl_sales_id,
    fl_sales_number,
    fl_sales_name,
    bbc_sales_id,
    bbc_sales_number,
    bbc_sales_name,
    rp_service_user_id,
    rp_service_user_work_no,
    rp_service_user_name,
    fl_service_user_id,
    fl_service_user_work_no,
    fl_service_user_name,
    bbc_service_user_id,
    bbc_service_user_work_no,
    bbc_service_user_name,
    rp_sales_fp_rate,
    fl_sales_sale_fp_rate ,
    bbc_sales_sale_fp_rate ,
    substr(regexp_replace(bill_date, '-', ''), 1, 6),
    bill_date,
    paid_date,
    yufu_flag,
    substr(regexp_replace(happen_date, '-', ''), 1, 6),
    dff_rate_new
)
select
  a.region_code,
  a.region_name,
  a.province_code,
  a.province_name,
  a.city_group_code,
  a.city_group_name,
  a.customer_code,
  a.customer_name,
  a.credit_code,
  a.company_code,
  a.account_period_code,
  a.account_period_name,
  a.sales_id,
  a.work_no,
  a.sales_name,
  a.rp_sales_id,
  a.rp_sales_number,
  a.rp_sales_name,
  a.fl_sales_id,
  a.fl_sales_number,
  a.fl_sales_name,
  a.bbc_sales_id,
  a.bbc_sales_number,
  a.bbc_sales_name,
  a.rp_service_user_id,
  a.rp_service_user_work_no,
  a.rp_service_user_name,
  a.fl_service_user_id,
  a.fl_service_user_work_no,
  a.fl_service_user_name,
  a.bbc_service_user_id,
  a.bbc_service_user_work_no,
  a.bbc_service_user_name,
  a.rp_sales_fp_rate,
  a.fl_sales_fp_rate,
  a.bbc_sales_fp_rate,
  a.bill_month,
  a.bill_date,
  a.paid_date,
  a.yufu_flag,
  a.happen_month,
  a.dff_rate,
  a.pay_amt,
  a.rp_pay_amt,
  a.bbc_pay_amt,
  a.bbc_ly_pay_amt,
  a.bbc_zy_pay_amt,
  a.fl_pay_amt,
  a.sale_amt,
  a.rp_sale_amt,
  a.bbc_sale_amt,
  a.bbc_ly_sale_amt,
  a.bbc_zy_sale_amt,
  a.fl_sale_amt,
  a.profit,
  a.rp_profit,
  a.bbc_profit,
  a.bbc_ly_profit,
  a.bbc_zy_profit,
  a.fl_profit,
  if(b.sale_amt is not null, b.prorate, b2.prorate) as profit_rate,
  if(b.sale_amt is not null, b.rp_prorate, b2.rp_prorate) as rp_profit_rate,
  if(
    b.sale_amt is not null,
    (b.bbc_profit_zy + b.bbc_profit_ly) / abs(b.bbc_sale_amt_zy + b.bbc_sale_amt_ly),
    (b2.bbc_profit_zy + b2.bbc_profit_ly) / abs(b2.bbc_sale_amt_zy + b2.bbc_sale_amt_ly)
  ) as bbc_profit_rate,
  if(
    b.sale_amt is not null,
    b.bbc_prorate_zy,
    b2.bbc_prorate_zy
  ) as bbc_zy_profit_rate,
  if(
    b.sale_amt is not null,
    b.bbc_prorate_ly,
    b2.bbc_prorate_ly
  ) as bbc_ly_profit_rate,
  if(b.sale_amt is not null, b.fl_prorate, b2.fl_prorate) as fl_profit_rate,
  if(b.sale_amt is not null, b.sale_amt, b2.sale_amt) as sale_amt_real,
  if(b.sale_amt is not null, b.rp_sale_amt, b2.rp_sale_amt) as rp_sale_amt_real,
  if(
    b.sale_amt is not null,
    b.bbc_sale_amt_zy,
    b2.bbc_sale_amt_zy
  ) as bbc_sale_amt_zy_real,
  if(
    b.sale_amt is not null,
    b.bbc_sale_amt_ly,
    b2.bbc_sale_amt_ly
  ) as bbc_sale_amt_ly_real,
  if(b.sale_amt is not null, b.fl_sale_amt, b2.fl_sale_amt) as fl_sale_amt_real,
  b.service_falg as service_falg,
  b.service_fee as service_fee,
  c.sale_amt as by_sale_amt,
  c.rp_sale_amt as by_rp_sale_amt,
  c.bbc_sale_amt_zy as by_bbc_sale_amt_zy,
  c.bbc_sale_amt_ly as by_bbc_sale_amt_ly,
  c.fl_sale_amt as by_fl_sale_amt,
  c.profit as by_profit,
  c.rp_profit as by_rp_profit,
  c.bbc_profit_zy as by_bbc_profit_zy,
  c.bbc_profit_ly as by_bbc_profit_ly,
  c.fl_profit as by_fl_profit
from
  tmp_tc_customer_credit_order_detail_2 a
  left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business b on a.customer_code = b.customer_code
  and a.happen_month = b.smonth
  left join csx_analyse.csx_analyse_fr_tc_customer_sale_fwf_business c on a.customer_code = c.customer_code
  and c.smonth = substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)), '-', ''),1,6
  )
  left join csx_analyse_tmp.tmp_tc_customer_sale_profit_ls b2 on a.customer_code = b2.customer_code
  and a.happen_month = b2.smonth;
  ;



-- 日配城市平均毛利率
drop table csx_analyse_tmp.tmp_city_porfit_physical;
CREATE TABLE csx_analyse_tmp.tmp_city_porfit_physical
AS
SELECT 
  substr(sdt,1,6) s_month,
  performance_region_name,
  performance_province_name, 
  performance_city_name,
  SUM(sale_amt) sale_amt,
  SUM(profit) profit,
  CASE 
    WHEN ABS(SUM(sale_amt)) = 0 THEN 0 
    ELSE SUM(profit)/ABS(SUM(sale_amt)) 
  END AS profit_rate
FROM csx_dws.csx_dws_sale_detail_di 
WHERE sdt >= '20230101'
  AND sdt <= regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','') -- 使用固定结束日期
  AND channel_code != '2'
  AND business_type_code = '1'
  AND partner_type_code NOT IN (1, 3) 
  AND goods_code NOT IN ('8718','8708','8649','840509')
GROUP BY 
  substr(sdt,1,6),
  performance_region_name,
  performance_province_name,
  performance_city_name
  ;
  
  
-- 客户+结算月+回款时间系数：各业务类型毛利率提成比例 管家系数判断
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc;
create table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
as
with 
special_rules as (
  select smt_date,customer_code,category_second,
    max(case when adjust_business_type in('日配','全业务') then back_amt_tc_rate end) as rp_rate,
    max(case when adjust_business_type in('BBC','BBC自营','全业务') then back_amt_tc_rate end) as bbc_rate_zy,
    max(case when adjust_business_type in('BBC','BBC联营','全业务') then back_amt_tc_rate end) as bbc_rate_ly,
    max(case when adjust_business_type in('福利','全业务') then back_amt_tc_rate end) as fl_rate
  from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
  where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
    and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
    and category_second like'%调整提成比例%'
  group by smt_date,customer_code,category_second
)
select a.*,
  coalesce(e.rp_rate,
    case when rp_profit_rate<0.08 then 0.002
      when rp_profit_rate>=0.08 and rp_profit_rate<0.12 then 0.005
      when rp_profit_rate>=0.12 and rp_profit_rate<0.16 then 0.007
      when rp_profit_rate>=0.16 and rp_profit_rate<0.2 then 0.009
      when rp_profit_rate>=0.2 and rp_profit_rate<0.25 then 0.013
      when rp_profit_rate>=0.25 then 0.015
      else 0.002 
    end
  ) as cust_rp_profit_rate_tc,
  coalesce(e.bbc_rate_zy,
    case when bbc_zy_profit_rate<0.08 then 0.002
      when bbc_zy_profit_rate>=0.08 and bbc_zy_profit_rate<0.12 then 0.005
      when bbc_zy_profit_rate>=0.12 and bbc_zy_profit_rate<0.16 then 0.007
      when bbc_zy_profit_rate>=0.16 and bbc_zy_profit_rate<0.2 then 0.009
      when bbc_zy_profit_rate>=0.2 and bbc_zy_profit_rate<0.25 then 0.013
      when bbc_zy_profit_rate>=0.25 then 0.015
      else 0.002 
    end
  ) as cust_bbc_zy_profit_rate_tc,		
  coalesce(e.bbc_rate_ly,
    case when bbc_ly_profit_rate<0.03 then 0.002
      when bbc_ly_profit_rate>=0.03 and bbc_ly_profit_rate<0.07 then 0.0035
      when bbc_ly_profit_rate>=0.07 and bbc_ly_profit_rate<0.1 then 0.0045
      when bbc_ly_profit_rate>=0.1 and bbc_ly_profit_rate<0.13 then 0.0065
      when bbc_ly_profit_rate>=0.13 and bbc_ly_profit_rate<0.17 then 0.0095
      when bbc_ly_profit_rate>=0.17 and bbc_ly_profit_rate<0.23 then 0.013
      when bbc_ly_profit_rate>=0.23 then 0.015
      else 0.002 
    end
  ) as cust_bbc_ly_profit_rate_tc,
  coalesce(e.fl_rate,
    case when fl_profit_rate<0.03 then 0.002
      when fl_profit_rate>=0.03 and fl_profit_rate<0.07 then 0.0035
      when fl_profit_rate>=0.07 and fl_profit_rate<0.1 then 0.0045
      when fl_profit_rate>=0.1 and fl_profit_rate<0.13 then 0.0065
      when fl_profit_rate>=0.13 and fl_profit_rate<0.17 then 0.0095
      when fl_profit_rate>=0.17 and fl_profit_rate<0.23 then 0.013
      when fl_profit_rate>=0.23 then 0.015
      else 0.002 
    end
  ) as cust_fl_profit_rate_tc,
  case 
    when (a.work_no='' or a.work_no is null) and rp_service_user_work_no is not null then 0.4
    when coalesce(a.rp_profit_rate,0) >= coalesce(d.profit_rate,0) then 0.2
    when coalesce(a.rp_profit_rate,0) < coalesce(d.profit_rate,0)  then 0.1
    else 0 
  end as rp_service_user_fp_rate,
  case 
    when (a.work_no='' or a.work_no is null) and fl_service_user_work_no is not null then 0.4
    when coalesce(a.fl_profit_rate,0) >= coalesce(d.profit_rate,0)  then 0.2
    when coalesce(a.fl_profit_rate,0) < coalesce(d.profit_rate,0)  then 0.1
    else 0  
  end as fl_service_user_fp_rate,
  case 
    when (a.work_no='' or a.work_no is null) and bbc_service_user_work_no is not null then 0.4
    when coalesce(a.bbc_profit_rate,0) >= coalesce(d.profit_rate,0)  then 0.2
    when coalesce(a.bbc_profit_rate,0)  < coalesce(d.profit_rate,0)  then 0.1
    else 0   
  end as bbc_service_user_fp_rate
from csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_part1 a
left join special_rules e on e.customer_code=a.customer_code
left join csx_analyse_tmp.tmp_city_porfit_physical d on a.city_group_name=coalesce(d.performance_city_name,'') and a.happen_month=d.s_month and a.province_name=d.performance_province_name
 
 ;



-- 签呈处理：扣减回款金额
drop table if exists csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1;
create  table csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1
as
select
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号	
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称		
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	a.bill_month, -- 结算月
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.yufu_flag,
	a.happen_month, -- 销售月		
	a.dff_rate,  -- 回款时间系数
	a.pay_amt+nvl(b.adjust_amt,0) as pay_amt,	-- 核销金额
	a.rp_pay_amt+nvl(b.rp_adjust_amt,0) as rp_pay_amt,
	a.bbc_pay_amt+nvl(b.bbc_adjust_amt_zy,0)+nvl(b.bbc_adjust_amt_ly,0) as bbc_pay_amt,
	a.bbc_ly_pay_amt+nvl(b.bbc_adjust_amt_ly,0) as bbc_ly_pay_amt,
	a.bbc_zy_pay_amt+nvl(b.bbc_adjust_amt_zy,0) as bbc_zy_pay_amt,
	a.fl_pay_amt+nvl(b.fl_adjust_amt,0) as fl_pay_amt,

	
	-- 各类型销售额
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt,
	a.bbc_ly_sale_amt,
	a.bbc_zy_sale_amt,
	a.fl_sale_amt,
	-- 各类型定价毛利额
	a.profit,
	a.rp_profit,
	a.bbc_profit,
	a.bbc_ly_profit,
	a.bbc_zy_profit,
	a.fl_profit,	

	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_zy_profit_rate,
	a.bbc_ly_profit_rate,	
	a.fl_profit_rate,

	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,
	
	a.cust_rp_profit_rate_tc,
	a.cust_bbc_zy_profit_rate_tc,			
	a.cust_bbc_ly_profit_rate_tc,
	a.cust_fl_profit_rate_tc	
from 
(
	select *,
		-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金 --202406之前：销售月_回款金额
	-- 销售月_回款金额
	concat(happen_month,'_',cast(pay_amt as decimal(26,2))) as happen_month_pay_amt,
	-- 销售月_打款日期_回款金额,当出现销售月与金额重复里，需要调整代码销售月_打款日期_回款金  202406 202406_202406_0000
	concat(happen_month,'_',regexp_replace(paid_date,'-',''),'_',cast(pay_amt as decimal(26,2))) as happen_paid_month_pay_amt
	from csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc
)a 
left join
(
  select customer_code,
	smt_date as smonth,
	split(remark,'：')[1] as happen_month_pay_amt,  --扣减回款金额：销售月_打款日期_回款金额
	0-adjust_amount as adjust_amt,
	0-case when adjust_business_type='日配' then nvl(adjust_amount,0) end as rp_adjust_amt, 
	0-case when adjust_business_type='BBC自营' then nvl(adjust_amount,0) end as bbc_adjust_amt_zy,
	0-case when adjust_business_type='BBC联营' then nvl(adjust_amount,0) end as bbc_adjust_amt_ly,
	0-case when adjust_business_type='福利' then nvl(adjust_amount,0) end as fl_adjust_amt
  from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%扣减回款金额%'	
)b on a.customer_code=b.customer_code 
	-- 从202406开始出现金额相同，需要组合 ：销售月_打款日期_回款金额
	and  if(a.bill_month<'202406',a.happen_month_pay_amt,happen_paid_month_pay_amt)=b.happen_month_pay_amt 
    -- and  a.happen_month_pay_amt =b.happen_month_pay_amt 
;



DROP TABLE IF EXISTS csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0;

CREATE TABLE csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0
AS
WITH base_data AS (
    SELECT 
        a.*,
        IF(ABS(a.pay_amt) < ABS(a.sale_amt), a.pay_amt, a.sale_amt) AS pay_amt_use,
        d1.profit_basic AS sales_profit_basic,
        d1.profit AS sales_profit_finish,
        d1.profit_target_rate AS sales_target_rate,
        d1.profit_target_rate_tc AS sales_target_rate_tc,
        d2.profit_basic AS rp_service_profit_basic,
        d2.profit AS rp_service_profit_finish,
        d2.profit_target_rate AS rp_service_target_rate,
        d2.profit_target_rate_tc AS rp_service_target_rate_tc,
        d3.profit_basic AS fl_service_profit_basic,
        d3.profit AS fl_service_profit_finish,
        d3.profit_target_rate AS fl_service_target_rate,
        d3.profit_target_rate_tc AS fl_service_target_rate_tc,
        d4.profit_basic AS bbc_service_profit_basic,
        d4.profit AS bbc_service_profit_finish,
        d4.profit_target_rate AS bbc_service_target_rate,
        d4.profit_target_rate_tc AS bbc_service_target_rate_tc,
        COALESCE(f.adjust_rate, 
                 CASE 
                     WHEN j.renew_flag = '1' AND a.happen_month >= '202504' AND happen_month >= j.smt THEN j.renew_cust_rate
                     WHEN e.new_cust_flag = '1' THEN e.new_cust_rate
                     ELSE 1
                 END) AS new_cust_rate
    FROM csx_analyse_tmp.tmp_tc_business_billmonth_profit_rate_tc_1 a 
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d1 ON d1.work_no = a.work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d2 ON d2.work_no = a.rp_service_user_work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d3 ON d3.work_no = a.fl_service_user_work_no
    LEFT JOIN csx_analyse_tmp.tmp_tc_person_profit_target_rate_tc d4 ON d4.work_no = a.bbc_service_user_work_no
    LEFT JOIN (
        SELECT DISTINCT customer_code, 
            if(first_sale_month<'202504' ,1.2,1.0) AS new_cust_rate, 
            '1' new_cust_flag
        FROM (
            SELECT customer_code, substr(MIN(first_business_sale_date), 1, 6) first_sale_month
            FROM csx_dws.csx_dws_crm_customer_business_active_di
            WHERE sdt = 'current' 
              AND business_type_code = '1'
              AND shipper_code = 'YHCSX'
            GROUP BY customer_code
        ) a
        WHERE first_sale_month >= substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -12)), '-', ''), 1, 6)
          AND first_sale_month >= '202308'
    ) e ON a.customer_code = e.customer_code
    LEFT JOIN (
        SELECT smt_date, customer_code, category_second, adjust_rate
        FROM csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
        WHERE smt = substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)), '-', ''), 1, 6)
          AND smt_date = substr(regexp_replace(last_day(add_months('${sdt_yes_date}', -1)), '-', ''), 1, 6)
          AND category_second LIKE '%调整日配新客系数%'
    ) f ON f.customer_code = a.customer_code
    LEFT JOIN (
        -- 老客续签0.8系数,按照终止日期取最早一条
        SELECT customer_code, 
            0.8 AS renew_cust_rate, 
            '1' AS renew_flag,
            smt
        FROM (
            SELECT customer_code, 
                0.8 AS renew_cust_rate, 
                '1' AS renew_flag,
                smt,
                row_number() OVER (PARTITION BY customer_code ORDER BY smt ASC) rn
            FROM csx_analyse.csx_analyse_tc_renew_customer_df
            WHERE smt >= '202504'
              AND htzzrq >= last_day(add_months('${sdt_yes_date}', -1))
              AND row_rn = 1
            GROUP BY customer_code, smt
        ) a 
        WHERE rn = 1
    ) j ON j.customer_code = a.customer_code
),
calculated_data AS (
    SELECT 
        base.*,
        -- 销售员提成计算
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_sales_fp_rate) * new_cust_rate * coalesce(sales_target_rate_tc, 1) AS tc_sales_rp,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_bbc_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_bbc_ly,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_sales_fp_rate) * coalesce(sales_target_rate_tc, 1) AS tc_sales_fl,
        
        -- 管家提成计算
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_service_user_fp_rate) * coalesce(rp_service_target_rate_tc, 1) AS tc_rp_service,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_service_user_fp_rate) * coalesce(fl_service_target_rate_tc, 1) AS tc_fl_service,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service_ly,
        
        -- BBC管家总提成
        ((bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) +
         (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate)) * coalesce(bbc_service_target_rate_tc, 1) AS tc_bbc_service,
        
        -- 原始销售提成（不考虑目标达成率）
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_sales_fp_rate) * new_cust_rate AS original_tc_sales_rp,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_sales_fp_rate) AS original_tc_sales_bbc_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_sales_fp_rate) AS original_tc_sales_bbc_ly,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_sales_fp_rate) AS original_tc_sales_fl,
        
        -- 原始管家提成（不考虑目标达成率）
        (rp_pay_amt * cust_rp_profit_rate_tc * dff_rate * rp_service_user_fp_rate) AS original_tc_rp_service,
        (fl_pay_amt * cust_fl_profit_rate_tc * dff_rate * fl_service_user_fp_rate) AS original_tc_fl_service,
        (bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) AS original_tc_bbc_service_zy,
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) AS original_tc_bbc_service_ly,
        
        -- BBC管家原始总提成
        ((bbc_zy_pay_amt * cust_bbc_zy_profit_rate_tc * dff_rate * bbc_service_user_fp_rate) +
        (bbc_ly_pay_amt * cust_bbc_ly_profit_rate_tc * dff_rate * bbc_service_user_fp_rate)) AS original_tc_bbc_service
    FROM base_data base
)
SELECT 
    *,
    (original_tc_sales_rp + original_tc_sales_bbc_zy + original_tc_sales_bbc_ly + original_tc_sales_fl) AS original_tc_sales,
    (tc_sales_rp + tc_sales_bbc_zy + tc_sales_bbc_ly + tc_sales_fl) AS tc_sales
FROM calculated_data;




drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1;
create  table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1
as
select 
	-- concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.region_code,a.customer_code,a.bill_month,cast(a.dff_rate as string)) biz_id,
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.credit_code,	-- 信控号	
	a.company_code,	-- 签约公司编码
	a.account_period_code,	-- 账期编码
	a.account_period_name,	-- 账期名称		
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_sales_id,
  	a.rp_sales_number,
  	a.rp_sales_name,
  	a.fl_sales_id,
  	a.fl_sales_number,
  	a.fl_sales_name,
  	a.bbc_sales_id,
  	a.bbc_sales_number,
  	a.bbc_sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	a.pay_amt,	-- 核销金额
	a.rp_pay_amt,
	a.bbc_pay_amt,
	a.bbc_ly_pay_amt,
	a.bbc_zy_pay_amt,
	a.fl_pay_amt,		
	
	-- 各类型销售额
	a.sale_amt,
	a.rp_sale_amt,
	a.bbc_sale_amt,
	a.bbc_ly_sale_amt,
	a.bbc_zy_sale_amt,
	a.fl_sale_amt,
	-- 各类型定价毛利额
	a.profit,
	a.rp_profit,
	a.bbc_profit,
	a.bbc_ly_profit,
	a.bbc_zy_profit,
	a.fl_profit,
	a.profit_rate,
	a.rp_profit_rate,
	a.bbc_profit_rate,
	a.bbc_ly_profit_rate,
	a.bbc_zy_profit_rate,
	a.fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,
	
	-- 增加各业务类型提成字段
	-- tc_sales_rp
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.tc_sales_rp,0))
		))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_sales_rp,
	
	-- tc_sales_bbc = tc_sales_bbc_zy + tc_sales_bbc_ly
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_ly,0))
		))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_sales_bbc,
	
	-- tc_sales_fl
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.tc_sales_fl,0))
		))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_sales_fl,
	
	-- 原有tc_sales字段保持不变
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.tc_sales_rp,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.tc_sales_bbc_ly,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.tc_sales_fl,0))
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_sales,
	
	-- 取消打款
	-- if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	
	
	-- 增加字段，计算不考虑毛利目标达成情况的提成
	-- 原始tc_sales_rp
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.original_tc_sales_rp,0))
		))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_sales_rp,
		-- 原始tc_sales_bbc = original_tc_sales_bbc_zy + original_tc_sales_bbc_ly
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_ly,0))
		))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_sales_bbc,
	
	-- 原始tc_sales_fl
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.original_tc_sales_fl,0))
		))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_sales_fl,
	
	-- 原有original_tc_sales字段保持不变
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null and b3.category_second is null and b2.category_second is null,0,
	if(a.service_falg in('服务费','销售员按服务费'),a.service_fee*if((a.pay_amt/a.sale_amt_real)>1,1,if((a.pay_amt/a.sale_amt_real)<-1,-1,(a.pay_amt/a.sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','销售员不算提成'),0,
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,coalesce(a.original_tc_sales_rp,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_zy,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,coalesce(a.original_tc_sales_bbc_ly,0))+
			if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,coalesce(a.original_tc_sales_fl,0))
				))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_sales,
		
	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b1.category_second is null,0,	
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.rp_pay_amt/a.rp_sale_amt_real)>1,1,if((a.rp_pay_amt/a.rp_sale_amt_real)<-1,-1,(a.rp_pay_amt/a.rp_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_rp_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_rp_service,		


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b3.category_second is null,0,		
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee*if((a.fl_pay_amt/a.fl_sale_amt_real)>1,1,if((a.fl_pay_amt/a.fl_sale_amt_real)<-1,-1,(a.fl_pay_amt/a.fl_sale_amt_real)))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_fl_service))
		)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_fl_service,	


	if(a.paid_date<=last_day(add_months('${sdt_yes_date}',-3)) and a.yufu_flag='否' and c.paid_date_new is null and coalesce(e1.account_period_name,'0')<>'预付货款' and b2.category_second is null,0,
	if(a.service_falg in('服务费','管家按服务费'),a.service_fee
		*if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))>1,1,if(((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))<-1,-1,((a.bbc_zy_pay_amt+a.bbc_ly_pay_amt)/(a.bbc_sale_amt_zy_real+a.bbc_sale_amt_ly_real))))*dff_rate,
		if(d.category_second in('不算提成','管家不算提成'),0,
			a.original_tc_bbc_service))
	)*if(d.category_second like'%提成减半%',0.5,1) as original_tc_bbc_service,
	
	
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 		
	
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_0 a
-- 系统账期预付款客户
 -- 信控公司账期 
left join 
(
	select customer_code,credit_code,company_code,
		account_period_code,account_period_name,		-- 账期编码,账期名称
		account_period_value,		-- 账期值
		account_period_abbreviation_name,		-- 账期简称
		credit_limit,temp_credit_limit
		from csx_dim.csx_dim_crm_customer_company_details
		where sdt='current'
		    and shipper_code='YHCSX'
)e1 on a.credit_code=e1.credit_code and a.company_code=e1.company_code
-- 预付款客户
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('日配','全业务')
	)b1 on b1.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('BBC','全业务')
	)b2 on b2.customer_code=a.customer_code
left join
	(
	select smt_date,customer_code,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like'%预付款%'
	and adjust_business_type in('福利','全业务')
	)b3 on b3.customer_code=a.customer_code
-- 调整打款日期：按打款日期对应的实际回款时间系数计算	
left join 
		(
	select customer_code,smt_date as smonth,category_second,adjust_business_type,
	  date_star,date_end,
	  date_format(from_unixtime(unix_timestamp(paid_date_new,'yyyyMMdd')),'yyyy-MM-dd') as paid_date_new 
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and category_second like '%调整打款日期%'		
	)c on a.customer_code=c.customer_code and a.paid_date=c.paid_date_new
left join
	(
	select smt_date,customer_code,
		concat(customer_code,effective_period,remark) as dd,
		category_second,adjust_business_type,service_fee
	from csx_analyse.csx_analyse_tc_customer_special_rules_2023_1mf 
	where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and smt_date=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
	and (category_second like'%不算提成%'
	-- or category_second like'%服务费%'
	or category_second like'%提成减半%')
	)d on d.customer_code=a.customer_code
;





-- 增加跨业务销售提成50%规则
-- drop table if exists csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail;
-- create   table csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail
-- as
with tmp_new_customer_info as
(   select
    sales_user_number,
    sales_user_name,
    user_position_name,
    sales_person_target,	
    city_category_score,
    total_sale_amt,
    case   when   user_position_name in ('销售岗','销售员（旧）','销售员') 
                then  
                    case when (a.total_sale_amt)/10000/(sales_person_target) between 1 and 1.4999 then 1.1
                    when  (a.total_sale_amt)/10000/(sales_person_target)>=1.5 then 1.2
                    else 1 
                    end
             when   user_position_name like '%销售经理%'  then 
                case when (a.total_sale_amt)/10000/(city_category_score) between 1 and 1.4999 then 1.1
                     when (a.total_sale_amt)/10000/(city_category_score)>=1.5 then 1.2
                else 1 end 
        else 1 
        end as sales_coefficient
from 
(select 
    sales_user_number,
    sales_user_name,
    user_position_name,
    sum(a.sale_amt) total_sale_amt,
    max(sales_target) sales_person_target,	
    max(city_manager_target ) city_category_score
from    csx_analyse.csx_analyse_tc_development_customer_info  a 
where  smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
and (business_type_name='日配业务' or other_needs_code='餐卡') and active_new_flag=1
and (user_position_name in ('销售岗','销售员（旧）','销售员')  or user_position_name like '%销售经理%')
group by sales_user_number,
    sales_user_name,
    user_position_name
)a
) ,
tmp_csx_analyse_customer_sale_service_info_rate_qc_mi
as 
(select customer_no,city_type_name 
    from csx_analyse.csx_analyse_customer_sale_service_info_rate_qc_mi
    where smt=substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6)
group by customer_no,city_type_name 
),

tmp_position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
tmp_sales_leader_info as (
  select a.*,b.name as user_position_name,c.name as leader_position_name from 
  (select
    a.user_id,
    a.user_number,
    a.user_name,
    a.source_user_position,
    a.leader_user_id,
    b.user_number as leader_user_number,
    b.user_name as leader_user_name,
    b.source_user_position as leader_user_position
  from
       csx_dim.csx_dim_uc_user a
    left join (
      select
        user_id,
        user_number,
        user_name,
        source_user_position
      from
        csx_dim.csx_dim_uc_user a
      where
        sdt = regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
        and status = 0
    ) b on a.leader_user_id = b.user_id
  where
    sdt = regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-','')
    and status = 0
    )a 
    left join tmp_position_dic b on a.source_user_position=b.code
    left join tmp_position_dic c on a.leader_user_position=c.code
) 
insert overwrite table csx_analyse.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail partition(smt)
select 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)) biz_id,
	
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month, -- 结算月
	cast(a.dff_rate as decimal(20,6)) dff_rate,		
	sum(pay_amt) as pay_amt,	-- 核销金额
	sum(rp_pay_amt) as rp_pay_amt,
	sum(bbc_pay_amt) as bbc_pay_amt,
	sum(bbc_ly_pay_amt) as bbc_ly_pay_amt,
	sum(bbc_zy_pay_amt) as bbc_zy_pay_amt,
	sum(fl_pay_amt) as fl_pay_amt,		
	
	-- 各类型销售额
	sum(a.sale_amt) as sale_amt,
	sum(rp_sale_amt) as rp_sale_amt,
	sum(bbc_sale_amt) as bbc_sale_amt,
	sum(bbc_ly_sale_amt) as bbc_ly_sale_amt,
	sum(bbc_zy_sale_amt) as bbc_zy_sale_amt,
	sum(fl_sale_amt) as fl_sale_amt,
	-- 各类型定价毛利额
	sum(profit) as profit,
	sum(rp_profit) as rp_profit,
	sum(bbc_profit) as bbc_profit,
	sum(bbc_ly_profit) as bbc_ly_profit,
	sum(bbc_zy_profit) as bbc_zy_profit,
	sum(fl_profit) as fl_profit,
	
	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	coalesce(a.cust_rp_profit_rate_tc,0.002) as cust_rp_profit_rate_tc, 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,
	sum(tc_sales)*(case when e.city_type_name not  in ('C','D') then 
	                    case when coalesce(bbc_sale_amt,0)+ coalesce(fl_sale_amt,0)>0 
	                        and d.user_position_name  in ('销售岗','销售员（旧）','销售员','销售经理','高级销售经理')  then 0.5 
	                    when coalesce(rp_sale_amt,0)>0  and d.user_position_name like '%福利%' then 0.5 
	                    else 1  end 
	                else 1 end) *coalesce(sales_coefficient,1) as tc_sales,
	sum(tc_rp_service) as tc_rp_service,		
	sum(tc_fl_service) as tc_fl_service,	
	sum(tc_bbc_service) as tc_bbc_service,
	
	from_utc_timestamp(current_timestamp(),'GMT') update_time,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt_ct,
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,	

	-- 增加字段，计算不考虑毛利目标达成情况的提成
	sum(original_tc_sales) as original_tc_sales,
	sum(original_tc_rp_service) as original_tc_rp_service,		
	sum(original_tc_fl_service) as original_tc_fl_service,	
	sum(original_tc_bbc_service) as original_tc_bbc_service,	
	coalesce(b.sales_coefficient,1) as sales_coefficient,              -- 新客系数
	-- 跨业务系数
	coalesce(case when e.city_type_name not  in ('C','D') then case when coalesce(bbc_sale_amt,0)+ coalesce(fl_sale_amt,0)>0  and d.user_position_name  in ('销售岗','销售员（旧）','销售员','销售经理','高级销售经理')  then 0.5 
	 when coalesce(rp_sale_amt,0)>0  and d.user_position_name like '%福利%' then 0.5 
	  else 1 
	  end 
	 else 1 end,1) as cross_coefficient,              -- 销售员跨业务开客提成50%
	d.user_position_name,
	e.city_type_name,
	substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6) as smt -- 统计日期 
from csx_analyse_tmp.csx_analyse_fr_tc_customer_bill_month_dff_rate_detail_1 a
left join tmp_new_customer_info b on a.work_no=b.sales_user_number
left join tmp_sales_leader_info d on a.work_no=d.user_number   
left join tmp_csx_analyse_customer_sale_service_info_rate_qc_mi e on a.customer_code=e.customer_no
group by 
	concat_ws('-',substr(regexp_replace(last_day(add_months('${sdt_yes_date}',-1)),'-',''),1,6),a.province_code,a.customer_code,
	a.bill_month,a.happen_month,a.bill_date,a.paid_date,cast(a.dff_rate as string)),
	a.region_code,
	a.region_name,
	a.province_code,
	a.province_name,
	a.city_group_code,
	a.city_group_name,
	a.customer_code,	-- 客户编码
	a.customer_name,
	a.sales_id,
	a.work_no,
	a.sales_name,
	a.rp_service_user_id,
	a.rp_service_user_work_no,
	a.rp_service_user_name,
	a.fl_service_user_id,
	a.fl_service_user_work_no,
	a.fl_service_user_name,		
	a.bbc_service_user_id,
	a.bbc_service_user_work_no,
	a.bbc_service_user_name,
	a.bill_month,

	profit_rate,
	rp_profit_rate,
	bbc_profit_rate,
	bbc_ly_profit_rate,
	bbc_zy_profit_rate,
	fl_profit_rate,
	
	cast(a.dff_rate as decimal(20,6)),
	coalesce(a.cust_rp_profit_rate_tc,0.002), 
	a.cust_bbc_zy_profit_rate_tc, 
	a.cust_bbc_ly_profit_rate_tc, 
	a.cust_fl_profit_rate_tc, 
	
	-- 提成分配系数
	a.rp_sales_fp_rate,
	a.fl_sales_fp_rate,
	a.bbc_sales_fp_rate,
	a.rp_service_user_fp_rate,
	a.fl_service_user_fp_rate,
	a.bbc_service_user_fp_rate,	
	-- 目标毛利系数-销售员与客服经理
	a.sales_profit_basic,
	a.sales_profit_finish,
	a.sales_target_rate,
	a.sales_target_rate_tc,
	
	a.rp_service_profit_basic,
	a.rp_service_profit_finish,
	a.rp_service_target_rate,
	a.rp_service_target_rate_tc,
	
	a.fl_service_profit_basic,
	a.fl_service_profit_finish,
	a.fl_service_target_rate,
	a.fl_service_target_rate_tc,
	
	a.bbc_service_profit_basic,
	a.bbc_service_profit_finish,
	a.bbc_service_target_rate,
	a.bbc_service_target_rate_tc,	
	
	a.new_cust_rate,
	
	a.bill_date,  -- 结算日期
	a.paid_date,  -- 核销日期（打款日期）
	a.happen_month, -- 销售月
	-- 历史月销售额
	a.sale_amt_real,
	a.rp_sale_amt_real,
	a.bbc_sale_amt_zy_real,
	a.bbc_sale_amt_ly_real,
	a.fl_sale_amt_real,	
	
	-- 服务费
	a.service_falg,
	a.service_fee,
	-- 本月销售额毛利额
	a.by_sale_amt,
	a.by_rp_sale_amt,
	a.by_bbc_sale_amt_zy,
	a.by_bbc_sale_amt_ly,
	a.by_fl_sale_amt,
	a.by_profit,
	a.by_rp_profit,
	a.by_bbc_profit_zy,
	a.by_bbc_profit_ly,
	a.by_fl_profit,
	b.sales_coefficient,              -- 新客系数
	case when e.city_type_name not  in ('C','D') then 
            case when coalesce(bbc_sale_amt,0)+ coalesce(fl_sale_amt,0)>0  
                  and d.user_position_name  in ('销售岗','销售员（旧）','销售员','销售经理','高级销售经理')  then 0.5 
	                when coalesce(rp_sale_amt,0)>0  and d.user_position_name like '%福利%' then 0.5 
	            else 1 end 
	 else 1 end ,              -- 销售员跨业务开客提成50%
	d.user_position_name,
	e.city_type_name;

