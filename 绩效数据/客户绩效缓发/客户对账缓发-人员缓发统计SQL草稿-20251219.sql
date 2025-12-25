-- drop table csx_analyse_tmp.csx_analyse_tmp_hr_personnel_deduction_delay ;
-- select * from csx_analyse_tmp.csx_analyse_tmp_hr_personnel_deduction_delay 
create table csx_analyse_tmp.csx_analyse_tmp_hr_personnel_deduction_delay as
with 
tmp_position_dic as 
(select dic_key as code,dic_value as name
       from csx_ods.csx_ods_csx_b2b_ucenter_user_dic_df
       where sdt=regexp_replace(date_sub(current_date(),1),'-','')
       and dic_type = 'POSITION'
),
tmp_sales_info as (
  select a.*,
    b.name as user_position_name,
    c.name as leader_position_name ,
    d.begin_date,
    d.position_name,
    d.position_title_name,
    d.employee_status
from 
  (select
    a.user_id,
    a.user_number,
    a.user_name,
    a.source_user_position,
    a.leader_user_id,
    b.user_number as leader_user_number,
    b.user_name as leader_user_name,
    b.source_user_position as leader_user_position,
    province_id,
    province_name,
    city_code,
    city_name
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
        sdt = '${sdt_yes_date}'
        -- and status = 0
    ) b on a.leader_user_id = b.user_id
  where
    sdt =  '${sdt_yes_date}'
    -- and status = 0
    )a 
    left join tmp_position_dic b on a.source_user_position=b.code
    left join tmp_position_dic c on a.leader_user_position=c.code
    left join 
    (select distinct
        employee_code,
        begin_date,
        record_type_name,
        sdt,
        if(employee_status='0','离职','在职')employee_status,
        position_name,
        position_title_name
    from    csx_dim.csx_dim_basic_employee 
        where sdt = '${sdt_yes_date}'
        and card_type=0 
      --  and record_type_code	!=4
    ) d on a.user_number=d.employee_code
) 
-- select * from tmp_sales_info where user_number= '81278914'
,
sales_data AS (
    -- 处理单个管家的情况
    SELECT 
        size(split(t.service_user_work_no,'、')) as count_person,
        t.smt,
        t.service_user_work_no as work_no,
        t.service_user_name as sales_name,
        t.customer_code,
        business_attribute_name
    FROM csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_result_mf t
    
    WHERE size(split(t.service_user_work_no,'、'))=1
        AND smt='202511' 
        AND t.service_user_work_no !=''
        AND t.service_user_work_no is not null 
    UNION ALL
    -- 处理多个管家的情况
    SELECT 
        size(split(t.service_user_work_no,'、')) as count_person,
        t.smt,
        split_work_no as work_no,
        split_sales_name as sales_name,
        t.customer_code,
        business_attribute_name
    FROM csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_result_mf t
    LATERAL VIEW posexplode(split(t.service_user_work_no,'、')) s2 AS pos1, split_work_no
    LATERAL VIEW posexplode(split(t.service_user_name,'、')) s3 AS pos2, split_sales_name
    WHERE size(split(t.service_user_work_no,'、'))>1
        AND pos1=pos2
        AND smt='202511'
        AND t.service_user_work_no!=''
        AND t.service_user_work_no is not null 
    UNION ALL 
    -- 处理销售员
    SELECT 
        size(split(t.sales_user_number,'、')) as count_person,
        t.smt,
        t.sales_user_number as work_no,
        t.sales_user_name as sales_name,
        t.customer_code,
        business_attribute_name
    FROM csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_result_mf t
    WHERE t.sales_user_number !='' 
        AND t.sales_user_number is not null 
        AND smt='202511'
)
select a.performance_region_code,
    a.performance_region_name,
    a.performance_province_code,
    a.performance_province_name,
    a.performance_city_code,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    a.sales_user_number,
    a.sales_user_name,
    a.service_user_work_no,
    a.service_user_name,
    a.business_attribute_name,
    a.tc_bill_type,
    a.tc_bill_type_no_send,
    a.tc_invoice_type,
    a.tc_history_invoice_type_no_send,
    a.tc_delayed_release_result,
    a.tc_suspended_result,
    b.work_no,
    b.sales_name,
    c.user_position_name,
    c.begin_date,
    -- c.position_name,
    -- c.position_title_name,
    c.employee_status,
     province_id,
  province_name,
  city_code,
  city_name
from  csx_analyse.csx_analyse_hr_customer_deferred_reconciliation_invoice_result_mf a 
left  join sales_data b on a.customer_code=b.customer_code and a.business_attribute_name=b.business_attribute_name
left join  tmp_sales_info c on b.work_no=c.user_number
where a.smt='202511'


;

WITH aggregated_data AS (
  SELECT
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    work_no,
    sales_name,
    begin_date,
    user_position_name,
    SUM(CASE WHEN tc_bill_type = '是' THEN 1 ELSE 0 END) AS tc_bill_type_cnt, -- 对账缓发客户数
    SUM(CASE WHEN tc_bill_type_no_send = '是' THEN 1 ELSE 0 END) AS tc_bill_type_no_send_cnt, -- 对账扣发客户数
    SUM(CASE WHEN tc_invoice_type = '是' THEN 1 ELSE 0 END) AS tc_invoice_type_cnt, -- 开票缓发客户数
    SUM(CASE WHEN tc_history_invoice_type_no_send = '是' THEN 1 ELSE 0 END) AS tc_history_invoice_type_no_send_cnt -- 开票账扣发客户数
  FROM
    csx_analyse_tmp.csx_analyse_tmp_hr_personnel_deduction_delay
  GROUP BY
    performance_region_code,
    performance_region_name,
    performance_province_code,
    performance_province_name,
    performance_city_code,
    performance_city_name,
    work_no,
    sales_name,
    begin_date,
    user_position_name
)
SELECT
  *,
  CASE 
    WHEN tc_bill_type_no_send_cnt > 0 OR tc_history_invoice_type_no_send_cnt > 0 THEN '否'
    WHEN tc_bill_type_cnt > 0 OR tc_invoice_type_cnt > 0 THEN '是'
    ELSE '否'
  END AS tc_delayed_release_result,
  CASE 
    WHEN tc_bill_type_no_send_cnt > 0 OR tc_history_invoice_type_no_send_cnt > 0 THEN '是'
    ELSE '否'
  END AS tc_suspended_result
FROM
  aggregated_data;




create table csx_analyse.csx_analyse_tmp_hr_personnel_deduction_delay_reult_mf 
(
region_code	string	 COMMENT '业绩区域编码',
region_name	string COMMENT '业绩区域名称',
province_code string	 COMMENT 'HR省区ID',
province_name	string	 COMMENT 'HR省区名称',
city_code	string COMMENT 'HR城市编码',	
city_name	string COMMENT 'HR城市名称',	
work_no	string COMMENT '人员工号',	
sales_name	string COMMENT '人员姓名',	
begin_date	string COMMENT '人员入职时间',	
user_position_name	string COMMENT '人员职位名称',	
tc_bill_type_cnt	bigint COMMENT '人员对账缓发客户数',	
tc_bill_type_no_send_cnt	bigint COMMENT '人员对账扣发客户数',	
tc_invoice_type_cnt	bigint COMMENT '人员开票缓发客户数',	
tc_history_invoice_type_no_send_cnt	bigint COMMENT '人员开票账扣发客户数',	
tc_delayed_release_result	string COMMENT '人员对账缓发结果',	
tc_suspended_result	string COMMENT '人员对账扣发结果',	
update_time timestamp comment '更新时间',
s_month string COMMENT '月分区'
) comment '人员对账缓发结果表';
 PARTITIONED BY (smt  string comment '分区字段' )
STORED AS parquet;
