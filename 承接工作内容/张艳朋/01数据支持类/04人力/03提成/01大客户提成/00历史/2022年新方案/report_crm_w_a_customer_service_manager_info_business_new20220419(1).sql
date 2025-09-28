-- job名称
set mapred.job.name=report_crm_w_a_customer_service_manager_info_business_new;
-- 动态分区
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
-- 来源表
set source_table_name = csx_dw.dws_crm_w_a_customer_sales_link;
-- 目标表
set target_table_name = csx_tmp.report_crm_w_a_customer_service_manager_info_business_new;
-- 昨天日期
set one_day_ago = regexp_replace(date_sub(current_date, 1), '-', '');
-- 昨天月份
set current_month=substr(regexp_replace(date_sub(current_date, 1), '-', ''), 1, 6);
-- 上月1日
set last_month_1_day = regexp_replace(add_months(trunc(date_sub(current_date,1),'MM'),-1),'-','');
set i_sdate_dd =from_utc_timestamp(current_timestamp(),'GMT');

insert overwrite table ${hiveconf:target_table_name} partition (month)
select
  concat_ws('&', a.province_code,a.city_group_code,a.customer_no, ${hiveconf:current_month}) as biz_id,
  a.region_code,
  a.region_name,  
  a.province_code, 
  a.province_name, 
  a.city_group_code, 
  a.city_group_name, 
  a.channel_code, 
  a.channel_name,
  a.customer_id,
  a.customer_no, 
  a.customer_name,
  a.sales_id,
  a.work_no, 
  a.sales_name,
  f.name as user_position,
  case when e.user_position='CUSTOMER_SERVICE_MANAGER' then '' else a.sales_id end sales_id_new,
  case when e.user_position='CUSTOMER_SERVICE_MANAGER' then '' else a.work_no end work_no_new,
  case when e.user_position='CUSTOMER_SERVICE_MANAGER' then '' else a.sales_name end sales_name_new,
  case when e.user_position='CUSTOMER_SERVICE_MANAGER' then '' else f.name end user_position_new,
  a.first_supervisor_code,
  a.first_supervisor_work_no,
  a.first_supervisor_name,
  a.third_supervisor_code,
  a.third_supervisor_work_no,
  a.third_supervisor_name,
  a.fourth_supervisor_code,
  a.fourth_supervisor_work_no,
  a.fourth_supervisor_name,  
  --if(b.user_id is not null, '是', '否') as is_part_time_service_manager,
  a1.rp_service_user_work_no,
  a1.rp_service_user_name,
  a1.rp_service_user_id,
  a1.rp_service_user_position,
  --当销售员岗位是服务管家，若服务管家id为空则=销售员id，若服务管家id包含（等于或包含）销售员id则=服务管家id，否则“销售员id;服务管家id”
  --因销售员离职后无工号，用id判断
  case when e.user_position='CUSTOMER_SERVICE_MANAGER' 
       then if(a1.rp_service_user_id is NULL or a1.rp_service_user_id='',a.work_no,
	           if(a1.rp_service_user_id like concat('%',a.sales_id,'%'),a1.rp_service_user_work_no,
			      concat(a.work_no,';',a1.rp_service_user_work_no)))
       else (case when a1.rp_service_user_position like'%服务管家%' then a1.rp_service_user_work_no end) end rp_service_user_work_no_new,

  case when e.user_position='CUSTOMER_SERVICE_MANAGER' 
       then if(a1.rp_service_user_id is NULL or a1.rp_service_user_id='',a.sales_name,
	           if(a1.rp_service_user_id like concat('%',a.sales_id,'%'),a1.rp_service_user_name,
			      concat(a.sales_name,';',a1.rp_service_user_name)))
       else (case when a1.rp_service_user_position like'%服务管家%' then a1.rp_service_user_name end) end rp_service_user_name_new,

  case when e.user_position='CUSTOMER_SERVICE_MANAGER' 
       then if(a1.rp_service_user_id is NULL or a1.rp_service_user_id='',a.sales_id,
	           if(a1.rp_service_user_id like concat('%',a.sales_id,'%'),a1.rp_service_user_id,
			      concat(a.sales_id,';',a1.rp_service_user_id)))
       else (case when a1.rp_service_user_position like'%服务管家%' then a1.rp_service_user_id end) end rp_service_user_id_new,

  case when e.user_position='CUSTOMER_SERVICE_MANAGER' 
       then if(a1.rp_service_user_id is NULL or a1.rp_service_user_id='',f.name,
	           if(a1.rp_service_user_id like concat('%',a.sales_id,'%'),a1.rp_service_user_position,
			      concat(f.name,';',a1.rp_service_user_position)))
       else (case when a1.rp_service_user_position like'%服务管家%' then a1.rp_service_user_position end) end rp_service_user_position_new,  

  a1.fl_service_user_work_no,
  a1.fl_service_user_name,
  a1.fl_service_user_id,
  a1.fl_service_user_position,
  a1.dz_service_user_work_no,
  a1.dz_service_user_name,
  a1.dz_service_user_id,
  a1.dz_service_user_position,
  a1.bbc_service_user_work_no,
  a1.bbc_service_user_name,
  a1.bbc_service_user_id,
  a1.bbc_service_user_position,
  a1.ng_service_user_work_no,
  a1.ng_service_user_name,
  a1.ng_service_user_id, 
  a1.ng_service_user_position, 
  case when a1.fl_service_user_position like '%服务管家%' then a1.fl_service_user_work_no end fl_service_user_work_no_new,
  case when a1.fl_service_user_position like '%服务管家%' then a1.fl_service_user_name end fl_service_user_name_new,
  case when a1.fl_service_user_position like '%服务管家%' then a1.fl_service_user_id end fl_service_user_id_new,
  case when a1.fl_service_user_position like '%服务管家%' then a1.fl_service_user_position end fl_service_user_position_new,

  case when a1.bbc_service_user_position like '%服务管家%' then a1.bbc_service_user_work_no end bbc_service_user_work_no_new,
  case when a1.bbc_service_user_position like '%服务管家%' then a1.bbc_service_user_name end bbc_service_user_name_new,
  case when a1.bbc_service_user_position like '%服务管家%' then a1.bbc_service_user_id end bbc_service_user_id_new,
  case when a1.bbc_service_user_position like '%服务管家%' then a1.bbc_service_user_position end bbc_service_user_position_new,  
  
  case when c.customer_no is not null then '是' else '否' end is_sale,
  case when d.customer_no is not null then '是' else '否' end is_overdue,
  --e.department_id = '1000000286' 则销售员属于战略部
  if(e.department_id = '1000000286', '是', '否') as is_strategy_department,   
	--福建省日配销售员无论是否有服务管家均按照70 50%，服务管家30 50%   
  case when e.department_id = '1000000286' then 0
       when a.sales_name is NULL or a.sales_name='' then 0
	   when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' then 0.7
	   when a1.rp_service_user_position like'%服务管家%' then 0.7 else 1 end as rp_sales_sale_rate,
  case when e.department_id = '1000000286' then 0
       when a.sales_name is NULL or a.sales_name='' then 0
	   when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' then 0.5
	   when a1.rp_service_user_position like'%服务管家%' then 0.5 else 1 end as rp_sales_profit_rate,	   
  case when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0.3
	   when a1.rp_service_user_position like'%服务管家%' then 0.3 else 0 end as rp_service_user_sale_rate,
  case when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0.5
	   when a1.rp_service_user_position like'%服务管家%' then 0.5 else 0 end as rp_service_user_profit_rate,
	--福建省福利只能有一个人销售员或服务管家，若销售员100%，若服务管家30 50% 
  case when e.department_id = '1000000286' then 0
       when a.sales_name is NULL or a.sales_name='' then 0
	   when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' then 1
       when a1.fl_service_user_position like'%服务管家%' then 0.7 else 1 end as fl_sales_sale_rate,
  case when e.department_id = '1000000286' then 0
       when a.sales_name is NULL or a.sales_name='' then 0
	   when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' then 1
       when a1.fl_service_user_position like'%服务管家%' then 0.5 else 1 end as fl_sales_profit_rate,	   
  case when a.province_code='15' and e.user_position<>'CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' and a1.fl_service_user_position like'%服务管家%' then 0.3
	   when a1.fl_service_user_position like'%服务管家%' then 0.3 else 0 end as fl_service_user_sale_rate,
  case when a.province_code='15' and e.user_position<>'CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' and a1.fl_service_user_position like'%服务管家%' then 0.5
	   when a1.fl_service_user_position like'%服务管家%' then 0.5 else 0 end as fl_service_user_profit_rate,	   
	--福建省BBC只能有一个人销售员或服务管家，若销售员70 50%，若服务管家30 50% 
  case when e.department_id = '1000000286' then 0
       when a.sales_name is NULL or a.sales_name='' then 0
	   when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' then 0.7
	   when a1.bbc_service_user_position like'%服务管家%' then 0.7 else 1 end as bbc_sales_sale_rate,
  case when e.department_id = '1000000286' then 0
       when a.sales_name is NULL or a.sales_name='' then 0
	   when e.user_position='CUSTOMER_SERVICE_MANAGER' then 0
	   when a.province_code='15' then 0.5
       when a1.bbc_service_user_position like'%服务管家%' then 0.5 else 1 end as bbc_sales_profit_rate,	   
  case when a.province_code='15' and e.user_position<>'CUSTOMER_SERVICE_MANAGER' then 0
       when a.province_code='15' and a1.bbc_service_user_position like'%服务管家%' then 0.3
       when a1.bbc_service_user_position like'%服务管家%' then 0.3 else 0 end as bbc_service_user_sale_rate,
  case when a.province_code='15' and e.user_position<>'CUSTOMER_SERVICE_MANAGER' then 0
       when a.province_code='15' and a1.bbc_service_user_position like'%服务管家%' then 0.5
       when a1.bbc_service_user_position like'%服务管家%' then 0.5 else 0 end as bbc_service_user_profit_rate,
  if(a.sales_name like '%A%' or a.sales_name like '%B%' or a.sales_name like '%C%' or a.sales_name like '%M%' 
      or a.sales_name like '%虚拟%' or a.sales_name like '%坏账%' or a.sales_name like '%历史遗留%', '是', '否') as is_sales_xuni,
  ${hiveconf:i_sdate_dd} update_time,
  ${hiveconf:current_month} as month
from
  (
  select 
    channel_code,
    channel_name,
	sales_region_code as region_code,
    sales_region_name as region_name,
    province_code,
    province_name,
    city_group_code,
    city_group_name,
    customer_no,
    customer_name,
    first_category_name,
    second_category_name,
    third_category_name,
    sales_id,
    work_no,
    sales_name,
    first_sign_time,
    sign_time,
    contact_person,
    contact_phone,
    customer_id,
    first_supervisor_code,
    first_supervisor_work_no,
    first_supervisor_name,
    third_supervisor_code,
    third_supervisor_work_no,
    third_supervisor_name,
    fourth_supervisor_code,
    fourth_supervisor_work_no,
    fourth_supervisor_name
  from 
    csx_dw.dws_crm_w_a_customer
  where 
    sdt='current' 
	--sdt=${hiveconf:one_day_ago}
	and customer_no<>''
    --and channel_code in('1','7')
  )a
  --关联服务管家
  left join   
    (
	select 
	  customer_no,
	  concat_ws('', collect_list(rp_service_user_work_no)) as rp_service_user_work_no,
      concat_ws('', collect_list(rp_service_user_name)) as rp_service_user_name,
      concat_ws('', collect_list(rp_service_user_id)) as rp_service_user_id,
	  concat_ws('', collect_list(rp_service_user_position)) as rp_service_user_position,
	  concat_ws('', collect_list(fl_service_user_work_no)) as fl_service_user_work_no,
      concat_ws('', collect_list(fl_service_user_name)) as fl_service_user_name,
      concat_ws('', collect_list(fl_service_user_id)) as fl_service_user_id,
	  concat_ws('', collect_list(fl_service_user_position)) as fl_service_user_position,
	  concat_ws('', collect_list(dz_service_user_work_no)) as dz_service_user_work_no,
      concat_ws('', collect_list(dz_service_user_name)) as dz_service_user_name,
      concat_ws('', collect_list(dz_service_user_id)) as dz_service_user_id,
	  concat_ws('', collect_list(dz_service_user_position)) as dz_service_user_position,
	  concat_ws('', collect_list(bbc_service_user_work_no)) as bbc_service_user_work_no,
      concat_ws('', collect_list(bbc_service_user_name)) as bbc_service_user_name,
      concat_ws('', collect_list(bbc_service_user_id)) as bbc_service_user_id,
	  concat_ws('', collect_list(bbc_service_user_position)) as bbc_service_user_position,
	  concat_ws('', collect_list(ng_service_user_work_no)) as ng_service_user_work_no,
      concat_ws('', collect_list(ng_service_user_name)) as ng_service_user_name,
      concat_ws('', collect_list(ng_service_user_id)) as ng_service_user_id,
	  concat_ws('', collect_list(ng_service_user_position)) as ng_service_user_position
	from
	  (
	  --attribute_code 1:日配客户,2:福利客户,3:大宗贸易,4:M端,5:BBC,6:内购,-1:其他
	  select customer_no,
	    case when attribute_code=1 then service_user_work_no end rp_service_user_work_no,
	    case when attribute_code=1 then service_user_name end rp_service_user_name,
	    case when attribute_code=1 then service_user_id end rp_service_user_id,
		case when attribute_code=1 then service_user_position end rp_service_user_position,
	    case when attribute_code=2 then service_user_work_no end fl_service_user_work_no,
	    case when attribute_code=2 then service_user_name end fl_service_user_name,
	    case when attribute_code=2 then service_user_id end fl_service_user_id,
		case when attribute_code=2 then service_user_position end fl_service_user_position,
	    case when attribute_code=3 then service_user_work_no end dz_service_user_work_no,
	    case when attribute_code=3 then service_user_name end dz_service_user_name,
	    case when attribute_code=3 then service_user_id end dz_service_user_id,	
		case when attribute_code=3 then service_user_position end dz_service_user_position,	
	    case when attribute_code=5 then service_user_work_no end bbc_service_user_work_no,
	    case when attribute_code=5 then service_user_name end bbc_service_user_name,
	    case when attribute_code=5 then service_user_id end bbc_service_user_id,
		case when attribute_code=5 then service_user_position end bbc_service_user_position,
	    case when attribute_code=6 then service_user_work_no end ng_service_user_work_no,
	    case when attribute_code=6 then service_user_name end ng_service_user_name,
	    case when attribute_code=6 then service_user_id end ng_service_user_id,
		case when attribute_code=6 then service_user_position end ng_service_user_position
	  from
	    (
        select 
          customer_no,attribute_code,attribute_name,
          concat_ws(';', collect_list(service_user_work_no)) as service_user_work_no,
          concat_ws(';', collect_list(service_user_name)) as service_user_name,
          concat_ws(';', collect_list(cast(service_user_id as string))) as service_user_id,
		  concat_ws(';', collect_list(c.name)) as service_user_position
        from 
          (
          select 
            distinct customer_no,service_user_work_no,service_user_name,service_user_id,attribute_code,attribute_name
          from 
            ${hiveconf:source_table_name} 
          where 
            sdt = 'current' 
            and is_additional_info = 1 
            and service_user_id <> 0
          )a
         left join
         (
           select *
           from csx_dw.dws_basic_w_a_user 
           where sdt = 'current' 
           and status = 0 and del_flag = '0'	   
         )b on a.service_user_work_no=b.user_number
		 --岗位对应名称
         left join
         (
           select * from csx_tmp.dws_basic_w_a_position_2mysql 	   
         )c on b.user_position=c.code		 
        group by 
          customer_no,attribute_code,attribute_name
	    )a 
		--剔除服务管家位置为非服务管家人员
		--where (fl_service_user_position like '%CUSTOMER_SERVICE_MANAGER%' or fl_service_user_position is NULL)
		--and (dz_service_user_position like '%CUSTOMER_SERVICE_MANAGER%' or dz_service_user_position is NULL) 
		--and (bbc_service_user_position like '%CUSTOMER_SERVICE_MANAGER%' or bbc_service_user_position is NULL) 
		--and (ng_service_user_position like '%CUSTOMER_SERVICE_MANAGER%' or ng_service_user_position is NULL) 
      )a  group by customer_no	
    )a1 on a1.customer_no=a.customer_no
  left join
    (
    select 
      customer_no,first_order_date,last_order_date
    from 
      csx_dw.dws_crm_w_a_customer_active
    where 
      sdt = 'current'
      and last_order_date>=${hiveconf:last_month_1_day}
    )c on a.customer_no = c.customer_no
  --至今有逾期
  left join
    (
    select 
      customer_code as customer_no
    from 
      csx_dw.dws_sss_r_d_customer_settle_detail
    where 
      sdt = ${hiveconf:one_day_ago} 
      and overdue_amount >0
    group by 
      customer_code
    )d on a.customer_no = d.customer_no
  --是否属于战略部、销售员岗位
  left join
    (
    select 
      user_number as work_no,department_id,user_position
    from 
      csx_dw.dws_basic_w_a_user
    where 
      sdt = 'current' 
      --and department_id = '1000000286' 
	  and status = 0 and del_flag = '0'
    )e on a.work_no = e.work_no	
    --岗位对应名称
    left join
    (
      select * from csx_tmp.dws_basic_w_a_position_2mysql 	   
    )f on e.user_position=f.code		
;


/*
---------------------------------------------------------------------------------------------------------
---------------------------------------------hive 建表语句-----------------------------------------------
drop table if exists csx_tmp.report_crm_w_a_customer_service_manager_info_business_new;
CREATE TABLE `csx_tmp.report_crm_w_a_customer_service_manager_info_business_new`(
  `biz_id` string COMMENT '唯一id', 
  `region_code` string COMMENT '大区编码', 
  `region_name` string COMMENT '大区名称', 
  `province_code` string COMMENT '省区编码', 
  `province_name` string COMMENT '省区名称', 
  `city_group_code` string COMMENT '城市编码',  
  `city_group_name` string COMMENT '城市名称', 
  `channel_code` string COMMENT '渠道编码',
  `channel_name` string COMMENT '渠道名称',  
  `customer_id` string COMMENT '客户id', 
  `customer_no` string COMMENT '客户编码',  
  `customer_name` string COMMENT '客户名称', 
  `sales_id` string COMMENT '主销售员Id', 
  `work_no` string COMMENT '销售员工号', 
  `sales_name` string COMMENT '销售员', 
  `user_position` string COMMENT '销售员岗位',  
  `sales_id_new` string COMMENT '主销售员Id_new', 
  `work_no_new` string COMMENT '销售员工号_new', 
  `sales_name_new` string COMMENT '销售员_new', 
  `user_position_new` string COMMENT '销售员岗位_new',  
  `first_supervisor_code` string COMMENT '一级主管编码', 
  `first_supervisor_work_no` string COMMENT '一级主管工号', 
  `first_supervisor_name` string COMMENT '一级主管姓名', 
  `third_supervisor_code` string COMMENT '三级主管编码', 
  `third_supervisor_work_no` string COMMENT '三级主管工号', 
  `third_supervisor_name` string COMMENT '三级主管姓名', 
  `fourth_supervisor_code` string COMMENT '四级主管编码', 
  `fourth_supervisor_work_no` string COMMENT '四级主管工号', 
  `fourth_supervisor_name` string COMMENT '四级主管姓名', 
  `rp_service_user_work_no` string COMMENT '日配_服务管家工号', 
  `rp_service_user_name` string COMMENT '日配_服务管家', 
  `rp_service_user_id` string COMMENT '日配_服务管家id', 
  `rp_service_user_position` string COMMENT '日配_服务管家岗位',
  `rp_service_user_work_no_new` string COMMENT '日配_服务管家工号_new', 
  `rp_service_user_name_new` string COMMENT '日配_服务管家_new', 
  `rp_service_user_id_new` string COMMENT '日配_服务管家id_new', 
  `rp_service_user_position_new` string COMMENT '日配_服务管家岗位_new',
  `fl_service_user_work_no` string COMMENT '福利_服务管家工号', 
  `fl_service_user_name` string COMMENT '福利_服务管家', 
  `fl_service_user_id` string COMMENT '福利_服务管家id', 
  `fl_service_user_position` string COMMENT '福利_服务管家岗位',
  `dz_service_user_work_no` string COMMENT '省区大宗_服务管家工号', 
  `dz_service_user_name` string COMMENT '省区大宗_服务管家', 
  `dz_service_user_id` string COMMENT '省区大宗_服务管家id', 
  `dz_service_user_position` string COMMENT '省区大宗_服务管家岗位',
  `bbc_service_user_work_no` string COMMENT 'BBC_服务管家工号', 
  `bbc_service_user_name` string COMMENT 'BBC_服务管家', 
  `bbc_service_user_id` string COMMENT 'BBC_服务管家id', 
  `bbc_service_user_position` string COMMENT 'BBC_服务管家岗位',
  `ng_service_user_work_no` string COMMENT '内购_服务管家工号', 
  `ng_service_user_name` string COMMENT '内购_服务管家', 
  `ng_service_user_id` string COMMENT '内购_服务管家id', 
  `ng_service_user_position` string COMMENT '内购_服务管家岗位',
  `fl_service_user_work_no_new` string COMMENT '福利_服务管家工号_new', 
  `fl_service_user_name_new` string COMMENT '福利_服务管家_new', 
  `fl_service_user_id_new` string COMMENT '福利_服务管家id_new', 
  `fl_service_user_position_new` string COMMENT '福利_服务管家岗位_new',  
  `bbc_service_user_work_no_new` string COMMENT 'BBC_服务管家工号_new', 
  `bbc_service_user_name_new` string COMMENT 'BBC_服务管家_new', 
  `bbc_service_user_id_new` string COMMENT 'BBC_服务管家id_new', 
  `bbc_service_user_position_new` string COMMENT 'BBC_服务管家岗位_new', 
  `is_sale` string COMMENT '是否有销售',   
  `is_overdue` string COMMENT '是否有逾期', 
  `is_strategy_department` string COMMENT '销售员是否属于战略部', 
  `rp_sales_sale_rate` decimal(20,6) COMMENT '日配销售员_销售额提成比例', 
  `rp_sales_profit_rate` decimal(20,6) COMMENT '日配销售员_毛利提成比例', 
  `rp_service_user_sale_rate` decimal(20,6) COMMENT '日配服务管家_销售额提成比例', 
  `rp_service_user_profit_rate` decimal(20,6) COMMENT '日配服务管家_毛利提成比例',
  `fl_sales_sale_rate` decimal(20,6) COMMENT '福利销售员_销售额提成比例', 
  `fl_sales_profit_rate` decimal(20,6) COMMENT '福利销售员_毛利提成比例', 
  `fl_service_user_sale_rate` decimal(20,6) COMMENT '福利服务管家_销售额提成比例', 
  `fl_service_user_profit_rate` decimal(20,6) COMMENT '福利服务管家_毛利提成比例',  
  `bbc_sales_sale_rate` decimal(20,6) COMMENT 'BBC销售员_销售额提成比例', 
  `bbc_sales_profit_rate` decimal(20,6) COMMENT 'BBC销售员_毛利提成比例', 
  `bbc_service_user_sale_rate` decimal(20,6) COMMENT 'BBC服务管家_销售额提成比例', 
  `bbc_service_user_profit_rate` decimal(20,6) COMMENT 'BBC服务管家_毛利提成比例',
  `is_sales_xuni` string COMMENT '是否虚拟销售员', 
  `update_time` timestamp comment '更新时间'
) COMMENT '客户销售员与服务管家对应表'
PARTITIONED BY (month string COMMENT '日期分区')
STORED AS TEXTFILE;

  

  
  
