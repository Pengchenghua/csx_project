-- 合同迭代更新商机状态20240801

-- 存在的问题：1、在泛微合同号中不同的销售业务类型出现的合同集一样。
-- drop table if exists  csx_analyse_tmp.csx_analyse_tmp_customer_contract;
create table csx_analyse_tmp.csx_analyse_tmp_customer_contract as
select distinct l1.contract_code,
    -- 合同号
    l1.createdate,
    -- 合同创建时间
    (
        case
            when l2.htbh is not null
            and substr(l4.NODENAME, 1, 2) not in ('归档', '10', '9上') then '归档后退回'
            else l4.NODENAME
        end
    ) as last_node_name,
    -- 合同最新节点名称
    (
        case
            when length(l2.customer_no) > 0 then l2.customer_no
            else l1.customer_code
        end
    ) as customer_code,
    -- 客户号
    l3.RECEIVEDATE as lastoperatedate,
    -- 合同最后操作时间
    coalesce(if_htzz,'') as if_htzz,
    -- 合同是否终止
    l2.htqsrq,
    -- 合同起始时间
    l2.htzzrq,
    -- 合同终止时间
     if(l2.htbh is not null ,'是','否') as is_gd,   --判断是否归档 csx_analyse_report_weaver_contract_df 归档合同
    (
        case
            when l2.htzzrq >= '${sdt_yes_date}'   then '是'
            when l2.htzzrq < '${sdt_yes_date}'   then '否'
            else ''
        end
    ) as if_effective -- 合同是否有效 （终止日期大于当前分区日期为有效，当为NULL 时表示合同未生效，当小于当前分区视为无效）
from (
        select REQUESTMARK as contract_code,
            createdate,   -- 合同创建时间
            status as last_node_name,
            substr(requestnamenew, instr(requestnamenew, '客户编码:') + 5, 6) as customer_code,
            concat(lastoperatedate, ' ', lastoperatetime) as lastoperatedate,
            REQUESTID
        from csx_ods.csx_ods_ecology_154_workflow_requestbase_df
        where sdt = '${sdt_yes}'
            and substr(REQUESTMARK, 1, 2) = 'XS'
    ) l1
    left join 
    -- 查询已归档合同数据
    (select
              htbh,
              customer_no,
              htqsrq,
              htzzrq,
              if_htzz
            from
              csx_analyse.csx_analyse_report_weaver_contract_df
            where
              sdt = '${sdt_yes}'
            group by
              htbh,
              customer_no,
              htqsrq,
              htzzrq,
              if_htzz
    ) l2 on l1.contract_code = l2.htbh 
    -- 查询合同最新状态
    left join 
    (
        select ll3.REQUESTID,
            ll3.NODEID,
            ll3.RECEIVEDATE
        from (
                select REQUESTID,
                    NODEID,
                    concat(RECEIVEDATE, ' ', RECEIVETIME) as RECEIVEDATE,
                    row_number() over(
                        partition by REQUESTID
                        order by concat(RECEIVEDATE, ' ', RECEIVETIME) desc
                    ) as pm
                from csx_ods.csx_ods_ecology_154_workflow_currentoperator_df
                where replace(RECEIVEDATE, '-', '') <= '${sdt_yes}'
            ) ll3
        where ll3.pm = 1
    ) l3 on l1.REQUESTID = l3.REQUESTID -- 查询节点名称 
    left join csx_ods.csx_ods_ecology_154_workflow_nodebase_df l4 on l3.NODEID = l4.id
;

-- 商机状态信息
-- drop table csx_analyse_tmp.csx_analyse_tmp_customer_business ;
create table csx_analyse_tmp.csx_analyse_tmp_customer_business as
select 
    m2.* 
from 
    (select 
        m1.*,
        row_number()over(partition by m1.business_attribute_name,m1.customer_code order by m1.create_time desc) as pm 
    from 
        (select 
            a.create_time,
            a.business_number,
            a.customer_code,
            a.contract_must,
            a.contract_type,
            a.business_attribute_code,
            a.contract_begin_date,
            a.contract_end_date,
            a.contract_number,
            c.htqsrq,
            c.htzzrq,
            (case when business_attribute_code=1 then '日配业务' 
                  when business_attribute_code=2 then '福利业务' 
                  when business_attribute_code=3 then '省区大宗' 
                  when business_attribute_code=5 then 'BBC' 
            end) as business_attribute_name,
            c.last_node_name,
            c.lastoperatedate,
            c.is_gd,
            c.if_effective,
            c.if_htzz,
            (case when contract_must=0 then '不签' 
                  when contract_must=1 and contract_type=2 and (c.if_effective='是' and c.if_htzz='否') and substr(c.last_node_name,1,2) in ('10','归档') then '已签' 
                  when contract_must=1 and contract_type=2 and (c.if_effective='否' or c.if_htzz='是') and substr(c.last_node_name,1,2) in ('10','归档') then '失效'  
                --  when contract_must=1 and contract_type=1 and substr(last_node_name,1,2) not in ('10','归档')  then '流程中' 
                  when contract_must=1 and contract_type=1 and   d.contract_code is not null  then '流程中' 
                -- 在“查询合同最新流程节点sql”中此客户合同，则判定为此类型
                  when contract_must=1 and contract_type=1 and c.contract_code is null   then '待签'  
                  
            end) as cus_bus_contract_status ,
            c.createdate
        from csx_dim.csx_dim_crm_business_info a 
        left join 
        (select  
            contract_code,
            last_node_name,
            customer_code,
            lastoperatedate,
            is_gd ,
            htqsrq,
            htzzrq,
            if_effective ,
            if_htzz,
            createdate,
            row_number()over(partition by customer_code order by createdate desc) rn
         from  csx_analyse_tmp.csx_analyse_tmp_customer_contract 
         group by  
            contract_code,
            last_node_name,
            customer_code,
            lastoperatedate,
            is_gd,
            htqsrq,
            htzzrq,
            if_effective,
            if_htzz,
            createdate) c on a.customer_code=c.customer_code  and a.contract_number=c.contract_code
        left join 
        (select * 
        from 
            (select  
            contract_code,
            last_node_name,
            customer_code,
            lastoperatedate,
            is_gd ,
            htqsrq,
            htzzrq,
            if_effective ,
            if_htzz,
            createdate,
            row_number()over(partition by customer_code order by createdate desc) rn
         from  csx_analyse_tmp.csx_analyse_tmp_customer_contract 
         group by  
            contract_code,
            last_node_name,
            customer_code,
            lastoperatedate,
            is_gd,
            htqsrq,
            htzzrq,
            if_effective,
            if_htzz,
            createdate
            ) a where rn=1
            and   (if_effective is null or if_effective='')
            and if_htzz !='是'
            and substr(last_node_name,1,2) not in ('10','归档')
        ) d on a.customer_code=d.customer_code  
        where sdt='current'  
        and business_stage = 5  
        and business_attribute_code in (1,2,3,5) 
        and regexp_replace(substr(create_time,1,10),'-','')<='20240731' 
        ) m1 
    ) m2 
where m2.pm=1 

;




drop table  csx_analyse_tmp.csx_analyse_tmp_report_sale_weaver_contract_df;

-- insert overwrite table csx_analyse.csx_analyse_report_sale_weaver_contract_df partition (sdt) 
create table csx_analyse_tmp.csx_analyse_tmp_report_sale_weaver_contract_df as 
-- 当年各业务类型相关数据
select 
    a.business_type_code,
    a.business_type_name,
    (case when b.performance_region_name is not null then b.performance_region_name else a.performance_region_name end) as performance_region_name,
    (case when b.performance_province_name is not null then b.performance_province_name else a.performance_province_name end) as performance_province_name,
    (case when b.performance_city_name is not null then b.performance_city_name else a.performance_city_name end) as performance_city_name,
    a.customer_code,
    (case when b.customer_name is not null then b.customer_name else a.customer_name end) as customer_name,
    b.second_category_name,
    b.sales_user_number,
    b.sales_user_name,
    (case when d.table_type_name='永辉' then '是' else '否' end) as if_jz,
    (case when b.cooperation_mode_name='一次性客户' then '是' else '否' end) as if_ycx,
    f.first_business_sale_date,
    a.year_first_business_sale_date,
    a.sale_amt,
    a.profit,
    a.profitlv,
    (case when l.customer_code is not null then '有合同' else '无合同' end) as if_hav_con,
    substr('${sdt_yes}',1,4) as year,
    (case when g.overdue_amount_all>0 then '是' else '否' end) as if_overdue,
    (case when h.customer_code is not null then '是' else '否' end) as if_dy,
    a.sign_company_code,
    a.sign_company_name,
    coalesce(m.contract_code, l.contract_code) contract_code,
    coalesce(m.last_node_name,l.last_node_name) last_node_name,
    coalesce(m.lastoperatedate,l.lastoperatedate) lastoperatedate,
    cus_bus_contract_status,
    m.business_number,
    '${sdt_yes}' as sdt  
from 
(select 
    customer_code,
    business_type_code,
    business_type_name,
    max(performance_region_name) as performance_region_name,
    max(performance_province_name) as performance_province_name,
    max(performance_city_name) as performance_city_name,
    max(customer_name) as customer_name,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit,
    sum(profit)/sum(sale_amt) as profitlv,
    min(sdt) as year_first_business_sale_date,
    concat_ws(',',collect_list(distinct sign_company_code)) as sign_company_code,
    concat_ws(',',collect_list(distinct sign_company_name)) as sign_company_name   
from csx_dws.csx_dws_sale_detail_di  
where substr(sdt,1,4)=substr('${sdt_yes}',1,4)  
and sdt<='${sdt_yes}' 
and channel_code in ('1','7','9') 
and business_type_code in (1,2,5,6) 
and performance_province_name not like '%平台%'
group by 
    customer_code,
    business_type_code,
    business_type_name 
) a 
left join 
(select * 
from csx_dim.csx_dim_crm_customer_info 
where sdt='current') b 
on a.customer_code=b.customer_code 
left join 
(
  select company_code as code,
         company_name as name,
         table_type,
         case when table_type='1' then '彩食鲜'
                  when table_type='2' then '永辉'
                  else '其他' end table_type_name
  from csx_dim.csx_dim_basic_company 
  where sdt ='current'
)d 
on b.sign_company_code = d.code 
left join 
(select customer_code,2b_first_order_date,2b_last_order_date  
 from csx_dws.csx_dws_crm_customer_active_di
 where sdt='current'
) e 
on a.customer_code=e.customer_code 
left join 
(select 
    customer_code,
    business_type_code,
    first_business_sale_date 
from csx_dws.csx_dws_crm_customer_business_active_di  
where sdt='current' 
and business_type_code in (1,2,5,6)
) f 
on a.customer_code=f.customer_code and a.business_type_code=f.business_type_code  
left join 
-- 查看客户是否逾期
(select 
    customer_code,
    sum(overdue_amount) as overdue_amount_all 
from csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di 
where sdt='${sdt_yes}' 
group by customer_code 
having overdue_amount_all>0
) g 
on a.customer_code=g.customer_code 
left join 
-- 查看客户是否断约
(select 
    customer_code,
    (case when business_attribute_code=1 then 1 
          when business_attribute_code=2 then 2 
          when business_attribute_code=5 then 6 
          when business_attribute_code=3 then 5 end) as business_attribute_code,
    max(status) as status  
from csx_dim.csx_dim_crm_terminate_customer 
where sdt='current' 
and is_valid=1 
and business_attribute_code in (1,2,3,5) 
and status=2 
group by customer_code,
    (case when business_attribute_code=1 then 1 
          when business_attribute_code=2 then 2 
          when business_attribute_code=5 then 6 
          when business_attribute_code=3 then 5 end)
) h 
on a.customer_code=h.customer_code and a.business_type_code=h.business_attribute_code   
left join 
-- 查看客户合同最新状态
(select 
    l5.customer_code,
    concat_ws(',',collect_list(l5.contract_code)) as contract_code,
    concat_ws(',',collect_list(l5.last_node_name)) as last_node_name, 
    concat_ws(',',collect_list(l5.lastoperatedate)) as lastoperatedate 
 from    csx_analyse_tmp.csx_analyse_tmp_customer_contract l5  
 where l5.if_htzz='否' 
    and l5.if_effective in ('是','') 
 group by l5.customer_code 
) l 
on a.customer_code=l.customer_code 
left join 
csx_analyse_tmp.csx_analyse_tmp_customer_business m 
on a.customer_code=m.customer_code and a.business_type_name=m.business_attribute_name 

union all 

-- 平台业绩客户数据
select 
    a.business_type_code,
    a.business_type_name,
    (case when b.performance_region_name is not null then b.performance_region_name else a.performance_region_name end) as performance_region_name,
    (case when b.performance_province_name is not null then b.performance_province_name else a.performance_province_name end) as performance_province_name,
    (case when b.performance_city_name is not null then b.performance_city_name else a.performance_city_name end) as performance_city_name,
    a.customer_code,
    (case when b.customer_name is not null then b.customer_name else a.customer_name end) as customer_name,
    b.second_category_name,
    b.sales_user_number,
    b.sales_user_name,
    (case when d.table_type_name='永辉' then '是' else '否' end) as if_jz,
    (case when b.cooperation_mode_name='一次性客户' then '是' else '否' end) as if_ycx,
    f.first_business_sale_date,
    a.year_first_business_sale_date,
    a.sale_amt,
    a.profit,
    a.profitlv,
    (case when l.customer_code is not null then '有合同' else '无合同' end) as if_hav_con,
    substr('${sdt_yes}',1,4) as year,
    (case when g.overdue_amount_all>0 then '是' else '否' end) as if_overdue,
    (case when h.customer_code is not null then '是' else '否' end) as if_dy,
    a.sign_company_code,
    a.sign_company_name,
    coalesce(l.contract_code,'') contract_code,
    coalesce(l.last_node_name,'') last_node_name,
    coalesce(l.lastoperatedate,'') lastoperatedate,
    '' as cus_bus_contract_status,
    '' as business_number,
    '${sdt_yes}' as sdt    
from 
(select 
    customer_code,
    0 as business_type_code,
    '平台业绩' as business_type_name,
    max(performance_region_name) as performance_region_name,
    max(performance_province_name) as performance_province_name,
    max(performance_city_name) as performance_city_name,
    max(customer_name) as customer_name,
    sum(sale_amt) as sale_amt,
    sum(profit) as profit,
    sum(profit)/sum(sale_amt) as profitlv,
    min(sdt) as year_first_business_sale_date,
    concat_ws(',',collect_list(distinct sign_company_code)) as sign_company_code,
    concat_ws(',',collect_list(distinct sign_company_name)) as sign_company_name  
from csx_dws.csx_dws_sale_detail_di  
where substr(sdt,1,4)=substr('${sdt_yes}',1,4)  
and sdt<='${sdt_yes}' 
and performance_province_name like '%平台%'
group by 
    customer_code) a 
left join 
(select * 
from csx_dim.csx_dim_crm_customer_info 
where sdt='current') b 
on a.customer_code=b.customer_code 
left join 
(
  select company_code as code,
         company_name as name,
         table_type,
         case when table_type='1' then '彩食鲜'
                  when table_type='2' then '永辉'
                  else '其他' end table_type_name
  from csx_dim.csx_dim_basic_company 
  where sdt ='current'
)d 
on b.sign_company_code = d.code 
left join 
(select customer_code,2b_first_order_date,2b_last_order_date  
 from csx_dws.csx_dws_crm_customer_active_di
 where sdt='current'
) e 
on a.customer_code=e.customer_code 
left join 
(select 
    customer_code,
    min(sdt) as first_business_sale_date 
from csx_dws.csx_dws_sale_detail_di  
where performance_province_name like '%平台%'
group by 
    customer_code) f 
on a.customer_code=f.customer_code 
left join 
-- 查看客户是否逾期
(select 
    customer_code,
    sum(overdue_amount) as overdue_amount_all 
from csx_dws.csx_dws_sss_customer_credit_invoice_bill_settle_stat_di 
where sdt='${sdt_yes}' 
group by customer_code 
having overdue_amount_all>0
) g 
on a.customer_code=g.customer_code 
left join 
-- 查看客户是否断约
(select 
    customer_code,
    max(status) as status  
from csx_dim.csx_dim_crm_terminate_customer 
where sdt='current' 
and is_valid=1 
and status=2 
group by customer_code
) h 
on a.customer_code=h.customer_code 
left join 
-- 查看客户合同最新状态
(select 
    l5.customer_code,
    concat_ws(',',collect_list(l5.contract_code)) as contract_code,
    concat_ws(',',collect_list(l5.last_node_name)) as last_node_name, 
    concat_ws(',',collect_list(l5.lastoperatedate)) as lastoperatedate 
 from  csx_analyse_tmp.csx_analyse_tmp_customer_contract l5  
 where l5.if_htzz='否' and l5.if_effective='是' 
 group by l5.customer_code 
) l 
on a.customer_code=l.customer_code 
; 