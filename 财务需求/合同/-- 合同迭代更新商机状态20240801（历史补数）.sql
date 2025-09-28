drop table if exists csx_analyse_tmp.csx_analyse_report_sale_weaver_contract_df_${sdt_yes};
create temporary table csx_analyse_tmp.csx_analyse_report_sale_weaver_contract_df_${sdt_yes} as 
select * 
from csx_analyse.csx_analyse_report_sale_weaver_contract_df 
where sdt='${sdt_yes}'
;


-- 合同迭代更新商机状态20240801

-- 存在的问题：1、在泛微合同号中不同的销售业务类型出现的合同集一样。
drop table if exists  csx_analyse_tmp.csx_analyse_tmp_customer_contract_${sdt_yes};
create temporary table csx_analyse_tmp.csx_analyse_tmp_customer_contract_${sdt_yes} as
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
drop table if exists csx_analyse_tmp.csx_analyse_tmp_customer_business_${sdt_yes} ;
create temporary table csx_analyse_tmp.csx_analyse_tmp_customer_business_${sdt_yes} as
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
                  when contract_must=1 and contract_type=2 and (c.if_effective='是' and c.if_htzz='否') and c.is_gd='是' then '已签' 
                  when contract_must=1 and contract_type=2 and (c.if_effective='否' or c.if_htzz='是') and c.is_gd='是' then '失效'  
                --  when contract_must=1 and contract_type=1 and substr(last_node_name,1,2) not in ('10','归档')  then '流程中' 
                  when contract_must=1 and contract_type=1 and d.contract_code is not null  then concat('流程中-',d.last_node_name) 
                -- 在“查询合同最新流程节点sql”中此客户合同，则判定为此类型
                  when contract_must=1 and contract_type=1 and d.contract_code is null   then '待签'  
            else '不签'                   
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
         from  csx_analyse_tmp.csx_analyse_tmp_customer_contract_${sdt_yes} 
         where is_gd='是' 
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
         from  csx_analyse_tmp.csx_analyse_tmp_customer_contract_${sdt_yes} 
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
            and is_gd='否' 
        ) d on a.customer_code=d.customer_code  
        where sdt='current'  
        and business_stage = 5  
        and business_attribute_code in (1,2,3,5) 
        and regexp_replace(substr(create_time,1,10),'-','')<='${sdt_yes}' 
        ) m1 
    ) m2 
where m2.pm=1 
;

insert overwrite table csx_analyse.csx_analyse_report_sale_weaver_contract_df partition (sdt) 

-- 当年各业务类型相关数据
select 
    a.business_type_code,
    a.business_type_name,
    a.performance_region_name,
    a.performance_province_name,
    a.performance_city_name,
    a.customer_code,
    a.customer_name,
    a.second_category_name,
    a.sales_user_number,
    a.sales_user_name,
    a.if_jz,
    a.if_ycx,
    a.first_business_sale_date,
    a.year_first_business_sale_date,
    a.sale_amt,
    a.profit,
    a.profitlv,
    a.if_hav_con,
    a.year,
    a.if_overdue,
    a.if_dy,
    a.sign_company_code,
    a.sign_company_name,
    a.contract_code,
    a.last_node_name,
    a.lastoperatedate,
    nvl(m.cus_bus_contract_status,'不签') as cus_bus_contract_status,
    m.business_number,
    a.sdt
from 
    csx_analyse_tmp.csx_analyse_report_sale_weaver_contract_df_${sdt_yes} a 
    left join 
    csx_analyse_tmp.csx_analyse_tmp_customer_business_${sdt_yes} m 
    on a.customer_code=m.customer_code and a.business_type_name=m.business_attribute_name 