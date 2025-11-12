




with tmp_incidental_write_off_df as (
      select
        *,
        case
          when a.purchase_code in('111', '121', '119') then '125'
          else purchase_code
        end new_purchase_code,
        case
          when business_scene_code = '1' then datediff('${ytd_date}', to_date(approved_date))
          when business_scene_code in ('2', '3') then datediff(
            '${ytd_date}',
            coalesce(
              to_date(break_contract_date),
              to_date(approved_date)
            )
          )
          else ''
        end as account_diff,
        if(user_phone is null, employee_code, user_number) responsible_person_number
      from
        csx_ods.csx_ods_csx_b2b_sss_sss_incidental_write_off_df a
        left join (
          select
            user_number,
            user_name,
            user_phone
          from
            csx_dim.csx_dim_uc_user
          where
            sdt = regexp_replace(to_date(date_sub(current_date, 1)), '-', '')
            and user_phone != ''
          group by
            user_number,
            user_name,
            user_phone
        ) b on a.responsible_person_phone = b.user_phone
        left join (
          select
            employee_code,
            employee_name,
            mobile
          from
            csx_ods.csx_ods_csx_basic_data_erp_employee_df
          where
            sdt = regexp_replace(to_date(date_sub(current_date, 1)), '-', '')
            and card_type = '0'
            and mobile != ''
        ) c on a.responsible_person_phone = c.mobile
      where
        sdt = '${ytd}' and a.shipper_code = 'YHCSX'
    ),
tmp_incidental_write_off_sale_df as
  (SELECT
        incidental_expenses_no,
        break_contract,
        sale_remark,
        CASE 
            WHEN real_perform_custom IS NOT NULL AND trim(real_perform_custom) != '' AND trim(real_perform_custom) != '[]' THEN
                concat_ws(',', collect_set(get_json_object(real_perform_custom2, '$.userCode')))
            ELSE ''
        END as real_perform_customer_code
    FROM (
        SELECT
            incidental_expenses_no,
            break_contract,
            sale_remark,
            real_perform_custom,
            real_perform_custom2
        FROM (
            SELECT
                incidental_expenses_no,
                break_contract,
                sale_remark,
                cast(regexp_replace(real_perform_custom, '\\[|\\]', '') as string) as real_perform_custom
            FROM
                csx_ods.csx_ods_csx_b2b_sss_sss_incidental_write_off_sale_df
             WHERE
                sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '') 
                AND shipper_code = 'YHCSX'
                -- AND incidental_expenses_no IN ('4422081700035')
        ) t 
        LATERAL VIEW OUTER explode(
            split(
                CASE 
                    WHEN regexp_replace(real_perform_custom, '\\}\\,\\s*\\{', '\\}\\|\\|\\{') IS NOT NULL 
                    THEN regexp_replace(real_perform_custom, '\\}\\,\\s*\\{', '\\}\\|\\|\\{')
                    ELSE ''
                END,
                '\\|\\|'
            )
        ) t1 as real_perform_custom2
    ) tmp
    GROUP BY
        incidental_expenses_no,
        break_contract,
        sale_remark,
        real_perform_custom
    )   ,
tmp_urge_handle_task_df as 
(
      select
        a.incidental_expenses_no,
        a.job_status,
        a.no_payback_reason,
        a.payback_time,
        a.follow_up_user_code,
        a.follow_up_user_name,
        a.task_remark,
        a.reject_reason,
        a.real_perform_custom,
        is_problem_account,
        -- 是否问题账款 0-否 1-是
        is_review,
        --是否复核 0-未复核 1-已复核
        update_time max_update_time,
        create_time max_create_time
      from
        (
          select
            a.incidental_expense_code incidental_expenses_no,
            a.status job_status,
            a.no_back_reason no_payback_reason,
            a.money_back_date payback_time,
            a.follow_up_user_code,
            a.follow_up_user_name,
            a.remark task_remark,
            a.reject_reason,
            coalesce(a.real_performance_customer, '') real_perform_custom,
            update_time,
            create_time,
            is_problem_account,
            -- 是否问题账款 0-否 1-是
            is_review,
            -- 是否复核 0-未复核 1-已复核
            row_number() over(
              partition by incidental_expense_code
              order by
                update_time desc
            ) as rn
          from
            csx_dwd.csx_dwd_sss_urge_handle_task_df a where shipper_code = 'YHCSX'
        ) a
      where
        rn = 1
    ),
tmp_incidental_occupy_record_df as 
(
      select
        incidental_expenses_no,
        concat_ws(',', collect_set(operate_no)) as operate_no,
        concat_ws(',', collect_set(voucher_no)) as voucher_no,
        sum(operate_amount) as operate_amount,
        concat_ws(',', collect_set(cast(trade_time as string))) as trade_time,
        concat_ws(',', collect_set(cast(write_off_source as string))) as write_off_source,
        concat_ws(
          ',',
          collect_set(
            case
              when write_off_source = '1' then '认领'
              when write_off_source = '2' then '冲抵'
            end
          )
        ) as write_off_source_name,
        concat_ws(',', collect_set(cast(all_write_off as string))) as all_write_off,
        concat_ws(
          ',',
          collect_set(
            case
              when all_write_off = '0' then '否'
              when all_write_off = '1' then '是'
            end
          )
        ) as all_write_off_name,
        concat_ws(',', collect_set(cast(operate_type as string))) as operate_type,
        concat_ws(
          ',',
          collect_set(
            case
              when operate_type = '0' then '冲抵中'
              when operate_type = '1' then '取消占用'
              when operate_type = '2' then '已执行'
              when operate_type = '3' then '已释放'
            end
          )
        ) as operate_type_name
      from
        csx_ods.csx_ods_csx_b2b_sss_sss_incidental_occupy_record_df
      where
        sdt = '${ytd}' and shipper_code = 'YHCSX'
      group by
        incidental_expenses_no
    ) ,
 tmp_crm_credit_sign_company_active as    
(
      select
        credit_code,
        sign_company_code,
        case
          when diff_days < 30 then '活跃客户'
          when diff_days >= 30
          and diff_days <= 60 then '沉默客户'
          when diff_days > 60
          and diff_days <= 90 then '预流失客户'
          when diff_days > 90 then '流失客户'
          else null
        end as customer_status_name
      from
        -- 客户信息
        (
          select
            credit_code,
            sign_company_code,
            to_date(
              from_unixtime(unix_timestamp(last_sale_date, 'yyyyMMdd'))
            ) as last_sale_date_2,
            datediff(
              '${ytd_date}',
              to_date(
                from_unixtime(unix_timestamp(last_sale_date, 'yyyyMMdd'))
              )
            ) as diff_days
          from
            csx_dws.csx_dws_crm_credit_sign_company_active
          where
            sdt = '${ytd}'
          group by
            sign_company_code,
            credit_code,
            to_date(
              from_unixtime(unix_timestamp(last_sale_date, 'yyyyMMdd'))
            ),
            datediff(
              '${ytd_date}',
              to_date(
                from_unixtime(unix_timestamp(last_sale_date, 'yyyyMMdd'))
              )
            )
        ) a
    ),
csx_analyse_tmp_fr_sss_incidental_write_off_info_di as (
  select
    concat(a.incidental_expenses_no, '${ytd}') as biz_id,
    -- 业务主键
    coalesce(g.belong_region_code, '') as belong_region_code,
    -- 大区编码
    -- coalesce(g.belong_region_name,'') as belong_region_name, -- 大区名称
    belong_region_name as performance_region_name,
    coalesce(new_purchase_code, '') performance_province_code,
    -- 省份编码
    coalesce(performance_province_name, '') performance_province_name,
    -- 	coalesce(g.performance_province_name,'') as performance_province_name, -- 省份名称
    a.incidental_expenses_no,
    -- 杂项用款单号
    a.payment_unit_name,
    -- 签约主体
    a.payment_company_code,
    -- 实际付款公司编码
    a.payment_company_name,
    -- 实际付款公司名称
    a.receiving_customer_code,
    -- 收款客户编码
    regexp_replace(
      a.receiving_customer_name,
      '\n|\t|\r|\,|\"|\\\\n',
      ''
    ) as receiving_customer_name,
    -- 收款客户名称
    a.business_scene,
    -- 业务场景名称
    a.business_scene_code,
    -- 业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约
    a.payment_amount,
    -- 付款金额
    a.write_off_amount,
    -- 核销金额
    a.lave_write_off_amount,
    -- 剩余待核销金额
    a.payment_status,
    -- 付款状态  S:付款成功 T:已退汇
    case
      when a.payment_status = 'S' then '付款成功'
      when a.payment_status = 'T' then '已退汇'
      else ''
    end as payment_status_name,
    -- 付款状态名称
    a.payment_method,
    -- 支付方式  1:外付网银 2:线下
    case
      when a.payment_method = '1' then '外付网银'
      when a.payment_method = '2' then '线下'
      else ''
    end as payment_method_name,
    -- 支付方式名称
    a.apply_user,
    -- 申请人
    a.responsible_person,
    -- 负责人
    a.responsible_person_phone,
    a.write_off_status,
    -- 核销状态  0:未核销  1:已核销 2:部分核销
    case
      when a.write_off_status = '0' then '未核销'
      when a.write_off_status = '1' then '已核销'
      when a.write_off_status = '2' then '部分核销'
      else ''
    end as write_off_status_name,
    -- 核销状态名称
    a.write_off_type,
    -- 核销类型  1:慧共享核销  2:手工核销
    case
      when a.write_off_type = '1' then '慧共享核销'
      when a.write_off_type = '2' then '手工核销'
      else ''
    end as write_off_type_name,
    -- 核销类型名称
    to_date(a.apply_date) as apply_date,
    -- 单据申请日期
    regexp_replace(a.apply_reason, '\n|\t|\r|\,|\"|\\\\n', '') as apply_reason,
    -- 申请事由
    a.entry_company_code,
    -- 入账单位编码
    a.entry_company_name,
    -- 入账单位名称
    a.assignment_number,
    -- 分配号
    a.audit_status,
    -- 审批状态
    a.approved_date,
    -- 单据审批通过日期
    a.voucher_code,
    -- 凭证编码
    a.receiving_account,
    -- 客户收款账号
    coalesce(a.unpaid_progress, '') as unpaid_progress,
    -- 未回款进度 1:合同未签署 2:退款流程中 3:合同已丢失 4:诉讼中 5:收据已丢失 6:其他
    case
      when a.unpaid_progress = '1' then '合同未签署'
      when a.unpaid_progress = '2' then '退款流程中'
      when a.unpaid_progress = '3' then '合同已丢失'
      when a.unpaid_progress = '4' then '诉讼中'
      when a.unpaid_progress = '5' then '收据已丢失'
      when a.unpaid_progress = '6' then '其他'
      else ''
    end as unpaid_progress_name,
    -- 未回款进度名称
    a.form_todo_progress,
    -- 表单待办进度  0:未填写  1:已完成 2:待办中
    case
      when a.form_todo_progress = '0' then '未填写'
      when a.form_todo_progress = '1' then '已完成'
      when a.form_todo_progress = '2' then '待办中'
      else ''
    end as form_todo_progress_name,
    -- 表单待办进度名称
    a.finance_form_todo_progress,
    -- 财务表单待办进度  0:未填写  1:已完成
    case
      when a.finance_form_todo_progress = '0' then '未填写'
      when a.finance_form_todo_progress = '1' then '已完成'
      else ''
    end as finance_form_todo_progress_name,
    -- 财务表单待办进度名称
    a.sale_form_todo_progress,
    -- 销售表单待办进度  0:未填写  1:已完成
    case
      when a.sale_form_todo_progress = '0' then '未填写'
      when a.sale_form_todo_progress = '1' then '已完成'
      else ''
    end as sale_form_todo_progress_name,
    -- 销售表单待办进度名称
    a.tender_form_todo_progress,
    -- 投标表单待办进度  0:未填写  1:已完成
    case
      when a.tender_form_todo_progress = '0' then '未填写'
      when a.tender_form_todo_progress = '1' then '已完成'
      else ''
    end as tender_form_todo_progress_name,
    -- 投标表单待办进度名称
    a.attachment_todo_progress,
    -- 附件待办进度  0:未上传  1:已完成 2:待办中
    case
      when a.attachment_todo_progress = '0' then '未上传'
      when a.attachment_todo_progress = '1' then '已完成'
      when a.attachment_todo_progress = '2' then '待办中'
      else ''
    end as attachment_todo_progress_name,
    -- 附件待办进度名称
    a.finance_attachment_todo_progress,
    -- 财务附件待办进度  0:未上传  1:已完成
    case
      when a.finance_attachment_todo_progress = '0' then '未上传'
      when a.finance_attachment_todo_progress = '1' then '已完成'
      else ''
    end as finance_attachment_todo_progress_name,
    -- 财务附件待办进度名称
    a.tender_attachment_todo_progress,
    -- 投标附件待办进度  0:未上传  1:已完成
    case
      when a.tender_attachment_todo_progress = '0' then '未上传'
      when a.tender_attachment_todo_progress = '1' then '已完成'
      else ''
    end as tender_attachment_todo_progress_name,
    -- 投标附件待办进名称
    a.update_by,
    -- 中台更新人
    a.update_time,
    -- 中台更新时间
    a.is_deleted,
    -- 状态：0:正常、1:删除
    coalesce(b.self_employed, '') as self_employed,
    -- 是否自营  0:否  1:是
    case
      when b.self_employed = '0' then '否'
      when b.self_employed = '1' then '是'
      else ''
    end as self_employed_name,
    -- 是否自营
    coalesce(b.cooperation_deposit_recovery, '') as cooperation_deposit_recovery,
    -- 合作保证金是否已收回  0:否  1:是
    case
      when b.cooperation_deposit_recovery = '0' then '否'
      when b.cooperation_deposit_recovery = '1' then '是'
      else ''
    end as cooperation_deposit_recovery_name,
    -- 合作保证金是否已收回
    coalesce(b.money_back_no_write_off, '') as money_back_no_write_off,
    -- 是否已回款未核销  0:否  1:是
    case
      when b.money_back_no_write_off = '0' then '否'
      when b.money_back_no_write_off = '1' then '是'
      else ''
    end as money_back_no_write_off_name,
    -- 是否已回款未核销
    coalesce(b.change_business_scene, '') as change_business_scene,
    -- 转其他业务场景
    coalesce(b.change_business_scene_code, '') as change_business_scene_code,
    -- 转其他业务场景代码 1:投标保证金  2:履约保证金 3:投标转履约
    coalesce(b.change_performance_offline_voucher, '') as change_performance_offline_voucher,
    -- 转履约线下凭证号
    coalesce(b.receipt_recover, '') as receipt_recover,
    -- 收据是否回收  0:否  1:是
    case
      when b.receipt_recover = '0' then '否'
      when b.receipt_recover = '1' then '是'
      else ''
    end as receipt_recover_name,
    -- 收据是否回收
    coalesce(b.contract_recover, '') as contract_recover,
    -- 合同是否回收  0:否  1:是
    case
      when b.contract_recover = '0' then '否'
      when b.contract_recover = '1' then '是'
      else ''
    end as contract_recover_name,
    -- 合同是否回收
    coalesce(
      regexp_replace(b.finance_remark, '\n|\t|\r|\,|\"|\\\\n', ''),
      ''
    ) as finance_remark,
    -- 财务备注
    coalesce(b.sign_off_attachment_url, '') as sign_off_attachment_url,
    -- 签呈文件附件
    coalesce(b.receipt_attachment_url, '') as receipt_attachment_url,
    -- 收据文件附件
    coalesce(b.sales_contract_attachment_url, '') as sales_contract_attachment_url,
    -- 销售合同文件附件
    coalesce(c.break_contract, '') as break_contract,
    -- 是否已经断约  0:否  1:是
    case
      when c.break_contract = '0' then '否'
      when c.break_contract = '1' then '是'
      else ''
    end as break_contract_name,
    -- 是否已经断约
    coalesce(
      regexp_replace(c.sale_remark, '\n|\t|\r|\,|\"|\\\\n', ''),
      ''
    ) as sale_remark,
    -- 销售备注
    coalesce(d.won_bid, '') as won_bid,
    -- 是否中标
    coalesce(d.won_bid_date, '') as won_bid_date,
    -- 中标日期
    coalesce(d.target_payment_time, '') as target_payment_time,
    -- 目标回款时间
    coalesce(d.tender_attachment_url, '') as tender_attachment_url,
    -- 招标文件附件
    coalesce(
      regexp_replace(d.tender_remark, '\n|\t|\r|\,|\"|\\\\n', ''),
      ''
    ) as tender_remark,
    -- 投标备注
    e.operate_no,
    -- 核销单号
    e.voucher_no,
    -- sap凭证单号
    e.operate_amount,
    -- 操作金额
    e.trade_time,
    -- 交易时间
    e.write_off_source,
    -- 核销来源：1:认领、2:冲抵
    e.write_off_source_name,
    -- 核销来源名称
    e.all_write_off,
    -- 是否全部核销：0:否、1:是
    e.all_write_off_name,
    -- 是否全部核销
    e.operate_type,
    -- 操作类型 0:冲抵中 1:取消占用 2:已执行 3:已释放
    e.operate_type_name,
    -- 操作类型名称
    coalesce(a.break_contract_date, '') as break_contract_date,
    -- 断约时间
    a.receiving_account_name,
    -- 收款账号名称
    coalesce(a.account_diff, '') as account_diff,
    -- 账期天数差值
    case
      when account_diff >= 0
      and account_diff <= 60 then '[0,60天]'
      when account_diff > 60
      and account_diff <= 90 then '(60,90天]'
      when account_diff > 90
      and account_diff <= 180 then '(90,180天]'
      when account_diff > 180
      and account_diff <= 365 then '(180,365天]'
      when account_diff > 365 then '>365天'
      else ''
    end as account_type,
    -- 账期类型
    case
      when f.table_type = '1' then '否'
      when f.table_type = '2' then '是'
      else ''
    end as is_borrow_zizhi,
    -- 是否借用资质
    a.create_by,
    --  数据创建人
    coalesce(h.customer_status_name, '') as customer_status_name,
    if(
      j.follow_up_user_name = ''
      or j.follow_up_user_name is null,
      responsible_person_number,
      j.follow_up_user_code
    ) follow_up_user_code,
    -- 跟进人
    if(
      j.follow_up_user_name = ''
      or j.follow_up_user_name is null,
      responsible_person,
      j.follow_up_user_name
    ) follow_up_user_name,
    -- 跟进人姓名
    a.credit_customer_code,
    -- 信控号
    j.no_payback_reason,
    -- 未回款原因
    j.payback_time,
    -- 目标回款时间
    regexp_replace(j.task_remark, '\\n', '') task_remark,
    -- 任务备注
    j.job_status,
    -- 催办状态 任务状态 1-待处理 2-已提交 3-驳回待处理
    case
      when j.job_status = 1 then '待处理'
      when j.job_status = 2 then '已提交'
      when j.job_status = 3 then '驳回待处理'
      when j.job_status = 4  then '已过期'
      else ''
    end job_status_name,
    -- 催办状态 任务状态 1-待处理 2-已提交 3-驳回待处理
    --  j.reject_reason    , -- 驳回原因
    max_update_time,
    -- 最近处理最新时间
    real_perform_customer_code as real_perform_custom,
    regexp_replace(to_date(a.approved_date), '-', '') as sdt,
    is_problem_account,
    -- 是否问题账款 0-否 1-是
    is_review,
    -- 是否复核 0-未复核 1-已复核
    responsible_person_number -- 负责人工号
  from   tmp_incidental_write_off_df  a
    left join (
      select
        *
      from
        csx_ods.csx_ods_csx_b2b_sss_sss_incidental_write_off_finance_df
      where
        sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '') and shipper_code = 'YHCSX'
    ) b on b.incidental_expenses_no = a.incidental_expenses_no
    left join 
        tmp_incidental_write_off_sale_df c on c.incidental_expenses_no = a.incidental_expenses_no
    left join (
      select
        *
      from
        csx_ods.csx_ods_csx_b2b_sss_sss_incidental_write_off_tender_df
      where
        sdt = regexp_replace(to_date(date_sub(current_timestamp(), 1)), '-', '')
        and shipper_code = 'YHCSX'
    ) d on d.incidental_expenses_no = a.incidental_expenses_no
    left join 
    tmp_urge_handle_task_df j on j.incidental_expenses_no = a.incidental_expenses_no
    left join 
      tmp_incidental_occupy_record_df  e on e.incidental_expenses_no = a.incidental_expenses_no
    left join (
      select
        *
      from
        csx_dim.csx_dim_basic_company
      where
        sdt = 'current'
    ) f on f.company_code = a.payment_company_code
    left join (
      select
        distinct belong_region_code,
        belong_region_name,
        performance_province_code,
        performance_province_name
      from
        csx_dim.csx_dim_basic_performance_attribution
    ) g on g.performance_province_code = new_purchase_code
    left join 
      tmp_crm_credit_sign_company_active   h on h.credit_code = a.credit_customer_code
    and a.payment_company_code = h.sign_company_code
)

insert
  overwrite table csx_analyse.csx_analyse_fr_sss_incidental_write_off_info_di
select
  biz_id,
  belong_region_code,
  performance_region_name,
  performance_province_code,
  performance_province_name,
  incidental_expenses_no,
  payment_unit_name,
  payment_company_code,
  payment_company_name,
  receiving_customer_code,
  receiving_customer_name,
  business_scene,
  business_scene_code,
  payment_amount,
  write_off_amount,
  lave_write_off_amount,
  payment_status,
  payment_status_name,
  payment_method,
  payment_method_name,
  apply_user,
  responsible_person,
  write_off_status,
  write_off_status_name,
  write_off_type,
  write_off_type_name,
  apply_date,
  apply_reason,
  entry_company_code,
  entry_company_name,
  assignment_number,
  audit_status,
  approved_date,
  voucher_code,
  receiving_account,
  unpaid_progress,
  unpaid_progress_name,
  form_todo_progress,
  form_todo_progress_name,
  finance_form_todo_progress,
  finance_form_todo_progress_name,
  sale_form_todo_progress,
  sale_form_todo_progress_name,
  tender_form_todo_progress,
  tender_form_todo_progress_name,
  attachment_todo_progress,
  attachment_todo_progress_name,
  finance_attachment_todo_progress,
  finance_attachment_todo_progress_name,
  tender_attachment_todo_progress,
  tender_attachment_todo_progress_name,
  update_by,
  update_time,
  is_deleted,
  self_employed,
  self_employed_name,
  cooperation_deposit_recovery,
  cooperation_deposit_recovery_name,
  money_back_no_write_off,
  money_back_no_write_off_name,
  change_business_scene,
  change_business_scene_code,
  change_performance_offline_voucher,
  receipt_recover,
  receipt_recover_name,
  contract_recover,
  contract_recover_name,
  finance_remark,
  sign_off_attachment_url,
  receipt_attachment_url,
  sales_contract_attachment_url,
  break_contract,
  break_contract_name,
  sale_remark,
  won_bid,
  won_bid_date,
  target_payment_time,
  tender_attachment_url,
  tender_remark,
  operate_no,
  voucher_no,
  operate_amount,
  trade_time,
  write_off_source,
  write_off_source_name,
  all_write_off,
  all_write_off_name,
  operate_type,
  operate_type_name,
  break_contract_date,
  receiving_account_name,
  account_diff,
  account_type,
  is_borrow_zizhi,
  create_by,
  customer_status_name,
  follow_up_user_code,
  follow_up_user_name,
  credit_customer_code,
  no_payback_reason,
  payback_time,
  task_remark,
  job_status,
  job_status_name,
  max_update_time process_time,
  real_perform_custom real_perform_customer_code,
  sdt,
  is_problem_account,
  is_review,
  responsible_person_phone,
  responsible_person_number -- ,if(user_phone is null ,employee_code,user_number)  responsible_person_number
from
  csx_analyse_tmp_fr_sss_incidental_write_off_info_di a;




