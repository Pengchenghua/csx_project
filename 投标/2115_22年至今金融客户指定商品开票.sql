-- 大米 、 面粉 、 食用油 、 洗衣液 、 抽纸 、 陶瓷对杯 、 毛巾礼盒 、 雨伞 、 茶水分离杯 、 保温杯 、 家居保温壶 、 电动剃须刀 、 电吹风 、 电煮锅 、 电火锅 、 养生壶 、 早餐机 、 电饭煲 、 被子 、 床上四件套 、 拉杆箱 、 充电宝 、 空气炸锅 、 颈部按摩仪 、 蓝牙耳机 、 露营车 、 体脂称 -- 2021年至今2115公司的水果、蔬菜、豆制品、蛋开票情况 20240619
with apply_goods_group as -- 发票商品明细
(
  select
    d.order_code,
    d.total_amt as total_amount,
    -- e.purchase_group_code,
    -- e.purchase_group_name,
    e.classify_large_code,
    e.classify_large_name,
    -- 管理大类
    e.classify_middle_code,
    e.classify_middle_name,
    -- 管理中类
    e.classify_small_code,
    e.classify_small_name -- 管理小类
  from
    (
      select
        *,
        --  id排名取最新一条
        row_number() over(
          partition by id
          order by
            update_time desc
        ) as id_rank -- from csx_ods.csx_ods_csx_b2b_sss_sss_kp_apply_goods_group_di
        -- where (sdt>='20200101' or sdt='19990101')
      from
        csx_dwd.csx_dwd_sss_kp_apply_goods_group_di
      where
        sdt >= '20220101'
        and is_delete = '0'
    ) d
join (
      select
        goods_code,
        goods_name,
        classify_large_code,
        classify_large_name,
        -- 管理大类
        classify_middle_code,
        classify_middle_name,
        -- 管理中类
        classify_small_code,
        classify_small_name, -- 管理小类
        category_middle_code,
        category_middle_name,
        category_small_name,
        category_small_code
      from
        csx_dim.csx_dim_basic_goods
      where
        sdt = 'current'
        and((goods_name like '%挂烫机%'
        or goods_name like '%电风扇%'
        or (goods_name like '%电暖器%'  )
        or goods_name like '%小太阳%'
        or (goods_name like '%吸尘器%'  )
        or goods_name like '%煮茶器%'
        or (goods_name like '%干衣机%' )
        or goods_name like '%吹风机%' 
        or goods_name like '%饮水机%'
        or goods_name like '%电动牙刷%'
        or goods_name like '%蓝牙耳机%'
        or goods_name like '%空气净化器%'
        or goods_name like '%充气泵%'
        or goods_name like '%洗手液%'
        or goods_name like '%洗衣液%'
        or goods_name like '%沐浴露%'
        or goods_name like '%洗发水%'
        or goods_name like '%牙膏%'
        or goods_name like '%牙刷%'
        or goods_name like '%保温水壶%'
        or goods_name like '%保温杯%'
        or goods_name like '%茶具%' 
        or goods_name like '%茶道六君子%'
        or goods_name like '%指甲刀%'
        or goods_name like '%牛角梳%'
        or goods_name like '%充电宝%'
        )
        -- or classify_middle_code = 	'B0102'
        -- or classify_small_code =	'B010101'
        -- or classify_middle_code=	'B0603'
        )
) e on d.goods_code = e.goods_code
  where
    d.id_rank = 1 -- and e.classify_middle_name='水产'
    -- and e.classify_middle_name in('猪肉', '牛羊') --or (e.classify_middle_name='干货' and classify_small_name='豆制品')
),
-- 发票表
invoice as (
  select
    sdt invoice_date,
    invoice_no,
    -- 发票号码
    order_code,
    -- 订单编号
    invoice_code,
    -- 发票代码
    company_code,
    -- 公司代码
    customer_code,
    -- 客户编码
    total_amount,
    -- 总金额
    invoice_customer_name,
    -- 客户开发票名称
    if(offline_flag_code = 1, '是', '否') as offline_flag,
    -- 是否线下开票 0 否 1 是
    regexp_replace(invoice_remark, '\\n|\\r|\\t', '') invoice_remark -- 发票的备注
    -- row_number() over(partition by invoice_no order by sdt desc)	as num1
  from
    csx_dwd.csx_dwd_sss_invoice_di
  where
    sdt >= '20220101' -- and invoice_no in('05502566','85686779')
    and delete_flag = '0'
    and sync_status = 1
    -- and company_code = '2115'
)
select
  c.performance_region_name,
  --  销售大区名称(业绩划分)
  c.performance_province_name,
  --  销售归属省区名称
  c.performance_city_name,
  --  城市组名称(业绩划分)
  -- b.invoice_date,  -- 开票日期
  -- b.invoice_no,		-- 发票号码
  -- b.order_code,		-- 订单编号
  -- b.invoice_code,		-- 发票代码
  b.company_code,
  -- 公司代码
  b.customer_code,
  -- 客户编码
  c.customer_name,
  --  客户名称
  c.second_category_name,
  --  二级客户分类名称
  -- b.total_amount,		-- 总金额
  -- b.invoice_customer_name,		-- 客户开发票名称
  -- b.offline_flag,		-- 是否线下开票 0 否 1 是
  -- b.invoice_remark,		-- 发票的备注
  -- a.total_amount,
  -- a.classify_middle_name,-- 管理中类
  sum(a.total_amount) as total_amount_all,
  sum(
    case
      when substr(b.invoice_date, 1, 4) = '2022' then a.total_amount
    end
  ) as total_amount_2021,
  sum(
    case
      when substr(b.invoice_date, 1, 4) = '2023' then a.total_amount
    end
  ) as total_amount_2022,
  sum(
    case
      when substr(b.invoice_date, 1, 4) = '2024' then a.total_amount
    end
  ) as total_amount_2023,
  sum(
    case
      when substr(b.invoice_date, 1, 4) = '2025' then a.total_amount
    end
  ) as total_amount_2024
from
  apply_goods_group a
  join invoice b on a.order_code = b.order_code
  left join (
    select
      bloc_code,
      --  集团编码
      bloc_name,
      --  集团名称
      customer_id,
      customer_code,
      customer_name,
      --  客户名称
      first_category_code,
      --  一级客户分类编码
      first_category_name,
      --  一级客户分类名称
      second_category_code,
      --  二级客户分类编码
      second_category_name,
      --  二级客户分类名称
      third_category_code,
      --  三级客户分类编码
      third_category_name,
      --  三级客户分类名称
      contact_person,
      --  联系人姓名
      contact_phone,
      --  联系电话
      performance_region_name,
      --  销售大区名称(业绩划分)
      performance_province_name,
      --  销售归属省区名称
      performance_city_name,
      --  城市组名称(业绩划分)
      channel_code,
      channel_name,
      sales_user_number,
      sales_user_name
    from
      csx_dim.csx_dim_crm_customer_info
    where
      sdt = 'current'
      and customer_type_code = 4
      -- and second_category_name like '%金融%'
      -- and (customer_name like '%中国银行%' or customer_name like '%工商银行%'  or customer_name like '%农业银行%' or  customer_name like '%建设银行%' or customer_name like '%交通银行%' or customer_name like '%邮储银行%')
  ) c on b.customer_code = c.customer_code
  where c.customer_code is not null 
group by
  c.performance_region_name,
  --  销售大区名称(业绩划分)
  c.performance_province_name,
  --  销售归属省区名称
  c.performance_city_name,
  --  城市组名称(业绩划分)
  b.company_code,
  -- 公司代码
  b.customer_code,
  -- 客户编码
  c.customer_name,
  --  客户名称
  c.second_category_name --  二级客户分类名称
  -- a.classify_middle_name-- 管理中类
order by
  total_amount_all desc

