-- 帮我取一版数据，投标日期为2025年以来的标讯，并且关联了商机，所需字段包括
-- 标讯系统：标讯号、投标日期、项目名称、客户名称、标的总金额、投标状态（同商机总表规则）、业务类型
-- 商机系统：省、市（省市同商机总表规则）、商机归属人、商机号（多个的合并到一个单元格，同商机总表规则，下同）、商机创建时间、商机客户名称、预计合同签约金额



select bid_no,
    performance_region_name,
    performance_province_name,
    performance_city_name,
    bid_date,
    bid_status_name,
    regexp_replace(bid_name,'\n|\t','') bid_name,
    bid_customer_name,
    business_attribute_name,
    all_bid_amount,
    regexp_replace(list_business_number,'\n|\t','、') list_business_number,
    estimate_contract_amount,
    company_code,
    company_name,
    sales_user_name,
    second_category_name,
    business_create_time
from     csx_analyse_tmp.csx_analyse_tmp_fr_bid_business_million_detail_di where bid_date>='2025-01-01'