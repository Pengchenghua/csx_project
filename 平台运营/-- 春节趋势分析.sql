-- 春节趋势分析
--日配剔除直送仓 2023年春节前一个月到正月十五相对往前一个月各省区毛利波动 20240105
-- drop table csx_analyse_tmp.csx_analyse_tmp_sale_cj; 
create table csx_analyse_tmp.csx_analyse_tmp_sale_cj as 
select a.sdt,
case when a.sdt between '20201231' and '20210106' or a.sdt between '20211220' and '20211226' or a.sdt between '20221210' and '20221216' then '第一周'
       when a.sdt between '20210107' and '20210113' or a.sdt between '20211227' and '20220102' or a.sdt between '20221217' and '20221223' then '第二周'
       when a.sdt between '20210114' and '20210120' or a.sdt between '20220103' and '20220109' or a.sdt between '20221224' and '20221230' then '第三周'
       when a.sdt between '20210121' and '20210127' or a.sdt between '20220110' and '20220116' or a.sdt between '20221231' and '20230106' then '第四周'
       when a.sdt between '20210128' and '20210203' or a.sdt between '20220117' and '20220123' or a.sdt between '20230107' and '20230113' then '第五周'
       when a.sdt between '20210204' and '20210210' or a.sdt between '20220124' and '20220130' or a.sdt between '20230114' and '20230120' then '第六周'
       when a.sdt between '20210211' and '20210217' or a.sdt between '20220131' and '20220206' or a.sdt between '20230121' and '20230127' then '节后第一周'
       when a.sdt between '20210218' and '20210224' or a.sdt between '20220207' and '20220213' or a.sdt between '20230128' and '20230203' then '节后第二周'
       end week,
	bqsq,
	a.performance_region_name,
	a.performance_province_name,
	case when a.performance_city_name in('上海松江','上海宝山','江苏苏州') then a.performance_city_name
		else '-' end as performance_city_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	a.customer_code,
	a.customer_name,
	a.goods_code,
	a.goods_name,
	case when a.delivery_type_code=2 then '直送单' end as is_zs,
	case when a.order_channel_code=6 then '调价单' end as is_tj,
	case when a.order_channel_code=4 then '返利单' end as is_fl,
	-- 退货异常只看退货中“子退货单类型 ：1-子退货单逆向” 且 “订单来源字段 不是 改单退货的”
	case when a.refund_order_flag=1 and h.source_type_name is not null then '退货单' end as is_th,	
	
--	a.refund_order_flag !=1 and h.source_type_name is  null
    sum(sale_qty) sale_qty,
	sum(sale_amt)/10000 as sale_amt,
	sum(profit)/10000 as profit,
	sum(profit)/abs(sum(sale_amt)) as profit_rate
  from
  (
  -- 21年 前六周 20201231 - 20210211 除夕 节后20210225 
  -- 22年 20220131 除夕 节后20230214
  -- 23年 20230121 除夕 节后 20230204
    select *,
		case when sdt>='20201231' and sdt<'20210225' then '21年'
		    when  sdt>='20211220' and sdt<'20220214' then '22年'
			when  sdt>='20221210' and sdt<'20230204' then '23年'
			end as bqsq
	from csx_dws.csx_dws_sale_detail_di a 
    where ((sdt>='20201231' and sdt<'20210226') or (sdt>='20211220' and sdt<'20220215') or (sdt>='20221210' and sdt<'20230205'))
	-- and channel_code in ('1', '7', '9') 
	and business_type_code='1'
--	and a.delivery_type_code not in (2,6,4) 
	-- 退货异常只看退货中“子退货单类型 ：1-子退货单逆向” 且 “订单来源字段 不是 改单退货的”
--	and (a.refund_order_flag !=1 and h.source_type_name is null)
  ) a join
  (select * from csx_dim.csx_dim_shop where sdt='current' and shop_low_profit_flag =0) b
    on a.inventory_dc_code = b.shop_code
    left join	
(
select *
from 
(
	select row_number() over(partition by sale_order_code,goods_code order by refund_total_amt desc) as rno,
		inventory_dc_code,
		inventory_dc_name, 
		sdt,	
		refund_code,
		order_status_code,  -- 退货单状态: 10-差异待审(预留) 20-处理中 30-处理完成 -1-差异拒绝
		case order_status_code
		when -1 then '差异拒绝'
		when 10 then '差异待审'
		when 20 then '处理中'
		when 30 then '处理完成'
		else order_status_code end as order_status_name,  -- 退货单状态
		sale_order_code,
		customer_code,
		goods_code,
		regexp_replace(regexp_replace(goods_name,'\n',''),'\r','') as goods_name, 
		case source_biz_type
		when -1 then 'B端订单管理退货'
		when 0 then 'OMS物流审核'
		when 1 then '结算调整数量'
		when 2 then 'OMS调整数量'
		when 3 then 'CRM客诉退货'
		when 4 then 'CRM订单售后退货'
		when 5 then 'CRM预退货审核'
		when 6 then 'CRM签收'
		when 7 then '司机送达时差异'
		when 8 then '司机发起退货'
		when 9 then '实物退仓收货差异'
		when 10 then 'OMS签收'
		end as source_biz_type_name,  -- 订单业务来源（-1-B端订单管理退货 0-OMS物流审核 1-结算调整数量 2-OMS调整数量 3-CRM客诉退货 4-CRM订单售后退货 5-CRM预退货审核 6-CRM签收 7-司机送达时差异 8-司机发起退货 9-实物退仓收货差异 10-OMS签收）
		case refund_operation_type
		when -1 then '不处理'
		when 0 then '立即退'
		when 1 then '跟车退'
		end as refund_operation_type_name,  -- 退货处理方式 -1-不处理 0-立即退 1-跟车退
		case has_goods
		when 0 then '无实物'
		when 1 then '有实物'
		end as has_goods_name,	
		case source_type
		when 0 then '签收差异或退货'
		when 1 then '改单退货'
		end as source_type_name,  -- 订单来源(0-签收差异或退货 1-改单退货)		
		responsibility_reason,
		regexp_replace(regexp_replace(reason_detail,'\n',''),'\r','') as reason_detail,
		case child_return_type_code
		when 0 then '父退货单'
		when 1 then '子退货单逆向'
		when 2 then '子退货单正向'
		end as child_return_type_name,  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
		case refund_order_type_code
		when 0 then '差异单'
		when 1 then '退货单'
		end as refund_order_type_name,	-- 退货单类型(0:差异单 1:退货单）
		-- refund_qty,
		-- sale_price,
		-- refund_total_amt,
		-- refund_scale_total_amt,
		refund_reason,
		first_level_reason_name,
		regexp_replace(regexp_replace(second_level_reason_name,'\n',''),'\r','') as second_level_reason_name
	from csx_dwd.csx_dwd_oms_sale_refund_order_detail_di
	where sdt>='20190101'
	and child_return_type_code in(1)  -- 子退货单类型 ：0-父退货单 1-子退货单逆向 2-子退货单正向
	and parent_refund_code<>''
	and source_type=0   -- 订单来源(0-签收差异或退货 1-改单退货)	
 	)a
	where rno=1	
)h on a.original_order_code=h.sale_order_code and a.goods_code=h.goods_code
group by 
	bqsq, case when a.sdt between '20201231' and '20210106' or a.sdt between '20211220' and '20211226' or a.sdt between '20221210' and '20221216' then '第一周'
       when a.sdt between '20210107' and '20210113' or a.sdt between '20211227' and '20220102' or a.sdt between '20221217' and '20221223' then '第二周'
       when a.sdt between '20210114' and '20210120' or a.sdt between '20220103' and '20220109' or a.sdt between '20221224' and '20221230' then '第三周'
       when a.sdt between '20210121' and '20210127' or a.sdt between '20220110' and '20220116' or a.sdt between '20221231' and '20230106' then '第四周'
       when a.sdt between '20210128' and '20210203' or a.sdt between '20220117' and '20220123' or a.sdt between '20230107' and '20230113' then '第五周'
       when a.sdt between '20210204' and '20210210' or a.sdt between '20220124' and '20220130' or a.sdt between '20230114' and '20230120' then '第六周'
       when a.sdt between '20210211' and '20210217' or a.sdt between '20220131' and '20220206' or a.sdt between '20230121' and '20230127' then '节后第一周'
       when a.sdt between '20210218' and '20210224' or a.sdt between '20220207' and '20220213' or a.sdt between '20230128' and '20230203' then '节后第二周'
       end ,
       a.customer_code,
       a.customer_name,
	classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
		case when a.delivery_type_code=2 then '直送单' end ,
	case when a.order_channel_code=6 then '调价单' end ,
	case when a.order_channel_code=4 then '返利单' end ,
	-- 退货异常只看退货中“子退货单类型 ：1-子退货单逆向” 且 “订单来源字段 不是 改单退货的”
	case when a.refund_order_flag=1 and h.source_type_name is not null then '退货单' end,
	a.performance_region_name,
	a.performance_province_name,
	case when a.performance_city_name in('上海松江','上海宝山','江苏苏州') then a.performance_city_name
		else '-' end,a.sdt,
		a.goods_code,
	a.goods_name;

-- 21年 前六周 20201231 - 20210211 除夕 节后20210225 
  -- 22年 20220131 除夕 节后20230214
  -- 23年 20230121 除夕 节后 20230204
  -- (sdt>='20201231' and sdt<'20210226') or (sdt>='20211220' and sdt<'20220215') or (sdt>='20221210' and sdt<'20230205')
  select  bqsq,performance_region_name,
    performance_province_name,
    performance_city_name,
  case when sdt between '20201231' and '20210106' or sdt between '20211220' and '20211226' or sdt between '20221210' and '20221216' then '第一周'
       when sdt between '20210107' and '20210113' or sdt between '20211227' and '20220102' or sdt between '20221217' and '20221223' then '第二周'
       when sdt between '20210114' and '20210120' or sdt between '20220103' and '20220109' or sdt between '20221224' and '20221230' then '第三周'
       when sdt between '20210121' and '20210127' or sdt between '20220110' and '20220116' or sdt between '20221231' and '20230106' then '第四周'
       when sdt between '20210128' and '20210203' or sdt between '20220117' and '20220123' or sdt between '20230107' and '20230113' then '第五周'
       when sdt between '20210204' and '20210210' or sdt between '20220124' and '20220130' or sdt between '20230114' and '20230120' then '第六周'
       when sdt between '20210211' and '20210217' or sdt between '20220131' and '20220206' or sdt between '20230121' and '20230127' then '节后第一周'
       when sdt between '20210218' and '20210224' or sdt between '20220207' and '20220213' or sdt between '20230128' and '20230203' then '节后第二周'
       end week,
    --   when sdt between '20210124' and '202101' or sdt between '20220207' and '20220213' or sdt between '20230127' and '20230202' then '节后第二周'
    classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	sum(sale_amt) sale_amt,  --range是根据时间来进行汇总,而row是按照行.604800为秒数
    sum(profit) as profit   -- lead(sdt,6,0) over(partition by bqsq order by sdt asc) as enddate
  from csx_analyse_tmp.csx_analyse_tmp_sale_cj 
  where is_zs is null 
    and is_tj is null 
    and is_fl is null  
    and is_th is null 
 group by bqsq,
  case when sdt between '20201231' and '20210106' or sdt between '20211220' and '20211226' or sdt between '20221210' and '20221216' then '第一周'
       when sdt between '20210107' and '20210113' or sdt between '20211227' and '20220102' or sdt between '20221217' and '20221223' then '第二周'
       when sdt between '20210114' and '20210120' or sdt between '20220103' and '20220109' or sdt between '20221224' and '20221230' then '第三周'
       when sdt between '20210121' and '20210127' or sdt between '20220110' and '20220116' or sdt between '20221231' and '20230106' then '第四周'
       when sdt between '20210128' and '20210203' or sdt between '20220117' and '20220123' or sdt between '20230107' and '20230113' then '第五周'
       when sdt between '20210204' and '20210210' or sdt between '20220124' and '20220130' or sdt between '20230114' and '20230120' then '第六周'
       when sdt between '20210211' and '20210217' or sdt between '20220131' and '20220206' or sdt between '20230121' and '20230127' then '节后第一周'
       when sdt between '20210218' and '20210224' or sdt between '20220207' and '20220213' or sdt between '20230128' and '20230203' then '节后第二周'
       end ,
    classify_large_code,
	classify_large_name,
	classify_middle_code,
	classify_middle_name,
	performance_region_name,
    performance_province_name,
    performance_city_name
	