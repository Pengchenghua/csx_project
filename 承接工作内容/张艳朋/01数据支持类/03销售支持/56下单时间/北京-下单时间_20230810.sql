-- 小类维度
drop table if exists csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715;
create table csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715
as
	select 
		performance_province_name,to_date(order_time) as order_date,date_format(order_time,'HH:mm') as order_hour,business_type_name,classify_large_name,classify_middle_name,classify_small_name,
		count(distinct order_code) as order_cnt
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230701' and '20230809'
		and channel_code in ('1','7','9')
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and order_channel_code not in(4,6)
		-- and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and performance_province_name='北京市'
	group by 
		performance_province_name,to_date(order_time),date_format(order_time,'HH:mm'),business_type_name,classify_large_name,classify_middle_name,classify_small_name;

select * from csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715
-- 中类维度		
drop table if exists csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715_2;
create table csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715_2
as
	select 
		performance_province_name,to_date(order_time) as order_date,date_format(order_time,'HH:mm') as order_hour,business_type_name,classify_large_name,classify_middle_name,
		count(distinct order_code) as order_cnt
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230701' and '20230809'
		and channel_code in ('1','7','9')
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and order_channel_code not in(4,6)
		-- and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and performance_province_name='北京市'
	group by 
		performance_province_name,to_date(order_time),date_format(order_time,'HH:mm'),business_type_name,classify_large_name,classify_middle_name;
		
select * from csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715_2;

-- 客户维度		
drop table if exists csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715_3;
create table csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715_3
as
	select 
		performance_province_name,customer_code,customer_name,to_date(order_time) as order_date,date_format(order_time,'HH:mm') as order_hour,business_type_name,classify_large_name,classify_middle_name,
		count(distinct order_code) as order_cnt
	from 
		csx_dws.csx_dws_sale_detail_di
	where 
		sdt between '20230701' and '20230809'
		and channel_code in ('1','7','9')
		and refund_order_flag=0 -- 退货订单标识(0-正向单 1-逆向单)
		and order_channel_code not in(4,6)
		-- and business_type_code =1 -- 仅日配 业务类型编码(1.日配单 2.福利单 3.批发内购 4.城市服务商 5.省区大宗 6.BBC)
		and performance_province_name='北京市'
	group by 
		performance_province_name,to_date(order_time),date_format(order_time,'HH:mm'),business_type_name,classify_large_name,classify_middle_name,customer_code,customer_name;
		
select * from csx_analyse_tmp.csx_analyse_tmp_order_hour_20230715_3;
