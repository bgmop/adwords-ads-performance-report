-- session level configuration
set amz-assume-role-arn=${hivevar:role};
set parquet.compression=SNAPPY;  
set hive.exec.compress.output=true;
set mapred.output.compression.type=BLOCK;
-- set hive.tez.auto.reducer.parallelism=true;
-- set hive.exec.reducers.bytes.per.reducer=256000000;  
-- file size 256 MB for optimal query performance
set hive.exec.dynamic.partition=true;  
set hive.exec.dynamic.partition.mode=nonstrict;
set tez.grouping.split-count=1;


-- switch schema
create schema if not exists adtech;
use adtech;


------ create mapping of existing raw data
-- criteria table
create external table if not exists ${hivevar:raw_table} (
	ad_group_id string,
	ad_group_name string,
	ad_group_status string,
	ad_network_type1 string,
	ad_network_type2 string,
	all_conversions string,
	approval_status string,
	average_position string,
	bid_modifier string,
	campaign_id string,
	campaign_name string,
	campaign_status string,
	clicks string,
	conversions string,
	cost string,
	cpc_bid string,
	cpc_bid_source string,
	creative_quality_score string,
	criteria_destination_url string,
	criteria string,
	criteria_type string,
	cross_device_conversions string,
	customer_descriptive_name string,
	report_date string,
	device string,
	external_customer_id string,
	final_app_urls string,
	final_mobile_urls string,
	final_urls string,
	first_page_cpc string,
	first_position_cpc string,
	has_quality_score string,
	id string,
	impressions string,
	post_click_quality_score string,
	quality_score string,
	search_predicted_ctr string,
	status string,
	top_of_page_cpc string,
	tracking_url_template string,
	url_custom_parameters string,
	view_through_conversions string
)
PARTITIONED BY (year string, month string, day string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '${hivevar:raw_path}'
TBLPROPERTIES('skip.header.line.count'='1');

msck repair table ${hivevar:raw_table};


------ create schema for production data
-- criteria parquet
create external table if not exists ${hivevar:table} (
	ad_group_id string,
	ad_group_name string,
	ad_group_status string,
	ad_network_type1 string,
	ad_network_type2 string,
	all_conversions double,
	approval_status string,
	average_position double,
	bid_modifier string,
	campaign_id string,
	campaign_name string,
	campaign_status string,
	clicks int,
	conversions double,
	cost bigint,
	cpc_bid bigint,
	cpc_bid_source string,
	creative_quality_score string,
	criteria_destination_url string,
	criteria string,
	criteria_type string,
	cross_device_conversions double,
	customer_descriptive_name string,
	report_date string,
	device string,
	external_customer_id string,
	final_app_urls string,
	final_mobile_urls string,
	final_urls string,
	first_page_cpc bigint,
	first_position_cpc bigint,
	has_quality_score string,
	id string,
	impressions int,
	post_click_quality_score string,
	quality_score int,
	search_predicted_ctr string,
	status string,
	top_of_page_cpc bigint,
	tracking_url_template string,
	url_custom_parameters string,
	view_through_conversions int
)
PARTITIONED	BY (year string, month string, day string)
STORED as PARQUET
LOCATION '${hivevar:prod_path}'
TBLPROPERTIES('parquet.compress'='SNAPPY');


------ write data to parquet
-- criteria
insert overwrite table ${hivevar:table} partition (year, month, day)
	select 
		ad_group_id,
		ad_group_name,
		ad_group_status,
		ad_network_type1,
		ad_network_type2,
		all_conversions,
		approval_status,
		average_position,
		bid_modifier,
		campaign_id,
		campaign_name,
		campaign_status,
		clicks,
		conversions,
		cost,
		cpc_bid,
		cpc_bid_source,
		creative_quality_score,
		criteria_destination_url,
		criteria,
		criteria_type,
		cross_device_conversions,
		customer_descriptive_name,
		report_date,
		device,
		external_customer_id,
		final_app_urls,
		final_mobile_urls,
		final_urls,
		first_page_cpc,
		first_position_cpc,
		has_quality_score,
		id,
		impressions,
		post_click_quality_score,
		quality_score,
		search_predicted_ctr,
		status,
		top_of_page_cpc,
		tracking_url_template,
		url_custom_parameters,
		view_through_conversions,
		year,
		month,
		day
	from ${hivevar:raw_table}
	where year = '${hivevar:yr}' and month = '${hivevar:mth}' and day = '${hivevar:dt}';
