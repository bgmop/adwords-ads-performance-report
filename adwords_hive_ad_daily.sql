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
-- ad table
create external table if not exists ${hivevar:raw_table} (
	account_descriptive_name string,
	ad_group_id string,
	ad_group_name string,
	ad_group_status string,
	ad_network_type1 string,
	ad_network_type2 string,
	all_conversions string,
	average_position string,
	campaign_id string,
	campaign_name string,
	campaign_status string,
	clicks string,
	conversions string,
	cost string,
	creative_approval_status string,
	creative_destination_url string,
	creative_final_app_urls string,
	creative_final_mobile_urls string,
	creative_final_urls string,
	creative_tracking_url_template string,
	creative_url_custom_parameters string,
	criterion_id string,
	criterion_type string,
	customer_descriptive_name string,
	report_date string,
	description string,
	description1 string,
	description2 string,
	device string,
	device_preference string,
	display_url string,
	external_customer_id string,
	headline string,
	headline_part1 string,
	headline_part2 string,
	id string,
	impressions string,
	is_negative string,
	label_ids string,
	labels string,
	status string,
	view_through_conversions string
)
PARTITIONED BY (year string, month string, day string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
LOCATION '${hivevar:raw_path}'
TBLPROPERTIES('skip.header.line.count'='1');

msck repair table ${hivevar:raw_table};


------ create schema for production data
-- ad parquet 
create external table if not exists ${hivevar:table} (
	account_descriptive_name string,
	ad_group_id string,
	ad_group_name string,
	ad_group_status string,
	ad_network_type1 string,
	ad_network_type2 string,
	all_conversions double,
	average_position double,
	campaign_id string,
	campaign_name string,
	campaign_status string,
	clicks int,
	conversions double,
	cost bigint,  
	creative_approval_status string,
	creative_destination_url string,
	creative_final_app_urls string,
	creative_final_mobile_urls string,
	creative_final_urls string,
	creative_tracking_url_template string,
	creative_url_custom_parameters string,
	criterion_id string,
	criterion_type string,
	customer_descriptive_name string,
	report_date string,
	description string,
	description1 string,
	description2 string,
	device string,
	device_preference string,
	display_url string,
	external_customer_id string,
	headline string,
	headline_part1 string,
	headline_part2 string,
	id string,
	impressions int,
	is_negative string,
	label_ids string,
	labels string,
	status string,
	view_through_conversions int
)
PARTITIONED BY (year string, month string, day string)
STORED as PARQUET
LOCATION '${hivevar:prod_path}'
TBLPROPERTIES('parquet.compress'='SNAPPY');


------ write data to parquet
-- ad
insert overwrite table ${hivevar:table} partition (year, month, day)
	select
		account_descriptive_name,
		ad_group_id,
		ad_group_name,
		ad_group_status,
		ad_network_type1,
		ad_network_type2,
		all_conversions,
		average_position,
		campaign_id,
		campaign_name,
		campaign_status,
		clicks,
		conversions,
		cost,
		creative_approval_status,
		creative_destination_url,
		creative_final_app_urls,
		creative_final_mobile_urls,
		creative_final_urls,
		creative_tracking_url_template,
		creative_url_custom_parameters,
		criterion_id,
		criterion_type,
		customer_descriptive_name,
		report_date,
		description,
		description1,
		description2,
		device,
		device_preference,
		display_url,
		external_customer_id,
		headline,
		headline_part1,
		headline_part2,
		id,
		impressions,
		is_negative,
		label_ids,
		labels,
		status,
		view_through_conversions,
		year,
		month,
		day
	from ${hivevar:raw_table}
	where year = '${hivevar:yr}' and month = '${hivevar:mth}' and day = '${hivevar:dt}';
