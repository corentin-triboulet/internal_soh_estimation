DECLARE end_date DATE DEFAULT CURRENT_DATE();
DECLARE start_date DATE DEFAULT DATE_SUB(end_date, INTERVAL 2 MONTH );
--DECLARE hardware_selected ARRAY<STRING> DEFAULT ["SO04-00","SO02-03"];
DECLARE battery_sku_selected ARRAY<STRING> DEFAULT ["OK-BAT-00", "OK-BAT-01","OK-BAT-02","OK-BAT-03","OK-BAT-10", "OK-BAT-11","OK-BAT-12","OK-BAT-13"];
--DECLARE battery_sku_selected ARRAY<STRING> DEFAULT ["OK-BAT-00", "OK-BAT-01","OK-BAT-02","OK-BAT-03"];
DECLARE EoL_voltage FLOAT64 DEFAULT 45.5;
DECLARE SoL_voltage FLOAT64 DEFAULT 54.6;


WITH 
battery_snapshot AS (
  SELECT
    battery_id,
    battery_capacity,
    DATE_DIFF( date_registration_local,'2000-01-01', MONTH) AS age_month_ref_date_01_01_2000,
    is_broken,
    battery_sku,
    vendor_battery_id,
    battery_voltage,
    tags,
    market_name
  FROM `com-ridedott-data.bi_marts.dwd_dim_batteries`
  WHERE date_snapshot_local = CURRENT_DATE()
  AND battery_sku IN UNNEST(battery_sku_selected)
),

tags_list AS (
  SELECT DISTINCT tag
  FROM battery_snapshot, UNNEST(tags) AS tag
),
 


bmi AS (
  SELECT
    scooter_battery_sn AS vendor_battery_id,
    scooter_battery_hardware_version,
    vehicle_id,
    time_updated,
    voltage/1000 AS voltage,
    capacity_and_health_status,
    soc_percentage,
    soh_percentage,
    battery_cycles,
    ROW_NUMBER() OVER (PARTITION BY scooter_battery_sn ORDER BY time_updated) AS time_ranking
  FROM `com-ridedott-data.rdl.dwr_okai_gtbmi`
  WHERE DATE(time_updated) BETWEEN start_date AND end_date
    AND battery_cycles IS NOT NULL
    AND battery_cycles != 0
    AND capacity_and_health_status !=0
    AND voltage != 0

),

bmi_soc100 AS(
  SELECT
    vendor_battery_id,
    voltage voltage_soc100

  FROM bmi
  WHERE soc_percentage = 100
  AND voltage BETWEEN EoL_voltage AND SoL_voltage
),



agg_bmi_soc100 AS(
  SELECT
  vendor_battery_id
  , MAX(voltage_soc100) max_voltage_soc100
  , AVG(voltage_soc100) avg_voltage_soc100
  , MIN(voltage_soc100) min_voltage_soc100
  FROM bmi_soc100
  GROUP BY 1 
), 

agg_bmi AS(
  SELECT
  vendor_battery_id
  , MIN(voltage) min_voltage
  FROM bmi
  GROUP BY 1 
), 

drainage AS(
  SELECT 
   battery_id

  -- data
  , battery_delta
  , distance_geo_track/1000 distance_geo_track_km
  , SAFE_DIVIDE(battery_delta,distance_geo_track/1000) in_ride_drainage_prct_km
  -- time
  , time_ride_start
FROM `com-ridedott-data.tableau_custom_sql.dwd_fact_battery_efficiency`
WHERE DATE(time_ride_start) BETWEEN start_date AND end_date
),

drainage_agg AS(
  SELECT
  battery_id
  , PERCENTILE_CONT(in_ride_drainage_prct_km, 0.5) OVER (PARTITION BY battery_id) median_in_ride_drainage_prct_km
  FROM drainage

),


main AS(
  SELECT
  battery_id
  , vendor_battery_id
  , scooter_battery_hardware_version
  , battery_sku
  , battery_capacity
  , is_broken
  --, tags
  , tag = "QB-92" AS QB_92
  , market_name
  , capacity_and_health_status
  , CAST(SAFE_MULTIPLY(capacity_and_health_status/100,battery_capacity)AS INT)  health_capacity
  , agb_soc100.max_voltage_soc100
  , agb_soc100.avg_voltage_soc100
  , agb_soc100.min_voltage_soc100
  , ROUND(SAFE_DIVIDE(EoL_voltage-agb_soc100.max_voltage_soc100,EoL_voltage-SoL_voltage)*100, 1) max_voltage_soh
  , CAST(SAFE_DIVIDE(EoL_voltage-agb_soc100.max_voltage_soc100,EoL_voltage-SoL_voltage)*battery_capacity AS INT) max_voltage_capacity
  , ROUND(SAFE_DIVIDE(EoL_voltage-agb_soc100.avg_voltage_soc100,EoL_voltage-SoL_voltage)*100, 1) avg_voltage_soh
  , CAST(SAFE_DIVIDE(EoL_voltage-agb_soc100.avg_voltage_soc100,EoL_voltage-SoL_voltage)*battery_capacity AS INT) avg_voltage_capacity
  , ROUND(SAFE_DIVIDE(EoL_voltage-agb_soc100.min_voltage_soc100,EoL_voltage-SoL_voltage)*100, 1) min_voltage_soh
  , CAST(SAFE_DIVIDE(EoL_voltage-agb_soc100.min_voltage_soc100,EoL_voltage-SoL_voltage)*battery_capacity AS INT) min_voltage_capacity
  , agb.min_voltage
  , age_month_ref_date_01_01_2000
  , soh_percentage
  , battery_cycles

  , median_in_ride_drainage_prct_km
  , median_in_ride_drainage_prct_km*battery_voltage/100/1000 median_in_ride_drainage_wh_km

  FROM battery_snapshot, UNNEST(tags) AS tag 
  LEFT JOIN bmi USING(vendor_battery_id)
  LEFT JOIN agg_bmi_soc100 AS agb_soc100 USING(vendor_battery_id)
  LEFT JOIN agg_bmi AS agb USING(vendor_battery_id)
  LEFT JOIN drainage_agg AS da USING(battery_id)

  WHERE time_ranking = 1
), 


validation AS (
  SELECT
    'voltage_soc100_validation' AS validation_type,
    COUNT(*) AS total_lines,
    SUM(CASE WHEN max_voltage_soc100 IS NULL THEN 1 ELSE 0 END) AS null_voltage_soc100_count,
    ROUND(SUM(CASE WHEN max_voltage_soc100 IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS percentage_null_voltage_soc100
  FROM main
)

SELECT * FROM main