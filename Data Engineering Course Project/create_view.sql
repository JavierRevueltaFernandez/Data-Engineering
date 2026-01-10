CREATE OR REPLACE VIEW `wb-economic-pipeline.economic_indicators.vw_fact_enriched` AS
SELECT
  f.country_code,
  c.TableName AS country_name,
  c.Region AS region,
  c.IncomeGroup AS income_group,
  f.indicator_code,
  i.indicator_name,
  f.year,
  f.value
FROM `wb-economic-pipeline.economic_indicators.wb_fact_economic_indicators` f
LEFT JOIN `wb-economic-pipeline.economic_indicators.wb_dim_country` c
  ON f.country_code = c.`Country Code`
LEFT JOIN `wb-economic-pipeline.economic_indicators.wb_dim_indicator` i
  ON f.indicator_code = i.indicator_code;
