-- Bronze: JSON file ingestion from UC volume
CREATE OR REFRESH STREAMING TABLE bronze_customer_events
COMMENT "Raw events"
TBLPROPERTIES (
  "pipelines.autoOptimize.managed" = "true"
)
AS SELECT
  Customer_ID,
  Last_Login_Date,
  Ebooks_Downloaded_6_Months,
  Average_Session_Time,
  Subscription_Plan_Type,
  Primary_Discipline,
  Days_Since_Last_Activity,
  event_timestamp,
  _metadata.file_path as source_file_path,
  _metadata.file_modification_time as file_mod_time
FROM STREAM read_files(
  "/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/customer_json_files/", -- replace with your own volume path
  format => "json",
  header => "false"
);


-- Silver: Feature engineering
CREATE OR REFRESH STREAMING TABLE silver_customer_features
COMMENT "Engineered features"
AS SELECT
  Customer_ID,
  CAST(Last_Login_Date AS DATE) AS Last_Login_Date,
  Days_Since_Last_Activity,
  Ebooks_Downloaded_6_Months,
  Average_Session_Time,
  (Ebooks_Downloaded_6_Months * 0.3 + Average_Session_Time * 0.7) AS engagement_score,
  CASE
    WHEN Subscription_Plan_Type='Premium' THEN 3
    WHEN Subscription_Plan_Type='Standard' THEN 2
    ELSE 1 END AS subscription_tier_numeric,
  DATEDIFF(CURRENT_DATE(), CAST(Last_Login_Date AS DATE)) AS days_since_login,
  current_timestamp() AS feature_timestamp
FROM STREAM(LIVE.bronze_customer_events);

CREATE OR REFRESH STREAMING TABLE gold_batch_predictions
COMMENT "Customer anomaly scoring"
AS SELECT
  cf.*,
  CAST(predict_customer_anomaly(
    CAST(days_since_login AS INT),
    CAST(Ebooks_Downloaded_6_Months AS INT),
    CAST(Average_Session_Time AS FLOAT),
    CAST(Days_Since_Last_Activity AS FLOAT),
    CAST(engagement_score AS DOUBLE),
    CAST(subscription_tier_numeric AS INT)
  ) AS BIGINT) AS anomaly_prediction,
  CASE 
    WHEN CAST(predict_customer_anomaly(
      CAST(days_since_login AS INT),
      CAST(Ebooks_Downloaded_6_Months AS INT),
      CAST(Average_Session_Time AS FLOAT),
      CAST(Days_Since_Last_Activity AS FLOAT),
      CAST(engagement_score AS DOUBLE),
      CAST(subscription_tier_numeric AS INT)
    ) AS BIGINT) = -1 THEN 'ANOMALY'
    ELSE 'NORMAL'
  END AS anomaly_status,
  current_timestamp() AS prediction_timestamp
FROM STREAM(LIVE.silver_customer_features) cf;