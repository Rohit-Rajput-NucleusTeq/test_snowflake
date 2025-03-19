# Databricks notebook source
# MAGIC %run "./udf_informatica"

# COMMAND ----------


from pyspark.sql.types import *

spark.sql("use DELTA_TRAINING")
spark.sql("set spark.sql.legacy.timeParserPolicy = LEGACY")

# COMMAND ----------
%run ./MappingUtility

# COMMAND ----------
mainWorkflowId = dbutils.widgets.get("mainWorkflowId")
mainWorkflowRunId = dbutils.widgets.get("mainWorkflowRunId")
parentName = dbutils.widgets.get("parentName")
preVariableAssignment = dbutils.widgets.get("preVariableAssignment")
postVariableAssignment = dbutils.widgets.get("postVariableAssignment")
truncTargetTableOptions = dbutils.widgets.get("truncTargetTableOptions")
variablesTableName = dbutils.widgets.get("variablesTableName")

# COMMAND ----------
#Truncate Target Tables
truncateTargetTables(truncTargetTableOptions)

# COMMAND ----------
#Pre presession variable updation
updateVariable(preVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NUODATA_USER_TEST")

# COMMAND ----------
fetchAndCreateVariables(parentName,"m_NUODATA_USER_TEST", variablesTableName, mainWorkflowId)

# COMMAND ----------
# DBTITLE 1, Shortcut_to_TEST_USER_0 [Source Definition] 


query_0 = f"""SELECT
  UCL_USER_ID AS UCL_USER_ID,
  COMPANY_ID AS COMPANY_ID,
  USER_NAME AS USER_NAME,
  USER_PASSWORD AS USER_PASSWORD,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_SOURCE_TYPE_ID AS CREATED_SOURCE_TYPE_ID,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE_ID AS LAST_UPDATED_SOURCE_TYPE_ID,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  USER_TYPE_ID AS USER_TYPE_ID,
  LOCALE_ID AS LOCALE_ID,
  LOCATION_ID AS LOCATION_ID,
  USER_FIRST_NAME AS USER_FIRST_NAME,
  USER_MIDDLE_NAME AS USER_MIDDLE_NAME,
  USER_LAST_NAME AS USER_LAST_NAME,
  USER_PREFIX AS USER_PREFIX,
  USER_TITLE AS USER_TITLE,
  TELEPHONE_NUMBER AS TELEPHONE_NUMBER,
  FAX_NUMBER AS FAX_NUMBER,
  ADDRESS_1 AS ADDRESS_1,
  ADDRESS_2 AS ADDRESS_2,
  CITY AS CITY,
  STATE_PROV_CODE AS STATE_PROV_CODE,
  POSTAL_CODE AS POSTAL_CODE,
  COUNTRY_CODE AS COUNTRY_CODE,
  USER_EMAIL_1 AS USER_EMAIL_1,
  USER_EMAIL_2 AS USER_EMAIL_2,
  COMM_METHOD_ID_DURING_BH_1 AS COMM_METHOD_ID_DURING_BH_1,
  COMM_METHOD_ID_DURING_BH_2 AS COMM_METHOD_ID_DURING_BH_2,
  COMM_METHOD_ID_AFTER_BH_1 AS COMM_METHOD_ID_AFTER_BH_1,
  COMM_METHOD_ID_AFTER_BH_2 AS COMM_METHOD_ID_AFTER_BH_2,
  COMMON_NAME AS COMMON_NAME,
  LAST_PASSWORD_CHANGE_DTTM AS LAST_PASSWORD_CHANGE_DTTM,
  LOGGED_IN AS LOGGED_IN,
  LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
  DEFAULT_BUSINESS_UNIT_ID AS DEFAULT_BUSINESS_UNIT_ID,
  DEFAULT_WHSE_REGION_ID AS DEFAULT_WHSE_REGION_ID,
  CHANNEL_ID AS CHANNEL_ID,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  NUMBER_OF_INVALID_LOGINS AS NUMBER_OF_INVALID_LOGINS,
  TAX_ID_NBR AS TAX_ID_NBR,
  EMP_START_DATE AS EMP_START_DATE,
  BIRTH_DATE AS BIRTH_DATE,
  GENDER_ID AS GENDER_ID,
  PASSWORD_RESET_DATE_TIME AS PASSWORD_RESET_DATE_TIME,
  PASSWORD_TOKEN AS PASSWORD_TOKEN,
  ISPASSWORDMANAGEDINTERNALLY AS ISPASSWORDMANAGEDINTERNALLY,
  COPY_FROM_USER AS COPY_FROM_USER,
  EXTERNAL_USER_ID AS EXTERNAL_USER_ID,
  SECURITY_POLICY_GROUP_ID AS SECURITY_POLICY_GROUP_ID
FROM
  TEST_USER"""

df_0 = spark.sql(query_0)

df_0.createOrReplaceTempView("Shortcut_to_TEST_USER_0")

# COMMAND ----------
# DBTITLE 1, SQ_Shortcut_to_TEST_USER_1 [Source Qualifier] 


query_1 = f"""SELECT
  UCL_USER_ID AS UCL_USER_ID,
  COMPANY_ID AS COMPANY_ID,
  USER_NAME AS USER_NAME,
  USER_PASSWORD AS USER_PASSWORD,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_SOURCE_TYPE_ID AS CREATED_SOURCE_TYPE_ID,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE_ID AS LAST_UPDATED_SOURCE_TYPE_ID,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  USER_TYPE_ID AS USER_TYPE_ID,
  LOCALE_ID AS LOCALE_ID,
  LOCATION_ID AS LOCATION_ID,
  USER_FIRST_NAME AS USER_FIRST_NAME,
  USER_MIDDLE_NAME AS USER_MIDDLE_NAME,
  USER_LAST_NAME AS USER_LAST_NAME,
  USER_PREFIX AS USER_PREFIX,
  USER_TITLE AS USER_TITLE,
  TELEPHONE_NUMBER AS TELEPHONE_NUMBER,
  FAX_NUMBER AS FAX_NUMBER,
  ADDRESS_1 AS ADDRESS_1,
  ADDRESS_2 AS ADDRESS_2,
  CITY AS CITY,
  STATE_PROV_CODE AS STATE_PROV_CODE,
  POSTAL_CODE AS POSTAL_CODE,
  COUNTRY_CODE AS COUNTRY_CODE,
  USER_EMAIL_1 AS USER_EMAIL_1,
  USER_EMAIL_2 AS USER_EMAIL_2,
  COMM_METHOD_ID_DURING_BH_1 AS COMM_METHOD_ID_DURING_BH_1,
  COMM_METHOD_ID_DURING_BH_2 AS COMM_METHOD_ID_DURING_BH_2,
  COMM_METHOD_ID_AFTER_BH_1 AS COMM_METHOD_ID_AFTER_BH_1,
  COMM_METHOD_ID_AFTER_BH_2 AS COMM_METHOD_ID_AFTER_BH_2,
  COMMON_NAME AS COMMON_NAME,
  LAST_PASSWORD_CHANGE_DTTM AS LAST_PASSWORD_CHANGE_DTTM,
  LOGGED_IN AS LOGGED_IN,
  LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
  DEFAULT_BUSINESS_UNIT_ID AS DEFAULT_BUSINESS_UNIT_ID,
  DEFAULT_WHSE_REGION_ID AS DEFAULT_WHSE_REGION_ID,
  CHANNEL_ID AS CHANNEL_ID,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  NUMBER_OF_INVALID_LOGINS AS NUMBER_OF_INVALID_LOGINS,
  TAX_ID_NBR AS TAX_ID_NBR,
  EMP_START_DATE AS EMP_START_DATE,
  BIRTH_DATE AS BIRTH_DATE,
  GENDER_ID AS GENDER_ID,
  PASSWORD_RESET_DATE_TIME AS PASSWORD_RESET_DATE_TIME,
  PASSWORD_TOKEN AS PASSWORD_TOKEN,
  ISPASSWORDMANAGEDINTERNALLY AS ISPASSWORDMANAGEDINTERNALLY,
  COPY_FROM_USER AS COPY_FROM_USER,
  EXTERNAL_USER_ID AS EXTERNAL_USER_ID,
  SECURITY_POLICY_GROUP_ID AS SECURITY_POLICY_GROUP_ID,
  monotonically_increasing_id() AS Monotonically_Increasing_Id
FROM
  Shortcut_to_TEST_USER_0
WHERE
  {Initial_Load} (
    date_trunc('DAY', Shortcut_to_TEST_USER_0.CREATED_DTTM) >= date_trunc(
      'DAY',
      to_date(
        DATE_FORMAT(
          CAST('{Prev_Run_Dt}' AS timestamp),
          'MM/dd/yyyy HH:mm:ss'
        ),
        'MM/dd/yyyy HH:mm:ss'
      )
    ) - INTERVAL '1' DAY
  )
  OR (
    date_trunc('DAY', Shortcut_to_TEST_USER_0.LAST_UPDATED_DTTM) >= date_trunc(
      'DAY',
      to_date(
        DATE_FORMAT(
          CAST('{Prev_Run_Dt}' AS timestamp),
          'MM/dd/yyyy HH:mm:ss'
        ),
        'MM/dd/yyyy HH:mm:ss'
      )
    ) - INTERVAL '1' DAY
  )
  AND 1 = 1"""

df_1 = spark.sql(query_1)

df_1.createOrReplaceTempView("SQ_Shortcut_to_TEST_USER_1")

# COMMAND ----------
# DBTITLE 1, EXPTRANS_2 [Expression] 


query_2_1 = f"""SELECT
  UCL_USER_ID AS UCL_USER_ID,
  COMPANY_ID AS COMPANY_ID,
  USER_NAME AS USER_NAME,
  USER_PASSWORD AS USER_PASSWORD,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_SOURCE_TYPE_ID AS CREATED_SOURCE_TYPE_ID,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE_ID AS LAST_UPDATED_SOURCE_TYPE_ID,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  USER_TYPE_ID AS USER_TYPE_ID,
  LOCALE_ID AS LOCALE_ID,
  LOCATION_ID AS LOCATION_ID,
  USER_FIRST_NAME AS USER_FIRST_NAME,
  USER_MIDDLE_NAME AS USER_MIDDLE_NAME,
  USER_LAST_NAME AS USER_LAST_NAME,
  USER_PREFIX AS USER_PREFIX,
  USER_TITLE AS USER_TITLE,
  TELEPHONE_NUMBER AS TELEPHONE_NUMBER,
  FAX_NUMBER AS FAX_NUMBER,
  ADDRESS_1 AS ADDRESS_1,
  ADDRESS_2 AS ADDRESS_2,
  CITY AS CITY,
  STATE_PROV_CODE AS STATE_PROV_CODE,
  POSTAL_CODE AS POSTAL_CODE,
  COUNTRY_CODE AS COUNTRY_CODE,
  USER_EMAIL_1 AS USER_EMAIL_1,
  USER_EMAIL_2 AS USER_EMAIL_2,
  COMM_METHOD_ID_DURING_BH_1 AS COMM_METHOD_ID_DURING_BH_1,
  COMM_METHOD_ID_DURING_BH_2 AS COMM_METHOD_ID_DURING_BH_2,
  COMM_METHOD_ID_AFTER_BH_1 AS COMM_METHOD_ID_AFTER_BH_1,
  COMM_METHOD_ID_AFTER_BH_2 AS COMM_METHOD_ID_AFTER_BH_2,
  COMMON_NAME AS COMMON_NAME,
  LAST_PASSWORD_CHANGE_DTTM AS LAST_PASSWORD_CHANGE_DTTM,
  LOGGED_IN AS LOGGED_IN,
  LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
  DEFAULT_BUSINESS_UNIT_ID AS DEFAULT_BUSINESS_UNIT_ID,
  DEFAULT_WHSE_REGION_ID AS DEFAULT_WHSE_REGION_ID,
  CHANNEL_ID AS CHANNEL_ID,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  NUMBER_OF_INVALID_LOGINS AS NUMBER_OF_INVALID_LOGINS,
  TAX_ID_NBR AS TAX_ID_NBR,
  EMP_START_DATE AS EMP_START_DATE,
  BIRTH_DATE AS BIRTH_DATE,
  GENDER_ID AS GENDER_ID,
  PASSWORD_RESET_DATE_TIME AS PASSWORD_RESET_DATE_TIME,
  PASSWORD_TOKEN AS PASSWORD_TOKEN,
  ISPASSWORDMANAGEDINTERNALLY AS ISPASSWORDMANAGEDINTERNALLY,
  COPY_FROM_USER AS COPY_FROM_USER,
  EXTERNAL_USER_ID AS EXTERNAL_USER_ID,
  SECURITY_POLICY_GROUP_ID AS SECURITY_POLICY_GROUP_ID,
  Monotonically_Increasing_Id AS Monotonically_Increasing_Id
FROM
  SQ_Shortcut_to_TEST_USER_1"""

df_2_1 = spark.sql(query_2_1)

df_2_1.createOrReplaceTempView("EXPTRANS_2")

query_2_2 = f"""SELECT
  *,
  {LOCATION_ID} AS DC_NBR_EXP,
  current_timestamp() AS LOAD_TSTMP_EXP
FROM
  EXPTRANS_2"""

df_2_2 = spark.sql(query_2_2)

df_2_2.createOrReplaceTempView("EXPTRANS_2")

# COMMAND ----------
# DBTITLE 1, NUODATA_TEST_USER [Target Definition] 


spark.sql("""INSERT INTO
  NUODATA_TEST_USER
SELECT
  DC_NBR_EXP AS DC_NBR,
  UCL_USER_ID AS UCL_USER_ID,
  COMPANY_ID AS COMPANY_ID,
  USER_NAME AS USER_NAME,
  USER_PASSWORD AS USER_PASSWORD,
  IS_ACTIVE AS IS_ACTIVE,
  CREATED_SOURCE_TYPE_ID AS CREATED_SOURCE_TYPE_ID,
  CREATED_SOURCE AS CREATED_SOURCE,
  CREATED_DTTM AS CREATED_DTTM,
  LAST_UPDATED_SOURCE_TYPE_ID AS LAST_UPDATED_SOURCE_TYPE_ID,
  LAST_UPDATED_SOURCE AS LAST_UPDATED_SOURCE,
  LAST_UPDATED_DTTM AS LAST_UPDATED_DTTM,
  USER_TYPE_ID AS USER_TYPE_ID,
  LOCALE_ID AS LOCALE_ID,
  LOCATION_ID AS LOCATION_ID,
  USER_FIRST_NAME AS USER_FIRST_NAME,
  USER_MIDDLE_NAME AS USER_MIDDLE_NAME,
  USER_LAST_NAME AS USER_LAST_NAME,
  USER_PREFIX AS USER_PREFIX,
  USER_TITLE AS USER_TITLE,
  TELEPHONE_NUMBER AS TELEPHONE_NUMBER,
  FAX_NUMBER AS FAX_NUMBER,
  ADDRESS_1 AS ADDRESS_1,
  ADDRESS_2 AS ADDRESS_2,
  CITY AS CITY,
  STATE_PROV_CODE AS STATE_PROV_CODE,
  POSTAL_CODE AS POSTAL_CODE,
  COUNTRY_CODE AS COUNTRY_CODE,
  USER_EMAIL_1 AS USER_EMAIL_1,
  USER_EMAIL_2 AS USER_EMAIL_2,
  COMM_METHOD_ID_DURING_BH_1 AS COMM_METHOD_ID_DURING_BH_1,
  COMM_METHOD_ID_DURING_BH_2 AS COMM_METHOD_ID_DURING_BH_2,
  COMM_METHOD_ID_AFTER_BH_1 AS COMM_METHOD_ID_AFTER_BH_1,
  COMM_METHOD_ID_AFTER_BH_2 AS COMM_METHOD_ID_AFTER_BH_2,
  COMMON_NAME AS COMMON_NAME,
  LAST_PASSWORD_CHANGE_DTTM AS LAST_PASSWORD_CHANGE_DTTM,
  LOGGED_IN AS LOGGED_IN,
  LAST_LOGIN_DTTM AS LAST_LOGIN_DTTM,
  DEFAULT_BUSINESS_UNIT_ID AS DEFAULT_BUSINESS_UNIT_ID,
  DEFAULT_WHSE_REGION_ID AS DEFAULT_WHSE_REGION_ID,
  CHANNEL_ID AS CHANNEL_ID,
  HIBERNATE_VERSION AS HIBERNATE_VERSION,
  NUMBER_OF_INVALID_LOGINS AS NUMBER_OF_INVALID_LOGINS,
  TAX_ID_NBR AS TAX_ID_NBR,
  EMP_START_DATE AS EMP_START_DATE,
  BIRTH_DATE AS BIRTH_DATE,
  GENDER_ID AS GENDER_ID,
  PASSWORD_RESET_DATE_TIME AS PASSWORD_RESET_DATE_TIME,
  PASSWORD_TOKEN AS PASSWORD_TOKEN,
  ISPASSWORDMANAGEDINTERNALLY AS ISPASSWORDMANAGEDINTERNALLY,
  COPY_FROM_USER AS COPY_FROM_USER,
  EXTERNAL_USER_ID AS EXTERNAL_USER_ID,
  SECURITY_POLICY_GROUP_ID AS SECURITY_POLICY_GROUP_ID,
  LOAD_TSTMP_EXP AS LOAD_TSTMP
FROM
  EXPTRANS_2""")

# COMMAND ----------
#Post session variable updation
updateVariable(postVariableAssignment, variablesTableName, mainWorkflowId, parentName, "m_NUODATA_USER_TEST")

# COMMAND ----------
#Update Mapping Variables in database.
persistVariables(variablesTableName, "m_NUODATA_USER_TEST", mainWorkflowId, parentName)
