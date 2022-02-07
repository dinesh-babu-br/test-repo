// Databricks notebook source
dbutils.fs.put("dbfs:/FileStore/tables/business_options_data.json", """[

{  

   "new_option_id": 333,   
   "business" : "toy",
   "options":{ 

   "pos_mde":{ 

      "codesets":{ 

         "name":"toy_a_customgeo_germanyregion", 

         "description":"LEVEl0_CODESET of German Region in Toys Custom Geographic Table (for Special Allocation)" 

      },
      "flags":{
         "name":"zipcode_flag", 

         "value":"LEVEl0_CODESET of German Region in Toys Custom Geographic Table (for Special Allocation)" 
      }

   },
   "pos_mes":{ 

      "codesets":{ 

         "name":"mes", 

         "description":"mes_desc" 

      }
      }

}, 

  "added_user": "Senthil Kumaran",  

  "added_date": "2021-11-29 4:30:00" 


} 

]""", true)  

// COMMAND ----------

val jsonDataDF = spark.read.option("multiline", "true").json("dbfs:/FileStore/tables/business_options_data.json")
display(jsonDataDF)

// COMMAND ----------

// MAGIC %sql
// MAGIC use test_db;
// MAGIC drop table if exists business_options_json;

// COMMAND ----------

jsonDataDF.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("business_options_json")

// COMMAND ----------

// MAGIC %sql
// MAGIC select struct(options) from business_options_json  where struct(options) in ({"pos_mde"})

// COMMAND ----------

// MAGIC %sql
// MAGIC select options.pos_mees from business_options_json;

// COMMAND ----------

// MAGIC %sql
// MAGIC describe formatted test_db.business_options_json;

// COMMAND ----------

// MAGIC %fs
// MAGIC ls dbfs:/user/hive/

// COMMAND ----------

// MAGIC %sql
// MAGIC select options_info.options.codesets.CODESETNAME from business_options_json where business='Toy' and options_info.data_source='pos' and options_info.frequency='mde'

// COMMAND ----------

// MAGIC %sql
// MAGIC select options_info.options.DIMCUSTOMGEOGRAPHICS.DIMCUSTOMGEONAME from business_options_json where business='Toy' and options_info.data_source='pos' and options_info.frequency='mde'

// COMMAND ----------

dbutils.fs.put("dbfs:/FileStore/tables/newTestjsonData.json", """[{
        "OPTION_ID": 120,
        "BUSINESS": "Toy",
        "OPTIONS_INFO":
        {
        "pos_mde":{
        "DATA_SOURCE": "pos",
        "FREQUENCY": "mde",
        "DIMCUSTOMGEOGRAPHICS": {
                "NAME": "ODS_TOYDIMCUSTOMGEOGRAPHICS",
                "DESCRIPTION": "Table Name for Toys Custom Geographic (for Special Allocation)"
                },
                "CODESETS": {
                        "NAME": "toy_a_customgeo_germanyregion",
                        "DESCRIPTION": "LEVEl0_CODESET of German Region in Toys Custom Geographic Table (for Special  Allocation)"
                }
        },"pos_cns":
        {
        "DATA_SOURCE": "pos",
        "FREQUENCY": "cns",
         "DIMCUSTOMGEOGRAPHICS": {
                "NAME": "ODS_NEWTOYDIMCUSTOMGEOGRAPHICS",
                "DESCRIPTION": "Table Name for Toys  Geographic (for Special Allocation)"
                },
                "CODESETS": {
                        "NAME": "toy_a_customgeo_spainregion",
                        "DESCRIPTION": "Spain Region in Toys Custom Geographic Table (for Special  Allocation)"
                }
        }
        }
}]""", true)

// COMMAND ----------

val jsonDataDF1 = spark.read.option("multiline", "true").json("dbfs:/FileStore/tables/newTestjsonData.json")
display(jsonDataDF1)

// COMMAND ----------

jsonDataDF1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").option("mergeSchema", "true").saveAsTable("business_options_json1")

// COMMAND ----------

// MAGIC %python
// MAGIC dbutils.widgets.text("key2","pos_cns")
// MAGIC print(dbutils.widgets.get("key2"))

// COMMAND ----------

// MAGIC %sql
// MAGIC select * from business_options_json1;

// COMMAND ----------

// MAGIC %sql
// MAGIC select options_info.${key2}.DIMCUSTOMGEOGRAPHICS.NAME from business_options_json1 where business='Toy'

// COMMAND ----------

// MAGIC %sql
// MAGIC select options_info.${key2}.CODESETS.NAME from business_options_json1 where business='Toy'

// COMMAND ----------

// MAGIC %sql
// MAGIC select options_info.key2.frequency from business_options_json1

// COMMAND ----------

// MAGIC %python
// MAGIC loopJsonDataDf = spark.sql("select * from business_options_json1")
// MAGIC display(loopJsonDataDf)

// COMMAND ----------

// MAGIC %python
// MAGIC loopJsonDataDf1 = loopJsonDataDf.rdd.map(lambda x: (x.business, x.options_info))
// MAGIC print(loopJsonDataDf1)

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC 
// MAGIC df = pd.DataFrame({
// MAGIC         "OPTION_ID": 120,
// MAGIC         "BUSINESS": "Toy",
// MAGIC         "OPTIONS_INFO":
// MAGIC         {
// MAGIC         "KEY1":{
// MAGIC         "DATA_SOURCE": "pos",
// MAGIC         "FREQUENCY": "mde",
// MAGIC         "DIMCUSTOMGEOGRAPHICS": {
// MAGIC                "DIMCUSTOMGEONAME": "ODS_TOYDIMCUSTOMGEOGRAPHICS",
// MAGIC                 "DESCRIPTION": "Table Name for Toys Custom Geographic (for Special Allocation)"
// MAGIC                 },
// MAGIC                 "CODESETS": {
// MAGIC                         "CODESETNAME": "toy_a_customgeo_germanyregion",
// MAGIC                         "CODESETDESCRIPTION": "LEVEl0_CODESET of German Region in Toys Custom Geographic Table (for Special  Allocation)"
// MAGIC                 }
// MAGIC         },"KEY2":
// MAGIC         {
// MAGIC         "DATA_SOURCE": "pos",
// MAGIC         "FREQUENCY": "mdase",
// MAGIC          "DIMCUSTOMGEOGRAPHICS": {
// MAGIC                "DIMCUSTOMGEONAME": "ODS_aadTOYDIMCUSTOMGEOGRAPHICS",
// MAGIC                 "DESCRIPTION": "Table Name for Toys  Geographic (for Special Allocation)"
// MAGIC                 },
// MAGIC                 "CODESETS": {
// MAGIC                         "CODESETNAME": "aaatoy_a_customgeo_germanyregion",
// MAGIC                         "CODESETDESCRIPTION": "German Region in Toys Custom Geographic Table (for Special  Allocation)"
// MAGIC                 }
// MAGIC         }
// MAGIC         }
// MAGIC })
// MAGIC print(df.index)
// MAGIC for index in df.index:
// MAGIC    print(df['OPTIONS_INFO'][index])
// MAGIC   

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC for row in df.itertuples:
// MAGIC     print(getattr[row,'OPTIONS_INFO'])

// COMMAND ----------

// MAGIC %sql
// MAGIC show create table test_db.business_options_json1;

// COMMAND ----------

// MAGIC %sql
// MAGIC use test_db;
// MAGIC drop table if exists test_db.test_json1;
// MAGIC CREATE TABLE `test_db`.`test_json` (
// MAGIC   `BUSINESS` STRING,
// MAGIC   `OPTIONS_INFO` STRUCT<`pos_cns`: STRUCT<`CODESETS`: STRUCT<`DESCRIPTION`: STRING, `NAME`: STRING>, `DATA_SOURCE`: STRING, `DIMCUSTOMGEOGRAPHICS`: STRUCT<`DESCRIPTION`: STRING, `NAME`: STRING>, `FREQUENCY`: STRING>, `pos_mde`: STRUCT<`CODESETS`: STRUCT<`DESCRIPTION`: STRING, `NAME`: STRING>, `DATA_SOURCE`: STRING, `DIMCUSTOMGEOGRAPHICS`: STRUCT<`DESCRIPTION`: STRING, `NAME`: STRING>, `FREQUENCY`: STRING>>,
// MAGIC   `OPTION_ID` BIGINT)
// MAGIC USING delta

// COMMAND ----------

// MAGIC %sql
// MAGIC describe test_db.test_json1;

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists test_db.business_options;

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists test_db.business_options;
// MAGIC CREATE TABLE `test_db`.`business_options` (
// MAGIC   `option_id` BIGINT,
// MAGIC   `business` STRING,
// MAGIC   `option_info` STRUCT<>,
// MAGIC   `ADDED_USER` STRING,
// MAGIC   `ADDED_DATE` STRING,
// MAGIC   `UPDATED_USER` STRING,
// MAGIC   `UPDATED_DATE` STRING
// MAGIC   )
// MAGIC USING delta

// COMMAND ----------

// MAGIC %sql
// MAGIC describe test_db.business_options;

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO test_db.business_options
// MAGIC (option_id,business,option_info,ADDED_USER,ADDED_DATE,UPDATED_USER,UPDATED_DATE)
// MAGIC values(1,'toy',STRUCT({
// MAGIC    pos_mde:{
// MAGIC       DATA_SOURCE:"pos",
// MAGIC       FREQUENCY:"mde",
// MAGIC       DIMCUSTOMGEOGRAPHICS:{
// MAGIC          NAME:"ODS_TOYDIMCUSTOMGEOGRAPHICS",
// MAGIC          DESCRIPTION:"Table Name for Toys Custom Geographic (for Special Allocation)"
// MAGIC       },
// MAGIC       CODESETS:{
// MAGIC          NAME:"toy_a_customgeo_germanyregion",
// MAGIC          DESCRIPTION:"LEVEl0_CODESET of German Region in Toys Custom Geographic Table (for Special  Allocation)"
// MAGIC       }
// MAGIC    },
// MAGIC    pos_cns:{
// MAGIC       DATA_SOURCE:"pos",
// MAGIC       FREQUENCY:"cns",
// MAGIC       DIMCUSTOMGEOGRAPHICS:{
// MAGIC          NAME:"ODS_NEWTOYDIMCUSTOMGEOGRAPHICS",
// MAGIC          DESCRIPTION:"Table Name for Toys  Geographic (for Special Allocation)"
// MAGIC       },
// MAGIC       CODESETS:{
// MAGIC          NAME:"toy_a_customgeo_spainregion",
// MAGIC          DESCRIPTION:"Spain Region in Toys Custom Geographic Table (for Special  Allocation)"
// MAGIC       }
// MAGIC    }
// MAGIC }), 'Dinesh Babu',NOW(),'Dinesh Babu',NOW())

// COMMAND ----------

