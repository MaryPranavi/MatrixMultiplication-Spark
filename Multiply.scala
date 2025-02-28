// Databricks notebook source
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder.appName("Read Text Files").getOrCreate()

// COMMAND ----------

val text_M = spark.read.text("dbfs:/FileStore/tables/M_matrix_small.txt")

// COMMAND ----------

val text_N = spark.read.text("dbfs:/FileStore/tables/N_matrix_small.txt")

// COMMAND ----------

val table_M = text_M.toDF.show
val table_N = text_N.toDF.show

// COMMAND ----------

val matrix_M = text_M.selectExpr(
    "CAST(SPLIT(value, ',')[0] AS INTEGER) AS row_idx",
    "CAST(SPLIT(value, ',')[1] AS INTEGER) AS col_idx",
    "CAST(SPLIT(value, ',')[2] AS FLOAT) AS value"
)

// COMMAND ----------

val matrix_N = text_N.selectExpr(
    "CAST(SPLIT(value, ',')[0] AS INTEGER) AS row_idx",
    "CAST(SPLIT(value, ',')[1] AS INTEGER) AS col_idx",
    "CAST(SPLIT(value, ',')[2] AS FLOAT) AS value"
)

// COMMAND ----------

matrix_M.createOrReplaceTempView("matrix_M")
matrix_N.createOrReplaceTempView("matrix_N")


// COMMAND ----------


val df_result = spark.sql("""SELECT
        m.row_idx,
        n.col_idx,
        SUM(m.value * n.value) AS value
    FROM
        matrix_M m
        JOIN
        matrix_N n
        ON m.col_idx = n.row_idx
    GROUP BY
        m.row_idx,
        n.col_idx
    ORDER BY
        m.row_idx,
        n.col_idx""")
df_result.show()
