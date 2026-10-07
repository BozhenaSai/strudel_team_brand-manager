# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold layer: Brand Manager, questions 1 and 2
# MAGIC
# MAGIC

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.brand_gold")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SOURCE = "workspace.brand_silver"
TARGET = "workspace.brand_gold"
BRAND = "Brand#32"

lineitem = spark.table(f"{SOURCE}.lineitem")
part = spark.table(f"{SOURCE}.part")
orders = spark.table(f"{SOURCE}.orders")
partsupp = spark.table(f"{SOURCE}.partsupp")

net_revenue = F.col("l_extendedprice") * (1 - F.col("l_discount"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold tables

# COMMAND ----------

# Revenue by category, brand and quarter
brand_revenue = (
    lineitem
    .join(part, lineitem.l_partkey == part.p_partkey)
    .join(orders, lineitem.l_orderkey == orders.o_orderkey)
    .groupBy(
        "p_mfgr",
        "p_brand",
        F.year("o_orderdate").alias("order_year"),
        F.quarter("o_orderdate").alias("order_quarter"),
    )
    .agg(
        F.sum(net_revenue).alias("revenue"),
        F.sum("l_quantity").alias("quantity"),
        F.count("*").alias("line_items"),
    )
)

# Margin by brand, over all supplier offers in partsupp
brand_margin = (
    part
    .join(partsupp, part.p_partkey == partsupp.ps_partkey)
    .withColumn("margin", F.col("p_retailprice") - F.col("ps_supplycost"))
    .groupBy("p_mfgr", "p_brand")
    .agg(
        F.avg("margin").alias("avg_margin"),
        F.avg(F.col("margin") / F.col("p_retailprice")).alias("avg_margin_ratio"),
        F.countDistinct("p_partkey").alias("parts"),
        F.count("*").alias("supplier_offers"),
    )
)

# COMMAND ----------


if TARGET:
    brand_revenue.write.mode("overwrite").saveAsTable(f"{TARGET}.brand_revenue")
    brand_margin.write.mode("overwrite").saveAsTable(f"{TARGET}.brand_margin")
    brand_revenue = spark.table(f"{TARGET}.brand_revenue")
    brand_margin = spark.table(f"{TARGET}.brand_margin")

display(brand_revenue.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1. Total revenue of Brand#32 and its share within its category

# COMMAND ----------

w = Window.partitionBy("p_mfgr")

brand_share = (
    brand_revenue
    .groupBy("p_mfgr", "p_brand")
    .agg(F.sum("revenue").alias("revenue"))
    .withColumn("share_pct", F.round(100 * F.col("revenue") / F.sum("revenue").over(w), 2))
)

q1 = brand_share.filter(F.col("p_brand") == BRAND)
display(q1)

# COMMAND ----------

# All brands of the same category
brand_mfgr = q1.first()["p_mfgr"]
display(brand_share.filter(F.col("p_mfgr") == brand_mfgr).orderBy(F.desc("revenue")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2. Average margin for products of Brand#32
# MAGIC
# MAGIC Two variants:
# MAGIC - **A**: average over all supplier offers in `partsupp`
# MAGIC - **B**: only what was actually bought (line items joined to `partsupp` on both keys)

# COMMAND ----------

# Variant A
q2_offers = brand_margin.filter(F.col("p_brand") == BRAND).withColumn(
    "avg_margin_pct", F.round(100 * F.col("avg_margin_ratio"), 2)
)
display(q2_offers)

# COMMAND ----------

# Variant B
part_brand = part.filter(F.col("p_brand") == BRAND)

q2_actual = (
    lineitem
    .join(part_brand, lineitem.l_partkey == part_brand.p_partkey)
    .join(
        partsupp,
        (lineitem.l_partkey == partsupp.ps_partkey)
        & (lineitem.l_suppkey == partsupp.ps_suppkey),
    )
    .withColumn("margin", F.col("p_retailprice") - F.col("ps_supplycost"))
    .agg(
        F.avg("margin").alias("avg_margin_per_line_item"),
        (F.sum(F.col("margin") * F.col("l_quantity")) / F.sum("l_quantity")).alias("avg_margin_per_unit"),
    )
)
display(q2_actual)

# COMMAND ----------

# Margin of all brands in the same category
display(
    brand_margin
    .filter(F.col("p_mfgr") == brand_mfgr)
    .select("p_brand", F.round("avg_margin", 2).alias("avg_margin"))
    .orderBy("p_brand")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

# V1. Revenue in gold equals revenue in the source, no rows lost
total_source = lineitem.agg(F.sum(net_revenue)).first()[0]
total_gold = brand_revenue.agg(F.sum("revenue")).first()[0]
rows_source = lineitem.count()
rows_gold = brand_revenue.agg(F.sum("line_items")).first()[0]

print(f"Revenue source: {float(total_source):,.2f}")
print(f"Revenue gold:   {float(total_gold):,.2f}")
print(f"Line items source: {rows_source:,}, in gold: {rows_gold:,}")

assert abs(total_source - total_gold) < 0.01, "Revenue does not match"
assert rows_source == rows_gold, "Some line items were lost in joins"
print("V1 passed")

# COMMAND ----------

# V2. Joining partsupp on both keys does not multiply rows (no double counting)
rows_both_keys = lineitem.join(
    partsupp,
    (lineitem.l_partkey == partsupp.ps_partkey) & (lineitem.l_suppkey == partsupp.ps_suppkey),
).count()
rows_partkey_only = lineitem.join(partsupp, lineitem.l_partkey == partsupp.ps_partkey).count()

print(f"Line items:              {rows_source:,}")
print(f"Join on both keys:       {rows_both_keys:,}")
print(f"Join on partkey only:    {rows_partkey_only:,} (x{rows_partkey_only / rows_source:.1f})")

assert rows_both_keys == rows_source, "Join on both keys changes the row count"
print("V2 passed")

# COMMAND ----------

# V3. Every brand belongs to exactly one category, so share within category is well defined
brands_in_many = (
    part.groupBy("p_brand")
    .agg(F.countDistinct("p_mfgr").alias("n_mfgr"))
    .filter("n_mfgr > 1")
    .count()
)
print(f"Brands with more than one manufacturer: {brands_in_many}")
assert brands_in_many == 0, "Some brand belongs to several manufacturers"
print("V3 passed")