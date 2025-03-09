from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, when, lower, lit, trim
from pyspark.sql.types import StringType
import pandas as pd

def process_azure_billing():
    spark = SparkSession.builder.appName("AzureBilling").getOrCreate()
    
    df_costs = spark.read.csv("/home/vangajic/airflow/EA-Cost-Actual.csv", header=True, inferSchema=True)
    df_prices = spark.read.csv("/home/vangajic/airflow/EA-Prices.csv", header=True, inferSchema=True)
    
    url = "https://raw.githubusercontent.com/finopsfoundation/focus_converters/dev/focus_converter_base/focus_converter/conversion_configs/azure/mapping_files/azure_category_mapping.csv"
    service_mapping_df = pd.read_csv(url)
    service_mapping_spark_df = spark.createDataFrame(service_mapping_df)
    
    df_costs_transformed = df_costs.select(
        col("MeterId").alias("CostMeterId"),
        col("AvailabilityZone").alias("AvailabilityZone"),
        col("CostInBillingCurrency").alias("BilledCost"),
        col("BillingAccountId").alias("BillingAccountId"),
        col("BillingAccountName").alias("BillingAccountName"),
        col("BillingCurrencyCode").alias("BillingCurrency"),
        from_utc_timestamp(to_timestamp(col("BillingPeriodEndDate"), "MM/dd/yyyy"), "UTC").alias("BillingPeriodEnd"),
        from_utc_timestamp(to_timestamp(col("BillingPeriodStartDate"), "MM/dd/yyyy"), "UTC").alias("BillingPeriodStart"),
        col("ChargeType"),
        col("Frequency"),
        col("benefitId"),
        col("UnitOfMeasure").alias("ConsumedUnit"),
        col("PricingModel"),
        col("ProductName").alias("ChargeDescription"),
        from_utc_timestamp(to_timestamp(col("Date"), "MM/dd/yyyy"), "UTC").alias("ChargePeriodEnd"),
        from_utc_timestamp(to_timestamp(col("BillingPeriodStartDate"), "MM/dd/yyyy"), "UTC").alias("ChargePeriodStart"),
        col("benefitId").alias("CommitmentDiscountId"),
        col("benefitName").alias("CommitmentDiscountName"),
        col("Quantity").alias("ConsumedQuantity"),
        col("CostInBillingCurrency").alias("EffectiveCost"),
        col("PublisherName").alias("Publisher"),
        col("ResourceLocation").alias("RegionId"),
        col("ResourceId").alias("ResourceId"),
        col("ResourceName").alias("ResourceName"),
        col("ConsumedService"),
        col("ConsumedService").alias("ServiceName"),
        col("SubscriptionId").alias("SubAccountId"),
        col("SubscriptionName").alias("SubAccountName"),
        col("PartNumber"),
        col("OfferID")
    ).withColumn(
        "ChargeCategory",
        when(col("ChargeType").isin(["Usage", "UnusedReservation", "UnusedSavingsPlan"]), "Usage")
        .when(col("ChargeType") == "Purchase", "Purchase")
        .when(col("ChargeType") == "Tax", "Tax")
        .otherwise("Adjustment")
    ).withColumn(
        "ChargeFrequency",
        when(col("Frequency") == "OneTime", "One-Time")
        .when(col("Frequency") == "Recurring", "Recurring")
        .when(col("Frequency") == "UsageBased", "Usage-Based")
        .otherwise("Other") 
    ).withColumn(
        "PricingCategory",
        when(col("PricingModel") == "OnDemand", "On-Demand")
        .when(col("PricingModel") == "Spot", "Dynamic")
        .when(col("PricingModel").isin(["Reservation", "Savings Plans"]), "Commitment Discount")
        .otherwise("Other")
    ).withColumn(
        "CommitmentDiscountCategory",
        when(lower(col("benefitId")).contains("/microsoft.capacity/"), "Usage")
        .when(lower(col("benefitId")).contains("/microsoft.billingbenefits/"), "Spend")
        .otherwise(None)
    ).withColumn(
        "CommitmentDiscountType",
        when(lower(col("benefitId")).contains("/microsoft.capacity/"), "Reservation")
        .when(lower(col("benefitId")).contains("/microsoft.billingbenefits/"), "Savings Plan")
        .otherwise(None)
    ).withColumn(
        "InvoiceIssuer", lit("Microsoft")
    ).withColumn(
        "Provider", lit("Azure")
    ).withColumn(
        "SkuPriceId", lit(None)
    ).drop("ChargeType", "Frequency", "benefitId", "UnitOfMeasure", "PricingModel")

    df = df_costs_transformed.join(
        service_mapping_spark_df,
        df_costs_transformed["ConsumedService"] == service_mapping_spark_df["ConsumedService"],
        "left"
    ).select(
        df_costs_transformed["*"],
        service_mapping_spark_df["ServiceCategory"]
    ).drop("ConsumedService")

    joined_df = df_costs.alias("c").join(
        df_prices.alias("p"),
        (trim(col("c.MeterId")).cast(StringType()) == trim(col("p.MeterID")).cast(StringType())) &
        (trim(col("c.BillingCurrencyCode")).cast(StringType()) == trim(col("p.CurrencyCode")).cast(StringType())) &
        (trim(col("c.PartNumber")).cast(StringType()) == trim(col("p.PartNumber")).cast(StringType())) & 
        (trim(col("c.OfferID")).cast(StringType()) == trim(col("p.OfferID")).cast(StringType())),
        "left"
    ).select(
        col("c.MeterId"),
        col("c.BillingCurrencyCode"),
        col("c.PartNumber").alias("CostPartNumber"),
        col("c.OfferId").alias("CostOfferId"),
        col("c.Quantity").alias("CostQuantity"),
        col("c.UnitPrice").alias("CostUnitPrice"),
        col("p.UnitPrice").alias("PriceUnitPrice"),
        col("p.UnitOfMeasure").alias("PriceUnitOfMeasure"),
        col("p.MeterType").alias("PriceMeterType"),
        col("p.MeterRegion").alias("PriceMeterRegion"),
        col("p.ServiceFamily").alias("PriceServiceFamily"),
        col("p.SkuID").alias("PriceSkuID")
    ).withColumn(
        "ContractedCost", col("CostQuantity") * col("CostUnitPrice")
    ).withColumn(
        "ContractedUnitPrice", col("CostUnitPrice")
    ).withColumn(
        "ListCost", col("CostQuantity") * col("PriceUnitPrice")
    ).withColumn(
        "ListUnitPrice", col("PriceUnitPrice")
    ).withColumn(
        "PricingQuantity", col("PriceUnitOfMeasure")
    ).withColumn(
        "PricingUnit", col("PriceMeterType")
    ).withColumn(
        "RegionName", col("PriceMeterRegion")
    ).withColumn(
        "ResourceType", col("PriceServiceFamily")
    ).withColumn(
        "SkuId", col("PriceSkuID")
    ).drop(
        "CostQuantity",
        "CostUnitPrice",
        "PriceUnitPrice",
        "PriceUnitOfMeasure",
        "PriceMeterType",
        "PriceMeterRegion",
        "PriceServiceFamily",
        "PriceSkuID"
    )

    df_focus = df.join(
        joined_df.dropDuplicates(["MeterId", "BillingCurrencyCode", "CostPartNumber", "CostOfferId"]),
        (trim(col("CostMeterId")).cast(StringType()) == trim(col("MeterId")).cast(StringType())) &
        (trim(col("BillingCurrency")).cast(StringType()) == trim(col("BillingCurrencyCode")).cast(StringType())) &
        (trim(col("PartNumber")).cast(StringType()) == trim(col("CostPartNumber")).cast(StringType())) & 
        (trim(col("OfferID")).cast(StringType()) == trim(col("CostOfferId")).cast(StringType())),
        "left"
    ).select(
        "ContractedCost",
        "ContractedUnitPrice",
        "ListCost",
        "ListUnitPrice",
        "PricingQuantity",
        "PricingUnit",
        "RegionName",
        "ResourceType",
        "SkuId",
        "AvailabilityZone",
        "BilledCost",
        "BillingAccountId",
        "BillingAccountName",
        "BillingCurrency",
        "BillingPeriodEnd",
        "BillingPeriodStart",
        "ChargeCategory",
        "ChargeDescription",
        "ChargeFrequency",
        "ChargePeriodEnd",
        "ChargePeriodStart",
        "CommitmentDiscountCategory",
        "CommitmentDiscountId",
        "CommitmentDiscountName",
        "CommitmentDiscountType",
        "ConsumedQuantity",
        "ConsumedUnit",
        "EffectiveCost",
        "InvoiceIssuer",
        "PricingCategory",
        "Provider",
        "Publisher",
        "RegionId",
        "ResourceId",
        "ResourceName",
        "ServiceCategory",
        "ServiceName",
        "SkuPriceId",
        "SubAccountId",
        "SubAccountName"
    )

    # Save FOCUS format
    df_focus.toPandas().to_csv("/home/vangajic/airflow/focus_billing.csv", index=False)
    

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 6),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "azure_billing_pipeline",
    default_args=default_args,
    description="Azure Billing to FOCUS Pipeline",
    schedule_interval="0 6 * * *",  # Runs daily at 6 AM UTC
)

process_task = PythonOperator(
    task_id="process_azure_billing",
    python_callable=process_azure_billing,
    dag=dag,
)

process_task