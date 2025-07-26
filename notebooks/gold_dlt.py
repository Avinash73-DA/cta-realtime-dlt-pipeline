import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="cta_project.gold.live_vehicle_counts_gold",
    comment="count of vehicles in each direction"
)

def live_vehicle_counts_gold():
    bus_df =spark.readStream.table("cta_project.silver.bus_silver")
    train_df = spark.readStream.table("cta_project.silver.train_silver")

    bus_agg_df = bus_df.groupBy("rt", "mode").agg(
    approx_count_distinct("vid").alias("active_vehicle"),
    sum(when(col("dly") == True, 1).otherwise(0)).alias("delays"))\
    .withColumn("updated_timestamp", current_timestamp())

    train_agg_df = train_df.groupBy(col("trDr").cast("string"),col("mode")).agg(
    approx_count_distinct(col("trdr")).alias("active_vehicle"),
    sum(when(col("isDly") == 1, 1).otherwise(0)).alias("delays"))\
    .withColumn("updated_timestamp", current_timestamp())

    final_df = bus_agg_df.unionAll(train_agg_df)

    return final_df

@dlt.table(
    name="cta_project.gold.hourly_route_performance_gold",
    comment="hourly performance of routes"
)

def hourly_route_performance_gold():
    bus_df =spark.readStream.table("cta_project.silver.bus_silver")
    train_df = spark.readStream.table("cta_project.silver.train_silver")

    train_agg_df = train_df.groupBy(col("mode"),col("trDr").cast("string").alias("route"),
                                    window(col("timestamp"), "60 minutes")).agg(
        (100 * (1 - (sum(col("isDly").cast("int")) / count(col("*"))))).alias("on_time_percentage"),
        count(col("*")).alias("total_vehicle_observations")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("route"),
        col("mode").alias("vehicle_type"),
        col("on_time_percentage"),
        col("total_vehicle_observations")
    )

    bus_agg_df = bus_df.groupBy(col("rt").cast("string").alias("route"),col("mode").alias("vehicle_type"),
        window(to_timestamp(concat(substring(col("tmstmp"), 1, 8), lit(" "), substring(col("tmstmp"), 10, 5)),"yyyyMMdd HH:mm"),"60 minutes")).agg(
        (100 * (1 - (sum(col("dly").cast("int")) / count("*")))).alias("on_time_percentage"),
        count("*").alias("total_vehicle_observations")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("route"),
        col("vehicle_type"),
        col("on_time_percentage"),
        col("total_vehicle_observations")
    )
    
    final_df = bus_agg_df.unionAll(train_agg_df)

    return final_df


@dlt.table(
    name="cta_project.gold.vehicle_positions_gold",
    comment="vehicle positions"
)

def vehicle_positions_gold():
    bus_df =spark.readStream.table("cta_project.silver.bus_silver")
    train_df = spark.readStream.table("cta_project.silver.train_silver")

    bus_agg_df = bus_df.select(
        col("vid").alias("vehicle_id"),
        col("mode").alias("vehicle_type"),
        (to_timestamp(concat(substring(col("tmstmp"), 1, 8), lit(" "), substring(col("tmstmp"), 10, 5)),"yyyyMMdd HH:mm")).alias("updated_timestamp"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude"),
        col("rt").cast("string").alias("route"),
        col("des").alias("heading"),
        col("dly").cast("string").alias("is_delayed"),
    )

    train_agg_df = train_df.select(
        col("rn").alias("vehicle_id"),
        col("mode").alias("vehicle_type"),
        col("timestamp"),
        col("lat").alias("latitude"),
        col("lon").alias("longitude"),
        col("trDr").cast("string").alias("route"),
        col("heading").cast("string"),
        when(col("isDly") == 1, "true").otherwise("false").alias("is_delayed")
    )

    df_final = bus_agg_df.unionAll(train_agg_df)

    return df_final

@dlt.table(
    name="cta_project.gold.train_arrival_accuracy_gold",
    comment="vehicle status"
)

def train_arrival_accuracy_gold():
    train_df = spark.readStream.table("cta_project.silver.train_silver")

    train_error_df = train_df.select(
        col("nextStaNm").alias("station_name"),
        col("trDr").alias("route"),
        (unix_timestamp("arrT") - unix_timestamp("prdt")).alias("avg_prediction_error_seconds"),
        current_timestamp().alias("last_calculated")
    )

    return train_error_df


@dlt.table(
  name="cta_project.gold.monthly_ridership_gold",
  comment="Monthly aggregated summary of bus, rail, and total rides.",
  table_properties={
        "pipelines.trigger.mode": "once"  # disables incremental processing
    }
)
def monthly_ridership_summary():
    df = spark.table("cta_project.silver.historical_silver")
    
    df_cleaned = df.withColumn("service_date", to_date(col("service_date"), "MM/dd/yyyy"))
    
    return df_cleaned.withColumn("year_month", date_format("service_date", "yyyy-MM")) \
        .groupBy("year_month") \
        .agg(
            sum(col("bus").cast("double")).alias("monthly_bus_rides"),
            sum(col("rail_boardings").cast("double")).alias("monthly_rail_rides"),
            sum(col("total_rides").cast("double")).alias("monthly_total_rides"),
        )

@dlt.table(
  name="cta_project.gold.yearly_ridership_growth_gold",
  comment="Yearly ridership totals for buses, rail, and overall rides.",
  table_properties={
        "pipelines.trigger.mode": "once"  # disables incremental processing
    }
)
def yearly_ridership_growth():
    df = spark.table("cta_project.silver.historical_silver")
    
    df_cleaned = df.withColumn("service_date", to_date(col("service_date"), "MM/dd/yyyy"))
    
    return df_cleaned.withColumn("year", year("service_date")) \
        .groupBy("year") \
        .agg(
            sum(col("bus").cast("double")).alias("yearly_bus_rides"),
            sum(col("rail_boardings").cast("double")).alias("yearly_rail_rides"),
            sum(col("total_rides").cast("double")).alias("yearly_total_rides"),
        )

@dlt.table(
    name="cta_project.gold.train_gold",
    comment="full train gold"
)

def train_gold():
    df_train = spark.readStream.table("cta_project.silver.train_silver")

    df_cleaned = df_train.select(
            col("rn"),
            col("destSt"),
            col("destNm"),
            col("trDr"),
            col("nextStaId"),
            col("nextStaNm"),
            col("prdt"),
            col("arrT"),
            col("isApp"),
            col("isDly"),
            col("lat"),
            col("lon"),
            col("heading")
        )\
            .withColumn("updated_timestamp", current_timestamp())

    return df_cleaned


@dlt.table(
    name="cta_project.gold.bus_gold",
    comment="full train gold"
)

def bus_gold():
    df_bus = spark.readStream.table("cta_project.silver.bus_silver")

    df_cleaned = df_bus.select(
            col("vid"),
            col("lat"),
            col("lon"),
            col("hdg"),
            col("rt"),
            col("des"),
            col("pdist"),
            col("dly"),
            col("origtatripno"),
        )\
            .withColumn("updated_timestamp", current_timestamp())

    return df_cleaned
