import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name="cta_project.silver.train_silver",
    comment="Polished train data from bronze layer"
)
#Quality Check
@dlt.expect_or_drop("valid_latitude", "lat IS NOT NULL AND lat BETWEEN -90 AND 90")
@dlt.expect_or_drop("valid_longitude", "lon IS NOT NULL AND lon BETWEEN -180 AND 180")
@dlt.expect_or_drop("valid_rn","rn IS NOT NULL")

def train_silver():

    # Define schema
    schema = StructType([
        StructField("tmst", StringType()),
        StructField("errCd", StringType()),
        StructField("errNm", StringType()),
        StructField("route", ArrayType(
            StructType([
                StructField("@name", StringType()),
                StructField("train", ArrayType(
                    StructType([
                        StructField("rn", StringType()),
                        StructField("destSt", StringType()),
                        StructField("destNm", StringType()),
                        StructField("trDr", StringType()),
                        StructField("nextStaId", StringType()),
                        StructField("nextStpId", StringType()),
                        StructField("nextStaNm", StringType()),
                        StructField("prdt", StringType()),  # Keep as String first
                        StructField("arrT", StringType()),  # Keep as String first
                        StructField("isApp", StringType()),
                        StructField("isDly", StringType()),
                        StructField("flags", StringType()),
                        StructField("lat", StringType()),
                        StructField("lon", StringType()),
                        StructField("heading", StringType())
                    ])
                ))
            ])
        ))
    ])

    # Read bronze stream
    df = spark.readStream.table("cta_project.bronze.train_bronze")

    # Parse JSON column
    df_parsed = df.withColumn("parsed", from_json(col("ctatt"), schema))

    # Explode routes
    df_routes = df_parsed.select(
        col("parsed.tmst").alias("timestamp"),
        explode(col("parsed.route")).alias("route"),
        col("_rescued_data")
    )

    # Explode trains inside routes
    df_trains = df_routes.select(
        col("timestamp"),
        explode(col("route.train")).alias("train"),
        col("_rescued_data")
    )

    # Final selection with casting
    df_final = df_trains.select(
        col("timestamp").cast("timestamp"),
        col("train.rn").cast("double").alias("rn"),
        col("train.destSt").cast("double").alias("destSt"),
        col("train.destNm"),
        col("train.trDr").cast("double").alias("trDr"),
        col("train.nextStaId").cast("double").alias("nextStaId"),
        col("train.nextStpId").cast("double").alias("nextStpId"),
        col("train.nextStaNm"),
        to_timestamp(col("train.prdt")).alias("prdt"),
        to_timestamp(col("train.arrT")).alias("arrT"),
        col("train.isApp").cast("double").alias("isApp"),
        col("train.isDly").cast("double").alias("isDly"),
        col("train.flags"),
        col("train.lat").cast("float").alias("lat"),
        col("train.lon").cast("float").alias("lon"),
        col("train.heading").cast("double").alias("heading"),
        col("_rescued_data")
    )\
        .withColumn("mode",lit("train"))

    return df_final

@dlt.table(
    name="cta_project.silver.bus_silver",
    comment="Polished bus data from bronze layer"
)

#Quality Check
@dlt.expect_or_drop("valid_tatripid", "tatripid NOT LIKE '%*%'")
@dlt.expect_or_drop("valid_vid", "vid IS NOT NULL")

def bus_silver():
    # Load bronze table
    df_s = spark.readStream.table("cta_project.bronze.bus_bronze")

    # Define schema_s for "vehicle"
    schema_s = StructType([
        StructField("vehicle", ArrayType(StructType([
            StructField("vid", StringType()),
            StructField("tmstmp", StringType()),
            StructField("lat", StringType()),
            StructField("lon", StringType()),
            StructField("hdg", StringType()),
            StructField("pid", StringType()),
            StructField("rt", StringType()),
            StructField("des", StringType()),
            StructField("pdist", StringType()),
            StructField("dly", StringType()),
            StructField("tatripid", StringType()),
            StructField("origtatripno", StringType()),
            StructField("tablockid", StringType()),
            StructField("zone", StringType()),
        ]))),
        StructField("error", ArrayType(StructType([
            StructField("rt", StringType()),
            StructField("msg", StringType())
        ])))
    ])

    # Parse JSON from "bustime-response" column
    df_parsed_s = df_s.withColumn("parsed", from_json(col("`bustime-response`"), schema_s))\
                        .withColumn("_rescued_data", col("_rescued_data"))

    # Explode vehicle array
    df_vehicle_s = df_parsed_s.select(explode(col("parsed.vehicle")).alias("vehicle"),
                                    col("_rescued_data")
                                    )

    # Select and cast desired fields
    df_final_s = df_vehicle_s.select(col("vehicle.vid").cast("double").alias("vid"),
                                col("vehicle.tmstmp"),
                                col("vehicle.lat").cast("float").alias("lat"),
                                col("vehicle.lon").cast("float").alias("lon"),
                                col("vehicle.hdg").cast("double").alias("hdg"),
                                col("vehicle.pid").cast("double").alias("pid"),
                                col("vehicle.rt"),
                                col("vehicle.des"),
                                col("vehicle.pdist"),
                                col("vehicle.dly").cast("boolean").alias("dly"),
                                col("vehicle.tatripid").cast("double").alias("tatripid"),
                                col("vehicle.origtatripno").cast("double").alias("origtatripno"),
                                col("vehicle.tablockid"),
                                col("vehicle.zone"),
                                col("_rescued_data")
                                )\
             .withColumn("mode",lit("bus"))

    return df_final_s

@dlt.table(
    name="cta_project.silver.historical_silver",
    comment="Polished historical data from bronze layer"
)

def historical_silver():
    df_h = spark.table("cta_project.bronze.bronze_historical")

    df_main = df_h.select(
        to_date(col("service_date"), "MM/dd/yyyy").alias("service_date"),
        col("day_type"),
        col("bus").cast("double").alias("bus"),
        col("rail_boardings").cast("double").alias("rail_boardings"),
        col("total_rides").cast("double").alias("total_rides")
    )\
                    .withColumn("timestamp", current_timestamp())\


    return df_main
