import dlt
import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql.types import *

train_path = '/Volumes/cta_project/landing_volumes/trains'
bus_path = '/Volumes/cta_project/landing_volumes/buses'
historical_path = '/Volumes/cta_project/landing_volumes/historical_ridership/cta_ridership.csv'

@dlt.table(
    name="cta_project.bronze.train_bronze",
    comment="Raw Bronze layer Ingestion of streaming trains data"
)

def train_bronze():
    return( 
        spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","json")\
        .load(train_path)
    )

@dlt.table(
    name="cta_project.bronze.bus_bronze",
    comment="Raw Bronze layer Ingestion of streaming buses data"
)

def bus_bronze():
    return (
        spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","json")\
        .option("multiLine", "true")\
        .load(bus_path)

    )

@dlt.table(
  name="cta_project.bronze.bronze_historical",
  comment="Raw CSV data for historical ridership, ingested from the volume."
)
def bronze_historical_ridership():
      df = pd.read_csv(historical_path)
      df_spark = spark.createDataFrame(df)
      return df_spark


