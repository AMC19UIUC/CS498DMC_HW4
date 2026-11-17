from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    spark = SparkSession.builder.getOrCreate()

    csv_file = "taxi_trips_clean.csv"
    print(f"Reading data from {csv_file}...")
    df = spark.read.csv(csv_file, header=True, inferSchema=True)

    df_transformed = df.withColumn(
        "fare_per_minute", 
        col("fare") / (col("trip_seconds") / 60.0)
    )

    df_transformed.createOrReplaceTempView("trips")

    df_comp_sum = spark.sql("SELECT company, COUNT(*) AS trip_count, ROUND(AVG(fare), 2) AS avg_fare, ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute FROM trips GROUP BY company ORDER BY trip_count DESC")

    output_dir = "processed_data"
    print(f"Saving results to {output_dir}/...")
    df_comp_sum.write.mode("overwrite").json(output_dir)
    
    spark.stop()

if __name__ == "__main__":
    main()