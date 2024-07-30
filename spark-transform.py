from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T

# Kafka 설정
topic_name = "bike-station-info"
bootstrap_servers = "172.31.30.11:9092,172.31.39.201:9092,172.31.52.183:9092"

# Spark 세션 생성
spark = (
    SparkSession
    .builder
    .appName("kafka_streaming")
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0')
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")  # 경고 비활성화
    .master("local[*]")
    .getOrCreate()
)

# Kafka 메시지의 값을 읽기
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", bootstrap_servers)
    .option("subscribe", topic_name)
    .option("startingOffsets", "earliest")
    .load()
)

# Kafka 메시지의 값은 바이트 배열이므로 문자열로 변환
kafka_df = kafka_df.selectExpr("CAST(value AS STRING) as value")

# JSON 문자열을 StructType으로 변환하기 위한 스키마 정의
schema = T.StructType([
    T.StructField("rackTotCnt", T.StringType()),
    T.StructField("stationName", T.StringType()),
    T.StructField("parkingBikeTotCnt", T.StringType()),
    T.StructField("shared", T.StringType()),
    T.StructField("stationLatitude", T.StringType()),
    T.StructField("stationLongitude", T.StringType()),
    T.StructField("stationId", T.StringType()),
    T.StructField("timestamp", T.StringType())
])

# JSON 문자열을 DataFrame으로 변환
json_df = kafka_df.select(F.from_json(F.col("value"), schema).alias("data")).select("data.*")

# 이벤트 타임스탬프 형식 변환 및 Null 값 확인
json_df = json_df.withColumn("event_time", F.to_timestamp("timestamp", "yyyy-MM-dd HH:mm:ss"))

# 카프카에서 들어오는 데이터 그대로 출력
query_kafka_data = (
    json_df.writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", False)
    .start()
)

# 10분 단위로 데이터 집계
windowed_df = json_df.groupBy(
    F.window("event_time", "10 minutes"),
    "stationId"
).agg(
    F.first("parkingBikeTotCnt").alias("first_parkingBikeTotCnt"),
    F.last("parkingBikeTotCnt").alias("last_parkingBikeTotCnt")
)

# 증감 계산
windowed_df = windowed_df.withColumn("change", F.col("last_parkingBikeTotCnt") - F.col("first_parkingBikeTotCnt"))
windowed_df = windowed_df.withColumn("rental", F.when(F.col("change") < 0, -F.col("change")).otherwise(None))
windowed_df = windowed_df.withColumn("return", F.when(F.col("change") > 0, F.col("change")).otherwise(None))

# 10분 단위로 누적된 증감 데이터 계산
windowed_10min_df = windowed_df.groupBy(
    "window",
    "stationId"
).agg(
    F.sum("change").alias("total_change"),
    F.sum("rental").alias("total_rental"),
    F.sum("return").alias("total_return")
)

# 카프카 스트리밍 데이터의 행 개수 출력
query_kafka_count = (
    json_df.writeStream
    .outputMode("append")
    .format("console")
    .foreachBatch(lambda df, epochId: print(f"Kafka Stream Row Count: {df.count()}"))
    .start()
)

# 10분 단위 집계 데이터의 행 개수 출력
query_10min_count = (
    windowed_10min_df.writeStream
    .outputMode("complete")  # Complete 모드로 설정
    .format("console")
    .option("truncate", False)
    .trigger(processingTime="10 minutes")  # 10분 단위 트리거 설정
    .foreachBatch(lambda df, epochId: print(f"10 Minute Window Row Count: {df.count()}"))
    .start()
)

query_kafka_data.awaitTermination()
query_kafka_count.awaitTermination()
query_10min_count.awaitTermination()