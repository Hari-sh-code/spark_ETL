from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == "LOCAL":
        return (
            SparkSession.builder
            .appName("app")
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:conf/log4j.properties")
            .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:conf/log4j.properties")
            .master("local[2]")
            .enableHiveSupport()
            .getOrCreate()
        )

    else:
        return (
            SparkSession.builder
            .enableHiveSupport()
            .getOrCreate()
        )