from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType

from test_pytest import spark


def get_account_schema():
    return (
        StructType([
            StructField("load_date", StringType(), True),
            StructField("active_ind", IntegerType(), True),
            StructField("account_id",StringType(), True),
            StructField("source_sys", StringType(), True),
            StructField("account_start_date", TimestampType(), True),
            StructField("legal_title_1", StringType(), True),
            StructField("legal_title_2", StringType(), True),
            StructField("tax_id_type", StringType(), True),
            StructField("tax_id", StringType(), True),
            StructField("branch_code", StringType(), True),
            StructField("country", StringType(), True)
        ])
    )

def get_party_schema():
    return (
        StructType([
            StructField("load_date", DateType(), True),
            StructField("account_id", StringType(), True),
            StructField("party_id", StringType(), True),
            StructField("relation_type", StringType(), True),
            StructField("relation_start_date", TimestampType(), True)
        ])
    )

def get_address_schema():
    return (
        StructType([
            StructField("load_date", DateType(), True),
            StructField("party_id", StringType(), True),
            StructField("address_line_1", StringType(), True),
            StructField("address_line_2", StringType(), True),
            StructField("city", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country_of_address", StringType(), True),
            StructField("address_start_date", DateType(), True)
        ])
    )

def read_account_data(spark):

    input_file = "test_data/accounts/account_samples.csv"
    schema = get_account_schema()

    return (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .schema(schema)
        .csv(input_file)
    )

def read_party_data(spark):

    input_file = "test_data/parties/party_samples.csv"
    schema = get_party_schema()

    return (
        spark.read
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
        .schema(schema)
        .csv(input_file)
    )

def read_address_data(spark):

    input_file = "test_data/party_address/address_samples.csv"
    schema = get_address_schema()

    return (
        spark.read
        .option("header", "true")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(input_file)
    )