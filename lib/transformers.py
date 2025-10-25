from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import collect_list, struct, col, concat_ws, expr, lit


def join_party_address(par,add):
    return (
        par.join(add,"party_id","left")
        .withColumn("full_address", concat_ws(", ", col("address_line_1"),col("address_line_2")))
        .groupBy("account_id")
        .agg(collect_list(struct(
            col("party_id"),
            col("relation_type"),
            col('relation_start_date'),
            col('full_address').alias("address"),
            col('city'),
            col('postal_code'),
            col('country_of_address'),
        )).alias("partyRelations"))
    )

def join_account_party(acc,par):
    return (
        acc.join(par,"account_id","left")
    )

def final_struct(df):

    header_col = struct(
        expr("uuid()").alias("eventIdentifier"),
        lit("app-contract").alias("eventType"),
        lit(1).alias("majorSchemaVersion"),
        lit(0).alias("minorSchemaVersion"),
        current_timestamp().alias("eventDateTime")
    ).alias("eventHeader")

    keys_col = struct(
        lit("contractIdentifier").alias("keyField"),
        col("account_id").alias("keyValue")
    ).alias("keys")

    payload_col = struct(
        col("account_id"),
        col("load_date"),
        col("active_ind"),
        col("source_sys"),
        col("account_start_date"),
        col("legal_title_1"),
        col("legal_title_2"),
        col("tax_id_type"),
        col("tax_id"),
        col("branch_code"),
        col("country"),
        col("partyRelations")
    ).alias("payload")

    return df.select(header_col, keys_col, payload_col)

