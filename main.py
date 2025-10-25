import sys
from lib import utils, data_loader
from lib.logger import Log4J
from lib.transformers import join_party_address, join_account_party, final_struct

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: python main.py : Arguments missing")
        sys.exit(1)

    job_run_env = sys.argv[1].upper()
    load_data = sys.argv[2]

    spark = utils.get_spark_session(job_run_env)
    logger = Log4J(spark)
    logger.info("Starting Spark Session")

    logger.info("Extracting Account Data")
    account_df = data_loader.read_account_data(spark)

    logger.info("Extracting Parties Data")
    party_df = data_loader.read_party_data(spark)

    logger.info("Extracting Address Data")
    address_df = data_loader.read_address_data(spark)

    logger.info("Joining Party and Address Data")
    join_party_address = join_party_address(party_df, address_df)

    logger.info("Joining Account Data with new join address")
    join_party_account = join_account_party(account_df, join_party_address)

    logger.info("Applying Final Json Structure")
    final_json_df = final_struct(join_party_account)

    (
        final_json_df.write
        .format("json")
        .mode("overwrite")
        .save("output/")
    )

    logger.info("Finished Creating Spark Session")