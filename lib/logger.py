import os

class Log4J:
    def __init__(self, spark):
        log4j = spark._jvm.org.apache.log4j
        root_class = "tyler.spark.app"
        conf = spark.sparkContext.getConf()
        app_name = conf.get("spark.app.name")
        props_file = os.path.abspath("conf/log4j.properties")
        if os.path.exists(props_file):
            log4j.PropertyConfigurator.configure(props_file)
            print(f"Loaded custom Log4J config from {props_file}")
        else:
            print(f"Log4J config not found at {props_file}, using default Spark config")

        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
