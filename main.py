__version__ = '0.1'
__author__ = 'Tal Ugashi'

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from etl import ETL

conf = (
    SparkConf()
    .set("spark.jars.packages", "io.delta:delta-core_2.12:2.3.0,org.apache.hadoop:hadoop-aws:3.3.2")
    .set("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .set("spark.hadoop.fs.s3a.access.key", 'AKIATUJS4ZI6DMB7KRGA')
    .set("spark.hadoop.fs.s3a.secret.key", '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')
    .set("spark.sql.shuffle.partitions", "4") # default is 200 partitions which is too many for local
    .setMaster("local[*]") # replace the * with your desired number of cores. * for use all.
)

spark = (SparkSession.builder.appName('ViHomeAssignment').config(conf=conf).getOrCreate())

spark.sql('set spark.sql.legacy.timeParserPolicy=LEGACY')

s3_path = 's3a://aws-glue-home-assignment-tal'


def main():
    df_stocks = spark.read.options(header='True', inferSchema='True', delimiter=',').csv('stock_prices.csv')
    stocks = ETL(s3_path, df_stocks)
    stocks.avg_daily()
    stocks.ticker_most_freq()
    stocks.ticker_most_volatile()
    stocks.top_three_returns()


if __name__ == '__main__':
    main()
    spark.stop()
