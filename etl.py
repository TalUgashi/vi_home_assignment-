from pyspark.sql.functions import col, avg, round, to_date, date_format, stddev, date_sub
from pyspark.sql.window import Window


class ETL:
  def __init__(self, s3_path,  df_stocks):
    self.s3_path = s3_path
    self.df = df_stocks

  def avg_daily(self):
      df_avg_tmp = self.df.withColumn('percentage_difference', (col('close') / col('open') - 1) * 100)
      df_avg_tmp2 = df_avg_tmp.groupBy('date').agg(avg('percentage_difference').alias('average_return')).sort('date')
      df_avg_tmp3 = (df_avg_tmp2.withColumn('average_return', round(col('average_return'), 3)).
                withColumn('date', to_date(col('date'), 'MM/d/yyyy')))
      df_avg = df_avg_tmp3.withColumn('date', date_format(col('date'), 'yyyy-MM-dd'))

      df_avg.show()
      """ Save to S3 Bucket"""
      s3_path = f'{self.s3_path}/avg_daily'
      df_avg.coalesce(1).write.option('header', 'true').mode('overwrite').csv(s3_path)

  def ticker_most_freq(self):
      df_freq_tmp = self.df.withColumn('frequency', col('close') * col('volume'))
      df_freq_tmp2 = df_freq_tmp.groupBy('ticker').agg(avg('frequency').alias('frequency')).sort(col('frequency').desc())
      df_freq = df_freq_tmp2.withColumn('frequency', col('frequency').cast('long')).limit(1)

      df_freq.show()
      """ Save to S3 Bucket"""
      s3_path = f'{self.s3_path}/ticker_most_freq'
      df_freq.coalesce(1).write.option("header", "true").mode("overwrite").csv(s3_path)

  def ticker_most_volatile(self):
      df_sd_tmp = self.df.withColumn('percentage_difference', col('close') / col('open') - 1)
      df_sd_tmp2 = (df_sd_tmp.groupBy('ticker').agg(stddev('percentage_difference').alias('standard deviation')).
                     sort(col('standard deviation').desc()))
      df_sd = df_sd_tmp2.limit(1)

      df_sd.show()
      """ Save to S3 Bucket"""
      s3_path = f'{self.s3_path}/ticker_most_volatile'
      df_sd.coalesce(1).write.option("header", "true").mode("overwrite").csv(s3_path)

  def top_three_returns(self):
      df_orig = self.df.withColumn('date', date_format(to_date(col('date'), 'MM/d/yyyy'), 'yyyy-MM-dd'))
      df_month_tmp = (df_orig.select(col('date'), col('close'), col('ticker')).withColumn('date_30_days', date_sub(col('date'), 30))
                      .withColumnRenamed('date', 'date_after_30_days').withColumnRenamed('close', 'close_after_30_days').
                      withColumnRenamed('ticker', 'ticker_after_30_days'))
      df_month_tmp2 = df_orig.join(df_month_tmp, (df_month_tmp.date_30_days == df_orig.date) & (df_month_tmp.ticker_after_30_days == df_orig.ticker), 'inner')
      df_month_tmp3 = df_month_tmp2.withColumn('return', col('close_after_30_days')/col('close') - 1).sort(col('return').desc())
      df_month = (df_month_tmp3.limit(3).select(col('ticker'), col('date'), col('date_after_30_days')).withColumnRenamed('date', 'from_date').
       withColumnRenamed('date_after_30_days', 'to_date'))

      df_month.show()
      """ Save to S3 Bucket"""
      s3_path = f'{self.s3_path}/top_three_returns'
      df_month.coalesce(1).write.option("header", "true").mode("overwrite").csv(s3_path)
