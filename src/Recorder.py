
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext ,SparkSession
import time

class Recorder:

    def record(self ,version=1 ,freq=1 ,input_path='./tick.csv' ,output_path='./min_bar'):
        """
        :param version: 版本号
        :param freq: 刷新频率 默认1s mock1分钟刷新
        :param input_path: 读路径
        :param output_path: 写路径
        """
        sc = SparkContext(appName="recorder " +str(version))
        sqlContext = SQLContext(sc)

        while True:
            df = sqlContext.read.csv(input_path, header=True)
            # print(df.schema)
            sqlContext.registerDataFrameAsTable(df, "tick")
            sql = "SELECT ExchangeID ,InstrumentID ,TradingDay,date_format(UpdateTime,'HHmm') as HHmm,round(first(LastPrice),2) AS open_price ,round(last(LastPrice),2) AS close_price ,round(MIN(LowerLimitPrice),2) AS lower_price ,round(MAX(UpperLimitPrice),2) AS upper_price FROM tick GROUP BY ExchangeID ,InstrumentID ,TradingDay, date_format(UpdateTime,'HHmm') order by TradingDay desc ,HHmm desc"
            df2 = sqlContext.sql(sql)
            print(df2.show(20)) #展示最新的20条
            df2.write.csv(output_path, mode='overwrite', header=True)
            time.sleep(freq)


if __name__ =='__main__':
    recorder = Recorder()
    recorder.record()