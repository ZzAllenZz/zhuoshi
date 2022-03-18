# import pandas as pd
#
# df = pd.read_parquet("../interview.parquet")
#
# print(len(df), df.columns)  # 获取数据条数和列名
# print(df.shape)  # 获取数据shape
#
# print(df.head(3))  # 获取前三行内容
# print(df[:3])  # 获取前三行内容
#
# print(df['LocalTime'][0])
# print(df['LocalTime'].shape)
# #
# # count = 0
# # for index in range(1, len(df['LocalTime'])):
# #     if (df['LocalTime'][index] < df['LocalTime'][index - 1]):
# #         count += 1
# #         # print(count)
# # print(count)
# # df['ExchangeID'].
# # print(df.groupby('ExchangeID').all())
#
#
# import pandas as pd
# import numpy as np
# dic={'科目':['语文','语文','语文','语文','数学','数学','数学','数学','英语','英语','英语','英语'],
#          '姓名':['赵大','钱二','孙三','李四','周五','郑六','王七','朱八','小红','小明','小李','小王'],
#          '分数':[95,84,93,88,91,93,84,85,94,93,83,87]}
# data=pd.DataFrame(dic)#转为DataFrame
# print(data)
# data=data.sort_values('分数', ascending = False)
# data_select = data.groupby('科目').head(2).sort_values('科目')
# print(data_select)
# print("====")
# all = data.groupby('科目').get_group('数学')
# print(all)
#
# # print(data.asfreq())
# # for a in all['科目']:
# #     print(a)
#
# data['rank'] = data.groupby('科目')['分数'].rank(ascending = False)
# data_select=data[data['rank']<=2]
# print(data_select)
#
#
# s = pd.Series([1, 2, 3], index=['a', 'b', 'c'])
# print(s)
# s.get('a')  # equivalent to s['a']
# print(s.get('a'))
# s.get('x', default=-1)
# # print(s)
#
#
# # 单调性
# # 1、19位时间戳含义
# # 2、tradingday和actionday
# # 3、UpdateMillisec含义
#
# #


# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SQLContext
#
# sc = SparkContext(appName="Pq")
# sqlContext = SQLContext(sc)
# df = sqlContext.read.parquet("../interview.parquet")
# # print(df.select('*').show(n=10))
# # print(df.select("ExchangeID").distinct().show()) # 四个交易所 INE CZCE DCE SHFE
# # print(df.select("InstrumentID").distinct().count()) # 96个合约
# # print(df.select("*").count())  #704,669条数据
# print(df.select("TradingDay").agg({"TradingDay": "min"}).show()) #TradingDay min 20211221 max 20211222
# print(df.select("ActionDay").agg({"ActionDay": "min"}).show()) #ActionDay min 20211221 max 20211222
# print(df.select("*").where("TradingDay<ActionDay").count()) # 293,802 不相等 #TradingDay比ActionDay大1天？
# print(df.select("*").where("ExchangeID='INE' and InstrumentID ='lu2212'").show()) # 293,802 不相等 #TradingDay比ActionDay大1天？
#
#
# print(df[df['ExchangeID'] == 'INE']['UpdateTime'].tail(10))

import pandas as pd
import time
from datetime import datetime as dt
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

# 读取.parquet文件
df = pd.read_parquet("../interview.parquet")

count = 0

print("shape：" + str(df.shape))
print("columns:" + str(df.columns))  # 获取数据条数和列名

print("前三行数据内容：\n" + str(df['UpdateTime'].head(3)))  # 获取前三行内容

# print(df[df['ExchangeID'] == 'INE'].head(10))
# df = df[df['ExchangeID'] == 'INE'].
# df =df.reindex(index=np.arange(0,len(df['UpdateTime']),1,int))


print("shape：" + str(df.shape))
print("columns:" + str(df.columns))  # 获取数据条数和列名
print("columns:" + str(df.head(10)))  # 获取数据条数和列名
print("first:" + str(df['UpdateTime'][0]))  # 获取数据条数和列名
print(df['UpdateTime'].dtype)
# check_ine = 0
# previous_ine = 0
# for index in range(1, len(df['UpdateTime'])):
#     if(df['ExchangeID'][index]=='INE'):
#         if previous_ine == 0:
#             previous_ine = index
#             continue
#         if dt.strptime(df['UpdateTime'][index],"%H:%M:%S") < dt.strptime(df['UpdateTime'][previous_ine],"%H:%M:%S") or (
#                 dt.strptime(df['UpdateTime'][index], "%H:%M:%S") == dt.strptime(df['UpdateTime'][previous_ine],"%H:%M:%S")
#                 and int(df['UpdateMillisec'][index]) < int(df['UpdateMillisec'][previous_ine])):
#             check_ine += 1
#         previous_ine = index
#
# if check_ine == 0:
#     print("ine不存在乱序")
# else:
#     print("ine存在乱序，乱序次数：" + str(check_ine))


#
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SQLContext
#
# sc = SparkContext()
# sqlContext = SQLContext(sc)
# data = sqlContext.read.parquet("../interview.parquet")
# print(data.schema)
# print(data.select("TradingDay").agg({"TradingDay": "min"}).show()) #TradingDay min 20211221 max 20211222
# print(data.select("ActionDay").agg({"ActionDay": "min"}).show()) #ActionDay min 20211221 max 20211222
# print(data.select("*").where("TradingDay<ActionDay").count()) # 293,802 不相等 #TradingDay比ActionDay大1天？
#
# ine = data.select("*").where("ExchangeID='INE' and InstrumentID ='lu2212'")
#
# print("======")
# # data.select("*").where(ExchangeID=INE,)
# print(data.select("*").where("ExchangeID='INE' and InstrumentID ='lu2212'").tail(30)) # 293,802 不相等 #TradingDay比ActionDay大1天？
#
#


def check_update_time(df,exchangeid):
    disorder_cnt = 0
    previous_index = 0
    for index in range(1, len(df['UpdateTime'])):
        if (df['ExchangeID'][index] == exchangeid):
            if previous_index == 0:
                previous_index = index
                continue
            if dt.strptime(df['UpdateTime'][index], "%H:%M:%S") < dt.strptime(df['UpdateTime'][previous_index],
                                                                              "%H:%M:%S") or (
                    dt.strptime(df['UpdateTime'][index], "%H:%M:%S") == dt.strptime(df['UpdateTime'][previous_index],
                                                                                    "%H:%M:%S")
                    and int(df['UpdateMillisec'][index]) < int(df['UpdateMillisec'][previous_index])):
                disorder_cnt += 1
            previous_index = index

    if disorder_cnt == 0:
        print(exchangeid+"不存在乱序")
    else:
        print(exchangeid+"存在乱序，乱序次数：" + str(disorder_cnt))


check_update_time(df,'INE')
check_update_time(df,'CZCE')
check_update_time(df,'DCE')
check_update_time(df,'SHFE')


