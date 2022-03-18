

import pandas as pd
import time

class Ticker:

    def tick(self,freq=0.001,input_path = './data.csv', output_path = './tick.csv'):
        '''
        :param freq: 出发频率-默认0.001 即1s1000条
        :param input_path: 文件路径-tick全量数据
        :param output_path: 文件路径-模拟的tick数据
        :return:
        '''

        with open(input_path, encoding='utf-8') as f:
            readlines = f.readlines()  # readlines是一个列表
            count = 1
            for i in readlines:
                contents = []
                line = i.strip().split(",")  # 去掉前后的换行符，之后按逗号分割开
                contents.append(line)  # contents二维列表
                df = pd.DataFrame(contents)
                time.sleep(freq)
                mode = 'a'
                if count == 1:
                    mode = 'w'
                df.to_csv(output_path, mode=mode, header=False, index=False)
                if(count%1000==0):
                    print("第" + str(count/1000) + "s,下发1000条")
                count += 1

if __name__ == '__main__':
    ticker = Ticker()
    ticker.tick()
