from pyspark import SparkContext,SparkConf,SparkFiles
from datetime import datetime
import os
from operator import add
import json
import time
import sys
os.environ['JAVA_HOME']='/usr/lib/java/jdk1.8.0_212/'
os.environ["PYSPARK_PYTHON"]='/root/anaconda3/envs/py37/bin/python'
os.environ['GDAL_DATA']='/root/anaconda3/envs/py37/share/gdal/gata-data'
UTC_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
if __name__ == "__main__":
    cof = SparkConf().setMaster("local[*]").setExecutorEnv("/root/anaconda3/envs/py37/bin/python")
    # cof =SparkConf().setMaster("local[*]").setAppName("myfmmapp")
    sc = SparkContext(conf=cof)

    url = './data/2017-07'
    files = os.listdir(url)
    all_num_per_day = {}
    zaike_per_day={}
    for f in files:
        real_path = os.path.join(url,f)
        rdd = sc.textFile(real_path+"/*.gz")
        all_num_per_day[f[:-3]]=rdd.count()
        zaike_per_day[f[:-3]] = rdd.filter(lambda x: x.split(',')[-2]!='0').count()

    # with open('summary_all_num_per_day.txt','w') as f:
    #     f.write(json.dumps(all_num_per_day))
    # with open('summary_zaike_num_per_day.txt', 'w') as f:
    #     f.write(json.dumps(zaike_per_day))

    rdd = sc.textFile('./res_2017_07/part*')
    rdd2 = rdd.map(lambda x: x.replace('(', '').replace(')', '').split(',')).map(
        lambda x: (time.strftime("%Y-%m-%d", time.localtime(int(x[1]) * 300)), int(x[3])))
    res = rdd2.reduceByKey(add)
    # print(res.first())
    matched_num_per_day = {}
    for i in res.collect():
        matched_num_per_day[i[0]] = i[1]
    # print(matched_num_per_day)
    fin = {}
    fin['all_num_per_day'] = all_num_per_day
    fin['zaike_num_per_day'] = zaike_per_day
    fin['matched_num_per_day'] = matched_num_per_day
    with open('summary.txt', 'w') as f:
        f.write(json.dumps(fin))
    sc.stop()

    # rdd = sc.wholeTextFiles("./data/2017-07/2017-07-*/part*.gz").filter(lambda x: x[1] != None)
    # print("hello")
    # print(rdd.take(1)[0][0].split(r'/')[-2][:-3])
    # print(rdd.countByKey())
    # rdd2 = rdd.flatMapValues(lambda x: x.split("\n"))
    # rdd2 = rdd.map(lambda x: (x[0].split(r'/')[-2][:-3], x[1])).flatMapValues(lambda x: x.split('\n'))
    # cnt = rdd2.count()
    # print(rdd2.take(10))
    # all_num_per_day = rdd2.countByKey()
    # all_num_month = rdd2.count()

    # rdd3 = rdd2.filter(lambda x: x[1].split(',')[-2] != '0')
    # zaike_num_per_day = rdd3.countByKey()
    # zaike_num_month = rdd3.count()
    # print(all_num, zaike_num)
    # print(zaike_num)
    # with open('summary.txt', 'a') as f:
    #     f.write(zaike_num_per_day)
    #     f.write(zaike_num_month)


    # rdd2 = rdd.map(lambda x: [i for i in x.split(",")]).filter(lambda x: x[-2] != '0').map(
    #     lambda x: (x[0], x[:])).groupByKey()

