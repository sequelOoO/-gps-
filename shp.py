from pyspark import SparkContext,SparkConf,SparkFiles
import fiona
from collections import namedtuple, defaultdict, OrderedDict
from pyspark.sql import SparkSession
from datetime import datetime
UTC_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
from fmm import Network,NetworkGraph,STMATCH,STMATCHConfig
#network = Network("../data/edges.shp")
import os
import utm
os.environ["PYSPARK_PYTHON"]='/root/anaconda3/envs/py37/bin/python'
os.environ['GDAL_DATA']='/root/anaconda3/envs/py37/share/gdal/gata-data'

'''
    该程序主要用来生成shp文件，包括轨迹点的，与匹配道路的，可用arcmap更直观的感受匹配效果
'''


def sort_split(x):
    x = sorted(x, key=lambda y: y[5])
    res = []
    tmp = [x[0]]
    for item in x[1:]:
        if (datetime.strptime(item[5], UTC_FORMAT) - datetime.strptime(tmp[-1][5], UTC_FORMAT)).seconds < 180:
            tmp.append(item)
        else:
            if len(tmp) >= 3:
                res.append(tmp)
            tmp = [item]
    if len(tmp) >= 3:
        res.append(tmp)
    #    print("sort_split ok!")
    return res

def compute_coor(x):
    res = []
    # with Network(SparkFiles.get("my_connected_path4.shp")) as network:
    # graph = NetworkGraph(netwk.value)
    # model = STMATCH(netwk.value, graph)
    network = Network("./data/my_connected_path4.shp")
    graph = NetworkGraph(network)
    model = STMATCH(network, graph)
    config = STMATCHConfig()
    config.k = 4
    config.gps_error = 0.5
    # config.radius = 0.4
    config.radius = 100
    config.vmax = 60
    config.factor = 1.5

    for traj in x:
        tmp = []
        for item in traj[1]:
            y,x = float(item[4]), float(item[3])
            if x>180 or x<-180 or y>80 or y<-80:
                continue
            trans_coor = utm.from_latlon(y,x, force_zone_number=49)
            tmp.append(str(trans_coor[0])+" "+str(trans_coor[1]))
        if len(tmp) == 0:
            continue
        wkt = "LINESTRING("+",".join(tmp)+")"
        result = model.match_wkt(wkt,config)
        opath = list(result.opath)
        x2 = zip(traj[1],opath)
        res.append(x2)

    return iter(res)
def v_num(x):
    total_v = 0.0
    cnt = 0
    for i in x:
        total_v += float(i)
        cnt += 1
    return total_v / cnt, cnt


if __name__=="__main__":



    # network = Network("./data/my_connected_path4.shp")

    # result = model.match_wkt(wkt, config)
    cof = SparkConf().setMaster("local[*]").setExecutorEnv("/root/anaconda3/envs/py37/bin/python")
    # cof =SparkConf().setMaster("local[*]").setAppName("myfmmapp")
    sc = SparkContext(conf=cof)
    # path = os.path.join("./data","my_connected_path4.shp")
    # sc.addFile(path)
    # sc = SparkContext("spark://172.16.101.5:9090", "yarn")
    # sc = SparkSession.builder.master("spark://172.16.101.5:9090").getOrCreate()
    # mod = sc.broadcast(model)
    # netwk = sc.broadcast(network)
    # con = sc.broadcast(config)
    # rdd = sc.textFile("hdfs://compute-5-2:8020/user/zhaojuanjuan/hxj/testfiles/*.gz")
    rdd = sc.textFile("./data/2017-05-08.gz/*.gz")
    # rdd = sc.textFile("./data/2017-07/2017-07-*/part*.gz").filter(lambda x:x!=None)
    # print(rdd.getNumPartitions())
    rdd2 = rdd.map(lambda x:[i for i in x.split(",")]).filter(lambda x:x[-2]!='0').map(lambda x:(x[0],x[:])).groupByKey()
    # print(rdd.count())
    rdd3 = rdd2.mapValues(sort_split).flatMapValues(lambda x:x).mapPartitions(compute_coor)
    # tmp = rdd3.takeSample(False,1,2)
    # tmp = rdd3.takeSample(False,0.001,2)
    tmp = rdd3.take(3)[2]
    sc.stop()
    # traj, road_id = zip(*tmp)
    # print(traj)
    # print(road_id)
    # (['粤B011YU', '红的', '深圳市国贸汽车实业有限公司', '114.076897', '22.540199', '2017-05-08T08:48:18.000Z', '1453571', '23', '270',
    #   '0', '', '', '1', '蓝色'],
    #  ['粤B011YU', '红的', '深圳市国贸汽车实业有限公司', '114.076035', '22.542534', '2017-05-08T08:49:58.000Z', '1453571', '0', '7', '0',
    #   '', '', '1', '蓝色'],
    #  ['粤B011YU', '红的', '深圳市国贸汽车实业有限公司', '114.076302', '22.543783', '2017-05-08T08:52:38.000Z', '1453571', '40', '357',
    #   '0', '', '', '1', '蓝色'],
    #  ['粤B011YU', '红的', '深圳市国贸汽车实业有限公司', '114.076118', '22.546633', '2017-05-08T08:54:08.000Z', '1453571', '0', '258',
    #   '0', '', '', '1', '蓝色'],
    #  ['粤B011YU', '红的', '深圳市国贸汽车实业有限公司', '114.076118', '22.55085', '2017-05-08T08:56:08.000Z', '1453571', '0', '359',
    #   '0', '', '', '1', '蓝色'])
    # (9782, 15580, 1270, 24167, 3361)

    traj, road_id = zip(*tmp)
    schema = {
        'properties': OrderedDict([

            ('idx', 'int')

        ]),
        'geometry': 'Point'
    }
    with fiona.open("./data/outputshp/traj2.shp","w",driver='ESRI Shapefile',schema=schema,crs={'init': 'epsg:32649'}) as file:

        # for t in tmp:
        #     traj, _ = zip(*t)

        for i,item in enumerate(traj):
            file.write({
                'properties':OrderedDict([('idx', i)]),
                 'geometry': {
                     'type': 'Point',
                     'coordinates': utm.from_latlon(float(item[4]), float(item[3]), force_zone_number=49)
                 }

            })
    with fiona.open("./data/my_connected_path4.shp") as file:
        d = file.driver
        s = file.schema
        c = file.crs
        li = []
        # print(file[0])
        # {'type': 'Feature', 'id': '0', 'properties': OrderedDict(
        #     [('_uid_', '0'), ('id', '0'), ('source', '11'), ('target', '16'), ('cost', 9.33238880357053),
        #      ('x1', 819130.9020999996), ('y1', 2495957.3815), ('x2', 819130.6361999996), ('y2', 2495966.7101000007)]),
        #  'geometry': {'type': 'LineString',
        #               'coordinates': [(819130.9020999996, 2495957.3815), (819130.6361999996, 2495966.7101000007)]}}
        # for t in tmp:
        #     _, road_id = zip(*t)
        for i in file:
            if int(i['properties']['id']) in road_id:
                li.append(i)
    if len(li) == 0:
        print("error!")
    else:
        print(li)
    with fiona.open("./data/outputshp/road2.shp","w",driver=d,schema=s,crs=c) as file:

        for i in li:
            file.write(i)








    # rdd3 = rdd3.flatMap(lambda x:x)
    # print(rdd3.first())
    # (['粤B011YU', '红的', '深圳市国贸汽车实业有限公司', '114.076897', '22.540199', '2017-05-08T08:48:18.000Z', '1453571', '23', '270',
    #   '0', '', '', '1', '蓝色'], 9782)
    # sc.stop()

    # rdd4 = rdd3.map(lambda x:((x[1],int(datetime.strptime(x[0][5], UTC_FORMAT).timestamp()/300)),x[0][7]))
    # rdd5 = rdd4.groupByKey().mapValues(v_num)
    # rdd5.saveAsTextFile("./res_2017_07")
    # sc.stop()

# 跑2017-02-22数据用时3分26秒
# 跑2017-12-02 的数据是乱的，不符合规范
#2017-07-17 2'52