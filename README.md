# -gps-
对出租车数据进行处理，得到路网中某一时间段内的平均速度数据。

程序功能介绍
程序主要功能是从批量的出租车运行数据中得到路网中路段在某一时间段（例如每5分钟划分为一个时间段）中的速度数据。

存在的问题
出租车运行数据中包括车牌号，时间，运营状态，速度，经度，纬度等，经纬度的值存在一定的误差，不能简单的准确对应到路网的道路中，因此需要做一定的处理才能从数据中得到路段在一个时间段内的平均速度。

主要思想
先将出租车运行数据按车辆（车牌号可作为分类依据）分类，再将数据按时间顺序排列，以前后两条数据的时间差作为单条轨迹的划分依据，例如两条数据之间时间相差五分钟则将其视为两条不同的轨迹。对轨迹进行筛选，一条轨迹中的轨迹点数目小于3则过滤掉。利用fmm库进行轨迹点匹配，得到轨迹点对应的路段id。对路段在一个时间段中的速度数据进行统计，得到最终结果。

数据
出租车数据
数据遵循一定的格式
格式如下：
id,type,company,lon, lat, time, device, speed, direction, pstatus, warning, cardno, status, color
例如：
粤B001VY,绿的,深圳市安恒运输有限公司,113.820396,22.802,2017-05-08T14:29:04.000Z,1700214,0,0,0,,,0,蓝色

路网数据
路网数据是ESRI shapefile，在路网数据中，properties中必须含有id，source，target，cost这四个属性，否则在fmm库在导入路网数据时会报错。
例如：
{'type': 'Feature', 'id': '0', 'properties': OrderedDict([('_uid_', '1'), ('id', '1'), ('source', '1'), ('target', '2'), ('cost', 1.0), ('x1', 2.0), ('y1', 1.0), ('x2', 2.0), ('y2', 0.0)]), 'geometry': {'type': 'LineString', 'coordinates': [(2.0, 1.0), (2.0, 0.0)]}}
这是原作者提供的示例程序中路网数据中的一条道路数据。

程序设置

在程序中使用到了spark计算引擎，所以需要对spark进行设置，由于fmm库的安装比较麻烦，在使用分布式集群时会出现各种问题，我这里设置master时使用的是local[*]的方式，虽然没有发挥出spark分布式计算的优势，但运行的效率还勉强能接受。然后是python解释器环境的设置，setExecutorEnv时选择anaconda中安装了fmm库的那个python解释器。接着是出租车数据路径的设置，在sc.textFile中指定要处理的数据的路径。再然后是路网数据路径的设置，在match_coor函数中，Network中指定shp文件路径。最后是结果文件的保存路径，在saveTextFile中指定。
