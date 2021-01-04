from fmm import Network,NetworkGraph,STMATCH,STMATCHConfig
#network = Network("../data/edges.shp")
import os
os.environ['GDAL_DATA']='/root/anaconda3/envs/py37/share/gdal/gata-data'
network = Network("./data/my_connected_path4.shp")
graph = NetworkGraph(network)
print(graph.get_num_vertices())
model = STMATCH(network,graph)
#wkt = "LINESTRING(0.200812146892656 2.14088983050848,1.44262005649717 2.14879943502825,3.06408898305084 2.16066384180791,3.06408898305084 2.7103813559322,3.70872175141242 2.97930790960452,4.11606638418078 2.62337570621469)"

wkt='LINESTRING(795303.034993372 2501688.01377713,795560.429338128 2501692.97797836,795707.140686964 2501540.6412883,795959.276230693 2501290.59074983,796195.485484709 2501295.15550407,796473.144461875 2501311.72028055,796750.807790852 2501328.06856179,796836.860706975 2501141.42621494,796847.689794327 2501108.38539077,796944.359476026 2500899.78195948,796965.69396551 2500855.75036811,797062.585471891 2500635.84639994,797159.475059414 2500416.16437263,797099.258340175 2500326.21774028,796997.875256631 2500246.66813235,796857.033213836 2500099.96478869,796715.150891706 2499964.32649271,796695.210068139 2499941.66269279,796523.197546747 2499783.16569196,796280.957501864 2499556.81308489,796058.360504587 2499330.95536357,796180.207237443 2499444.03203383,796373.773054363 2499547.52340627,796593.282889721 2499363.23775452,796897.356311945 2499069.9780287,797233.786376309 2498733.01571234,797444.205547783 2498493.14554583,797781.02829855 2498089.47660711,797670.189937025 2497976.6008924,797478.112838676 2497795.53887512,797377.028163958 2497705.0220508,797356.981862054 2497682.3556735,797409.093629283 2497628.05866213,797440.190719634 2497606.38343421)'

config = STMATCHConfig()
config.k = 4
config.gps_error = 0.5
#config.radius = 0.4
config.radius = 100
config.vmax = 30
config.factor =1.5
result = model.match_wkt(wkt,config)
print(type(result))
print("Opath ",list(result.opath))
print( "Cpath ",list(result.cpath))
print( "WKT ",result.mgeom.export_wkt())
