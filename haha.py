import fiona
import fmm
with fiona.open('./data/edges.shp') as f:
    for i in f:
        print(i)