import os
import rasterio
import numpy as np
from datetime import datetime
import geopandas as gpd
import subprocess
from shutil import copyfile, rmtree
import fiona
import rasterio
import rasterio.mask
import pandas as pd
from tqdm import tqdm


GDAL_POLYGONIZE = r"C:\ProgramData\Anaconda3\envs\geo\Scripts\gdal_polygonize.py"
TEMP_PATH = 'output/temp/temp.tif'
TEMP_DIR_PATH = 'output/temp'


def process_print(command_args):
    process = subprocess.Popen(command_args, stdout=subprocess.PIPE)
    stdout = process.communicate()[0]
    # print('{}'.format(stdout))


def clipTiffWithShapes(src, shapes):
    outImage, out_transform = rasterio.mask.mask(src, shapes, crop=True)
    outMeta = src.meta.copy()
    outMeta.update({"driver": "GTiff",
                    "height": outImage.shape[1],
                    "width": outImage.shape[2],
                    "transform": out_transform})
    return outImage, outMeta


def calculateRasterStats(district, raster):

    # array = raster.read(masked=True)
    band = raster[0]
    band = band[~np.isnan(band)]

    theSum = int(band.sum())
    stats = {'affected_population': theSum,
             'district': district}
    return stats


population_raster = 'input/population_uga_2019/population_uga_2019.tif'
raster_pop = rasterio.open(population_raster)
exposure_dir = 'output'
flood_dir = 'input/floodscan_data'
years = [x for x in os.listdir(flood_dir) if os.path.isdir(os.path.join(flood_dir, x))]
df_districts = gpd.read_file('input/admin_boundaries/uga_admbnda_adm1_UBOS_v2.shp')
df_impact = pd.DataFrame()

for year in tqdm(years):
    flood_rasters = os.listdir(os.path.join(flood_dir, year))
    for flood_raster in flood_rasters:
        date = datetime.strptime(flood_raster.split('_')[3], '%Y%m%d')

        if os.path.exists(TEMP_DIR_PATH):
            rmtree(TEMP_DIR_PATH)
        process_print(["python", GDAL_POLYGONIZE, os.path.join(flood_dir, year, flood_raster),
                       "-f", "ESRI Shapefile", TEMP_DIR_PATH])

        # keep only floods
        df = gpd.read_file(TEMP_DIR_PATH)
        df = df[df['DN'] == 1]
        shapes = [feature["geometry"] for ix, feature in df.iterrows()]

        out_image, out_transform = rasterio.mask.mask(raster_pop, shapes, crop=True)
        if not (out_image[~np.isnan(out_image)] > 0).any():
            # print('no population affected, skipping day')
            continue
        else:
            out_meta = raster_pop.meta
            out_meta.update({"driver": "GTiff",
                             "height": out_image.shape[1],
                             "width": out_image.shape[2],
                             "transform": out_transform})
            with rasterio.open(TEMP_PATH, "w", **out_meta) as dest:
                dest.write(out_image)

            # calculate stat per district
            stats = []
            src = rasterio.open(TEMP_PATH)
            for ix, row in df_districts.iterrows():
                district = row['ADM1_EN']
                try:
                    outImage, outMeta = clipTiffWithShapes(src, [row["geometry"]])

                    statsDistrict = calculateRasterStats(district, outImage)
                    if statsDistrict['affected_population'] > 0.:
                        stats.append(statsDistrict)
                except ValueError:
                    pass
            src.close()

            # save stat per district
            for idx, stat_region in enumerate(stats):
                stat_region['date'] = date
                df_impact = df_impact.append(pd.Series(stat_region), ignore_index=True)
    df_impact.to_csv('output/impact_data_'+str(year)+'.csv')
print(df_impact.head())
df_impact.to_csv('output/impact_data.csv')