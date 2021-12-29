# -*- coding: utf-8 -*-
"""
Created on Sat Jul 31 17:06:52 2021

@author: HP
"""
import pandas as pd
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point, Polygon
import mapclassify
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
input_directory = dir_path + "\\data\\semt_correlation" # Preprocessed data will be recorded here
output_directory = dir_path + "\\output\\correlation_maps" # Output Data will be saved in here

if not os.path.exists(dir_path + "\\output"):
    os.mkdir(dir_path + "\\output")
if not os.path.exists(output_directory): # create the folder if not exists already
    os.mkdir(output_directory)
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

# translator for turkish letters
Tr2Eng = str.maketrans("çğıöşüÇĞIÖŞÜ", "cgiosuCGIOSU")

#Import Turkey administrative map
turkey_map = gpd.read_file('district_maps\\tur_polbna_adm2.shp')
geo_df = gpd.GeoDataFrame(turkey_map)

# Take Istanbul data from Turkey map
istanbul_map = geo_df[geo_df["adm1_tr"] == "İSTANBUL"]
istanbul_map.rename(columns={"adm2_en":"district"},inplace=True) #match column names with data 
istanbul_map["adm2_tr"] = istanbul_map["adm2_tr"].apply(lambda x:x.lower().capitalize())  #string adjustments
istanbul_map["district"] = istanbul_map["district"].apply(lambda x:x.lower().capitalize())  #string adjustments
istanbul_map['coords'] = istanbul_map['geometry'].apply(lambda x: x.centroid.coords[:])
istanbul_map['coords'] = [coords[0] for coords in istanbul_map['coords']] # Generate coordinates of districts
for category in category_list:
    
    if os.path.exists(input_directory+"\\correlation_"+category+".pkl"):
        correlation_data = pd.read_pickle(input_directory+"\\correlation_"+category+".pkl") # Import correlation data here
    else:
        continue
    
    
    correlation_data["district"]=correlation_data["district"].apply(lambda x:x.translate(Tr2Eng)) # tranlate Turkish letters
    merged = pd.merge(left = istanbul_map, right = correlation_data,how="outer",on="district") # Merge 
    
    # scheme = mapclassify.Quantiles(merged["correlation_for_district"], k=5)
    
    # Plot and save the plot
    try:
        
        fig, ax = plt.subplots(figsize = (20,18))
        ax.set_title(category + " correlation")
        istanbul_map.plot(ax=ax,color="gray")
        for _, row in merged.iterrows():
    
            hue = round(row['correlation_for_district'],2)
            
            plt.text(s=row['adm2_tr'], x = row['coords'][0], y = row['coords'][1],
                     horizontalalignment='center', fontdict = {'weight': 'bold', 'size': 6})
    
            plt.text(s=f'{hue:,}', x=row['coords'][0],y = row['coords'][1] - 0.01 ,
            horizontalalignment='center', fontdict = {'size': 5})
        
        merged.plot(ax=ax,column='correlation_for_district', legend=True,cmap = "Blues" ,missing_kwds={
        "color": "gray",
        "label": "Missing values",
    })
        plt.axis('off')
        fig.savefig(output_directory+"\\"+category,dpi=600)
        fig.clf()

    except:
        continue