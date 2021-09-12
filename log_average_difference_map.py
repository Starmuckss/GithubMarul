# -*- coding: utf-8 -*-
"""
Created on Sun Sep 12 22:55:20 2021

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
input_directory = dir_path + "\\semt_log_average_abs_difference" # Preprocessed data will be recorded here
output_directory = dir_path + "\\log_average_difference_maps" # Output Data will be saved in here
if not os.path.exists(output_directory): # create the folder if not exists already
    os.mkdir(output_directory)
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

# translator for turkish letters
Tr2Eng = str.maketrans("çğıöşüÇĞIÖŞÜ", "cgiosuCGIOSU") # dictionary in order to translate turkish letters to english ones

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
    log_average_difference_data = pd.read_pickle(input_directory + "\\" + category + "_semt.pkl") # Import data here
    
    # Get number columns only
    try: 
        log_average_difference_data.drop(["Name","Code"],axis =1,inplace=True)
    except KeyError:
        continue
    
    # Take the average of each district's log average difference data
    data_averaged = log_average_difference_data.mean().reset_index()
    data_averaged.columns =  ["district","log_average_difference"] 
    
    data_averaged["district"]=data_averaged["district"].apply(lambda x:x.translate(Tr2Eng)) # tranlate Turkish letters
    
    merged = pd.merge(left = istanbul_map, right = data_averaged,how="outer",on="district") # Merge geographic data with our metric data
    
    # Plot and save the plot
    try:
        
        fig, ax = plt.subplots(figsize = (20,18))
        ax.set_title(category + " log average difference")
        istanbul_map.plot(ax=ax,color="gray")
        for _, row in merged.iterrows():
    
            hue = round(row['log_average_difference'],2)
            
            plt.text(s=row['adm2_tr'], x = row['coords'][0], y = row['coords'][1],
                     horizontalalignment='center', fontdict = {'weight': 'bold', 'size': 6})
    
            plt.text(s=f'{hue:,}', x=row['coords'][0],y = row['coords'][1] - 0.01 ,
            horizontalalignment='center', fontdict = {'size': 5})
        
        merged.plot(ax=ax,column='log_average_difference', legend=True)
        fig.savefig(output_directory+"\\"+category,dpi=600)
    except:
        continue