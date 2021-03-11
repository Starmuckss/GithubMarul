# -*- coding: utf-8 -*-
"""
Created on Fri Mar  5 00:37:50 2021

@author: HP
"""
import pandas as pd
import os
import numpy as np
import copy
from functools import reduce
import statistics
import time
import matplotlib 
from matplotlib import cm
import matplotlib.pyplot as plt
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

dir_path = os.path.dirname(os.path.realpath(__file__))
input_directory = dir_path+"\\data_for_plots" # Input data
root_directory = "C:\\Users\\HP\\Desktop\\10" # root directory where data is

output_path = dir_path+"\\figure1"
if not os.path.exists(output_path):  # Create output folder if it doesn't exist
    os.mkdir(output_path)

for market in os.listdir(input_directory): 
    
    market_dir = output_path+"\\"+market
    if not os.path.exists(market_dir): #Create a market folder
        os.mkdir(market_dir)
    for category in category_list:
        
        # read the data generetad by produce_all_data.py
        category_under_market_directory =  output_path+"\\"+market+"\\"+category
        all_data = pd.read_pickle(input_directory+"\\"+market+"\\"+category+".pkl")
        
        # all_data.dropna(axis=0,inplace=True) # we can drop nan values, dropping nan values means for a product, a day will not be graphed
                                               # it will be colored red in the graphs
        
        if not os.path.exists(category_under_market_directory): #Create a category folder
            os.mkdir(category_under_market_directory) 
        
        for product in np.unique(all_data["Code"]): 
            product_df = all_data[all_data["Code"] == product]
            product_df_formatted = product_df[product_df.columns[2:]]
            product_df_formatted.set_index("Date",inplace=True)
            product_df_formatted = product_df_formatted.transpose() # product_df_formatted has indices: branch_names, and columns : dates
            
            Z = product_df_formatted.to_numpy() # numpy array version of product_df_formatted, used in graph
            
            # x and y cannot be string in pcolor, therefore I use their len values as axis
            x = list(product_df_formatted.columns) 
            y = list(product_df_formatted.index)        
            x = [a for a in range(0,len(x))]
            y = [b for b in range(0,len(y))]
            
            fig, ax = plt.subplots()
        
            cmap = copy.copy(matplotlib.cm.get_cmap("Blues")) # Colormap is stored here,
            cmap.set_bad(color='red') # put color red if data is nan
            
            c= ax.pcolormesh(x,y,Z,shading= 'nearest',cmap = cmap, vmin=-0.5, vmax=2) # I set values here manually, if you want to see differences better,
                                                                                      # use vmin = Z.min(), vmax= Z.max()
            fig.colorbar(c, ax = ax) 
            plt.axis("off") # omits the x and y axis from data
            
            
            fig.savefig(category_under_market_directory+"\\"+product+".png",dpi=None) # save the figure, increase dpi for better quality, but be careful, png size increases
            plt.close()
