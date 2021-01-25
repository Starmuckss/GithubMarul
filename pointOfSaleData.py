# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 14:57:27 2021

@author: HP
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 09:52:45 2021

@author: HP
"""
import pandas as pd
import numpy as np
import os
from functools import reduce
import statistics
import time
import matplotlib.pyplot as plt



root_directory = "C:\\Users\\HP\\Desktop\\anadolu10" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
input_directory = "C:\\Users\\HP\\Desktop\\anadolu10_results"
output_directory = "C:\\Users\\HP\\Desktop\\anadolu10_plots" #You have to create this directory before using it

categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

plot_data_colums = ["Date","Category","Market","p_ratio","dispersion_measure","stdev/Average","log_Variance","Variance"]

data = {}

for date in dates:
    for category in category_list:
        path = os.path.join(input_directory, category)
        day_category_df =  pd.read_pickle(path + "\\" + category + "_" + date +".pkl")
        
        if len(day_category_df !=0):
            vendor_constraint = day_category_df["Number_of_vendors"]>=15 
            day_category_df = day_category_df[vendor_constraint]  # NOV greater than 15 
            unique_p_ratio = day_category_df["unique_p_ratio"].mean()
            dispersion_measure = day_category_df["dispersion_measure"].mean()
            stdev_average = day_category_df["Stdev/Average"].mean()
            log_variance = day_category_df["log10_Variance"].mean()
            variance = day_category_df["Variance"].mean()
            
            
        try:
            data["Date"] += [date]
            data["Category"] += [category]
            data["Market"] += ["POS"]

            data["unique_p_ratio"] += [unique_p_ratio]
            data["dispersion_measure"] += [dispersion_measure] 
            data["stdev_average"] += [stdev_average] 
            data["log_variance"] += [log_variance]
            data["Variance"] += [variance] 

        except:
            data["Date"] = [date]
            data["Category"] = [category]
            data["Market"] = ["POS"]

            data["unique_p_ratio"] = [unique_p_ratio]
            data["dispersion_measure"] = [dispersion_measure] 
            data["stdev_average"] = [stdev_average] 
            data["log_variance"] = [log_variance]
            data["Variance"] = [variance] 

pos_dataframe = pd.DataFrame(data = data)
#pos_dataframe.replace(np.NaN,0,inplace=True)
pos_dataframe.to_pickle("plotdata.pkl")

def plot_data(dataframe):
    
    criteria = ["unique_p_ratio","dispersion_measure","stdev_average","Variance"]
    for category in category_list:
        fig, axes = plt.subplots(nrows=2, ncols=2)
        category_dataframe = dataframe[dataframe["Category"]==category]
        axes = axes.flatten()
        for i in [0,1,2,3]:
            axes[i].plot(dates, category_dataframe[criteria[i]])
            axes[i].set_xlabel("Dates")
            axes[i].set_ylabel(criteria[i])
            # axes[i].set_title(category)
        fig.tight_layout()            
        
            
            
        
        plt.savefig(output_directory+"\\"+category +".png",dpi = 400)
        plt.close()             

plot_data(pos_dataframe)


