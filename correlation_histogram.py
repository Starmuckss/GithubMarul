# -*- coding: utf-8 -*-
"""
Histograms for Correlation data processed in correlation_process.py
Created on Tue Jun  1 12:27:17 2021

@author: HP
"""
import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.ticker import PercentFormatter


dir_path = os.path.dirname(os.path.realpath(__file__))
input_directory = dir_path+"\\correlation"
output_directory = dir_path+"\\correlation_histograms"
if not os.path.exists(input_directory):
    os.mkdir(input_directory)

if not os.path.exists(output_directory):
    os.mkdir(output_directory)

categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

full_within = pd.Series()
full_between = pd.Series()

bins = [x/100 for x in range(0,101,5)]

for category in category_list:
    category_between_path = input_directory + "\\correlation_for_between"+category+".csv" # Path for data obtained from correlation_process.py
    category_within_path = input_directory + "\\correlation_for_within"+category+".csv" # Path for data obtained from correlation_process.py
    
    if os.path.exists(category_between_path) and os.path.exists(category_between_path): # if both of them exists, we create a Histogram
        between = pd.read_csv(category_between_path) 
        within = pd.read_csv(category_within_path)
        between = between.rename(columns = {'Mean_correlation': 'Mean_correlation_between'}, inplace = False)
        within = within.rename(columns = {'Mean_correlation': 'Mean_correlation_within'}, inplace = False)
        
        # lower_pair_count_limit = 20
        # between = between[between["pair_count"]>lower_pair_count_limit]
        # within = within[within["pair_count"]>lower_pair_count_limit]
        
        if len(between) == 0 or len(within) == 0: # If one of the between or within is empty, skip to the next category 
            continue 
        
        merged = pd.merge(between,within,how="inner",on = ["Name","Code"]) # merge between and within according to code AND chain_name
        
        # Take the column where the abs difference data resides
        within_chain_data = merged.Mean_correlation_within 
        between_chain_data = merged.Mean_correlation_between
        
        # Populate full_within and full_between, where we keep all of the abs_difference data, in all categories and chains 
        if len(full_within) == 0 and len(full_between) == 0: 
            full_within = within_chain_data
            full_between = between_chain_data
        else:
            full_within=full_within.append(within_chain_data)
            full_between=full_between.append(between_chain_data)
        
        # plot the data for single category
        plt.hist(within_chain_data, alpha=0.8, label='within_chain',color="blue",weights=np.ones(len(merged)) / len(merged),bins=bins) # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.hist(between_chain_data, alpha=0.8, label='between_chain',color="red",weights=np.ones(len(merged)) / len(merged),bins=bins)
        
        # weights = np.ones(len(merged)) / len(merged) creates a numpy array of 1's. then divides the whole array to the length of the data.
        
        plt.legend(loc='upper right')
        plt.xlim(-1,1) # limit on the x axis # NOTE: on the article, this is set as (0,1) (winsorised)
        # plt.ylim(0,1)
        plt.title('correlation for '+ category)
        plt.ylabel("y label")
        plt.gca().yaxis.set_major_formatter(PercentFormatter(1)) # Formats the y limit to show percentages
        plt.xlabel('correlation')
        plt.savefig(output_directory+"\\"+category+"_.png", dpi=200) # change dpi to change picture size
        plt.clf() 
plt.hist(full_within, alpha=0.8, label='within_chain',color="blue",weights=np.ones(len(full_within)) / len(full_within),bins=bins) # alpha value is the transperancy value of the bars, increase to make them opaque 
plt.hist(full_between, alpha=0.8, label='between_chain',color="red",weights=np.ones(len(full_between)) / len(full_between),bins=bins) # alpha value is the transperancy value of the bars, increase to make them opaque 
plt.legend(loc='upper right')
plt.title("Correlation for all")
plt.xlim(-1, 1)
plt.ylabel('y label')
plt.gca().yaxis.set_major_formatter(PercentFormatter(1)) # Formats the y limit to show percentages
plt.xlabel("Correlation for all")
plt.savefig(output_directory+"\\"+"all"+"_.png", dpi=200)
plt.clf()