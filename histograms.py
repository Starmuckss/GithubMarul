# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 11:45:44 2021

@author: HP
"""
import pandas as pd
import os
import matplotlib.pyplot as plt

dir_path = os.path.dirname(os.path.realpath(__file__))
input_directory = dir_path + "\\" + "Share_of_identical_prices"
output_directory = dir_path + "\\" + "Histograms"
if not os.path.exists(input_directory):
    os.mkdir(input_directory)

if not os.path.exists(output_directory):
    os.mkdir(output_directory)

categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)



for category in category_list:
    category_between_path = input_directory+"//"+category +"_"+"between_chain.csv"
    category_within_path = input_directory+"//"+category +"_"+"within_chain.csv"
    
    if os.path.exists(category_between_path) and os.path.exists(category_between_path): # if both of them exists, we create a Histograms
        between = pd.read_csv(category_between_path) 
        within = pd.read_csv(category_within_path)
        
        merged = pd.merge(between,within,how="inner",on = ["Code","Chain_name"])
        within_chain_data = within.Share_of_identical_within
        between_chain_data = between.Share_of_identical_between
        
            
        plt.hist(within_chain_data, alpha=0.8, label='within_chain',color="blue") # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.hist(between_chain_data, alpha=0.8, label='between_chain',color="red")
        
        plt.legend(loc='upper right')
        
        plt.title('Share of Identical Prices for '+ category)
        plt.ylabel('Pair Count')
        plt.xlabel('Share of Identical Prices')
        plt.savefig(output_directory+"\\"+category+".png", dpi=200) # change dpi to change picture size
        plt.close()                
        