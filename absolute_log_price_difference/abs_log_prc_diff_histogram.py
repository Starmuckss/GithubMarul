# -*- coding: utf-8 -*-
"""
Created on Tue May  4 10:55:35 2021
Takes in data obtained from log_average_abs_difference.py and prints out histograms to log_average_abs_diff_histograms folder
@author: HP
"""
import pandas as pd
import os
import matplotlib.pyplot as plt

class absolute_log_price_difference_visuals():
    
    def __init__(self,data_directory,year,month,input_directory,output_directory,category_list):
    
        self.data_directory = data_directory
        self.year = year
        self.month = month
        
        if not os.path.exists(os.path.join(input_directory,year)): # create the folder if not exists already
            os.mkdir(os.path.join(input_directory,year)) 
        if not os.path.exists(os.path.join(input_directory,year,month)): # create the folder if not exists already
            os.mkdir(os.path.join(input_directory,year,month)) 
        self.input_directory = os.path.join(input_directory,year,month) 
        
        if not os.path.exists(os.path.join(output_directory,year)): # create the folder if not exists already
            os.mkdir(os.path.join(output_directory,year)) 
        if not os.path.exists(os.path.join(output_directory,year,month)): # create the folder if not exists already
            os.mkdir(os.path.join(output_directory,year,month)) 
        self.output_directory = os.path.join(output_directory,year,month) 
        
        self.category_list = category_list

    def create_visuals(self):

        full_within = pd.Series()
        full_between = pd.Series()
        
        for category in self.category_list:
            category_between_path = self.input_directory + "\\" + category + "_between.csv" # Path for data obtained from log_average_abs_difference.py
            category_within_path = self.input_directory + "\\" + category + "_within.csv" # Path for data obtained from log_average_abs_difference.py
            
            if os.path.exists(category_between_path) and os.path.exists(category_between_path): # if both of them exists, we create a Histogram
                between = pd.read_csv(category_between_path) 
                within = pd.read_csv(category_within_path)
                
                # If one of the between or between is empty, skip that category
                if len(between) == 0 or len(within) == 0: 
                    continue 
                
                # I wanted to drop products where the prod only has low pair_count like 2 or 5 or 10. 20 limit is arbitrary here, you can replace it with something better
                lower_pair_count_limit = 20
                between = between[between["pair_count"]>lower_pair_count_limit]
                within = within[within["pair_count"]>lower_pair_count_limit]
                
                merged = pd.merge(between,within,how="inner",on = ["Code","chain_name"]) # merge between and within according to code AND chain_name
                
                # Take the column where the abs difference data resides
                within_chain_data = merged.within_abs_difference 
                between_chain_data = merged.between_abs_difference
                
                # Populate full_within and full_between, where we keep all of the abs_difference data, in all categories and chains 
                if len(full_within) == 0 and len(full_between) == 0: 
                    full_within = within_chain_data
                    full_between = between_chain_data
                else:
                    full_within=full_within.append(within_chain_data)
                    full_between=full_between.append(between_chain_data)
                
                # plot the data for single category
                plt.hist(within_chain_data, alpha=0.8, label='within_chain',color="blue") # alpha value is the transperancy value of the bars, increase to make them opaque 
                plt.hist(between_chain_data, alpha=0.8, label='between_chain',color="red")
                
                plt.legend(loc='upper right')
                plt.xlim(0,1) # limit on the x axis
                plt.title('Log Average Absolute Difference for '+ category)
                plt.ylabel('Pair Count')
                plt.xlabel('Log Average Absolute Difference')
                plt.savefig(self.output_directory+"\\"+category+".png", dpi=200) # change dpi to change picture size
                plt.clf() 
        
        
        plt.hist(full_within, alpha=0.8, label='within_chain',color="blue") # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.hist(full_between, alpha=0.8, label='between_chain',color="red") # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.title("")
        plt.xlim(0, 1)
        plt.ylabel('Pair Count')
        plt.xlabel("Log Average Absolute Difference for all")
        plt.savefig(self.output_directory+"\\"+"all"+".png", dpi=200)
        plt.clf()
        
        print("visualization done!")