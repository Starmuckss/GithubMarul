# -*- coding: utf-8 -*-
"""
Created on Mon Mar 15 11:45:44 2021

@author: HP
"""
import pandas as pd
import os
import matplotlib.pyplot as plt

class shared_identical_prices_visuals():
    
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
            category_between_path = self.input_directory+"//"+category +"_"+"between_chain_identical_prices.csv"
            category_within_path = self.input_directory+"//"+category +"_"+"within_chain_identical_prices.csv"
            
            if os.path.exists(category_between_path) and os.path.exists(category_between_path): # if both of them exists, we create a Histograms
                between = pd.read_csv(category_between_path) 
                within = pd.read_csv(category_within_path)
                
                merged = pd.merge(between,within,how="inner",on = ["Code","Chain_name"])
                within_chain_data = merged.Share_of_identical_within
                between_chain_data = merged.Share_of_identical_between
                
                if len(full_within) == 0 and len(full_between) == 0: 
                    full_within = within_chain_data
                    full_between = between_chain_data
                else:
                    full_within=full_within.append(within_chain_data)
                    full_between=full_between.append(between_chain_data)
                
                plt.hist(within_chain_data, alpha=0.8, label='within_chain',color="blue") # alpha value is the transperancy value of the bars, increase to make them opaque 
                plt.hist(between_chain_data, alpha=0.8, label='between_chain',color="red")
                
                plt.legend(loc='upper right')
                
                plt.title('Share of Identical Prices for '+ category)
                plt.ylabel('Pair Count')
                plt.xlabel('Share of Identical Prices')
                plt.savefig(self.output_directory+"\\"+category+".png", dpi=200) # change dpi to change picture size
                plt.clf() 
        
        
        plt.hist(full_within, alpha=0.8, label='within_chain',color="blue") # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.hist(full_between, alpha=0.8, label='between_chain',color="red") # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.title("Share of Identical Prices for all")
        plt.ylabel('Pair Count')
        plt.xlabel('Share of Identical Prices')
        plt.savefig(self.output_directory+"\\"+"all"+".png", dpi=200)
        plt.clf()