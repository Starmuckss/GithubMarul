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


class correlation_histogram():
    
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
    
    def markets(self,directory):
        """
        Parameters
        ----------
        directory : String
            directory of where whole data is .
        Returns
        -------
        market_dictionary : Dictionary
            dictionary has key: markets , and values: *branch_names(sube_names)
            values are lists and they have every unique branch that market has
        """
        
        categories = pd.read_excel("categories.xlsx")
        category_list = list(categories.category)
        
        market_dictionary = {}
        check_distinct  = {}
        for subdir, dirs, files in os.walk(directory):   
            for category in category_list:
                for direc in dirs:
                    splitted = direc.split("-")
                    market_name = splitted[0]
                    sube_name_root = "-".join(splitted[0:len(splitted)-1])
                    
                    try:
                        check_distinct[sube_name_root] += 1 
                    except KeyError:
                        check_distinct[sube_name_root] = 1
                        
                    if check_distinct[sube_name_root] < 2:
                        
                        try:
                            market_dictionary[market_name] += [direc]  
                        except KeyError:
                            market_dictionary[market_name] = [direc]
        return market_dictionary
    
    
    def create_histograms(self,dpi = 200):
    
        chain_dictionary = self.markets(self.data_directory)
        
        full_within = pd.Series()
        full_between = pd.Series()
        
        bins = [x/100 for x in range(0,101,5)]
        
        for category in self.category_list:
            category_within = pd.Series()
            category_between = pd.Series()
            for chain in chain_dictionary.keys():
                category_between_path = self.input_directory +"\\"+"correlation_between_"+chain+"_"+category+".csv" # Path for data obtained from correlation_process.py
                category_within_path = self.input_directory +"\\"+"correlation_within_"+chain+"_"+category+".csv" # Path for data obtained from correlation_process.py
                
                if os.path.exists(category_between_path) and os.path.exists(category_within_path): # if both of them exists, we create a Histogram
                    between = pd.read_csv(category_between_path) 
                    within = pd.read_csv(category_within_path)
                    between = between.rename(columns = {'Mean_correlation': 'Mean_correlation_between'}, inplace = False)
                    within = within.rename(columns = {'Mean_correlation': 'Mean_correlation_within'}, inplace = False)
                    
                    lower_pair_count_limit = 10
                    between = between[between["pair_count"]>lower_pair_count_limit]
                    within = within[within["pair_count"]>lower_pair_count_limit]
                    
                    if len(between) == 0 or len(within) == 0: # If one of the between or within is empty, skip to the next category 
                        continue 
                    
                    merged = pd.merge(between,within,how="inner",on = ["Name","Code"]) # merge between and within according to code AND chain_name
                    
                    # Take the column where the correlation data resides
                    within_chain_data = merged.Mean_correlation_within 
                    between_chain_data = merged.Mean_correlation_between
                    
                    # Populate category_within and category_between, where we keep all of the correlation data, in all chains in a particular category 
                    if len(category_within) == 0 and len(category_between) == 0: 
                        category_within = within_chain_data
                        category_between = between_chain_data
                    else:
                        category_within=category_within.append(within_chain_data)
                        category_between=category_between.append(between_chain_data)
                    
            # plot the data for single category
            
            plt.hist(category_within, alpha=0.8, label='within_chain',color="blue",weights=np.ones(len(category_within)) / len(category_within),bins=bins) # alpha value is the transperancy value of the bars, increase to make them opaque 
            plt.hist(category_between, alpha=0.8, label='between_chain',color="red",weights=np.ones(len(category_within)) / len(category_within),bins=bins)
            
            if len(full_within) == 0 and len(full_between) == 0: 
                full_within = category_within
                full_between = category_between
            else:
                full_within=full_within.append(category_within)
                full_between=full_between.append(category_between)
           
            # weights = np.ones(len(merged)) / len(merged) creates a numpy array of 1's. then divides the whole array to the length of the data.
            
            plt.legend(loc='upper right')
            plt.xlim(0,1) # limit on the x axis # NOTE: on the article, this is set as (0,1) (winsorised)
            # But I don't know that if winsorization omits negative values or squzees them to 0 value. WINSORIZATION HERE
            plt.ylim(0,1)
            plt.title('correlation for '+ category)
            plt.ylabel("products in this category: "+str(len(merged)))
            plt.gca().yaxis.set_major_formatter(PercentFormatter(1)) # Formats the y limit to show percentages
            plt.xlabel('correlation')
            plt.savefig(self.output_directory+"\\"+category+"_.png", dpi=200) # change dpi to change picture size
            plt.clf() 
        
        plt.hist(full_within, alpha=0.8, label='within_chain',color="blue",weights=np.ones(len(full_within)) / len(full_within),bins=bins) # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.hist(full_between, alpha=0.8, label='between_chain',color="red",weights=np.ones(len(full_between)) / len(full_between),bins=bins) # alpha value is the transperancy value of the bars, increase to make them opaque 
        plt.legend(loc='upper right')
        plt.title("Correlation for all categories")
        plt.xlim(0, 1)
        plt.ylim(0, 1)
        plt.ylabel("total number of products: "+str(len(full_within)))
        plt.gca().yaxis.set_major_formatter(PercentFormatter(1)) # Formats the y limit to show percentages
        plt.xlabel("Correlation for all")
        plt.savefig(self.output_directory+"\\"+"all"+"_.png", dpi=dpi)
        plt.clf()