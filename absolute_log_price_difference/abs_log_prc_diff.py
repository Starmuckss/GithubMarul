# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 15:33:05 2021
Find average log prices of point of sale data at product level (done in pre_abs_log_prc_diff.py)
and take the absolute difference with other PoS, for both between and within. 
within: PoS from the same chain
between: PoS from different chains
Second metric in Uniform Pricing in US Retail Chains@author: HP
!!!!!!! problem on outer merge
"""
import pandas as pd
import os
import time
import numpy as np
from collections import defaultdict

class absolute_log_price_difference_process():
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
            
        #categories = pd.read_excel("categories.xlsx")
        #category_list = list(categories.category)
        self.category_list = category_list # hangi categoriler var bunun oldugu liste
        self.dates = os.listdir(data_directory)
    
    def process(self):
        sampled_dict_of_pairs = np.load(self.input_directory + "\\sampled_dict_of_pairs.npy",allow_pickle='TRUE').item()
        
        for category in self.category_list:
            start_category = time.time()
            # If pair is within, its dataframe will be stored in within, same for between
            # defaultdict is a dictionary with a useful mechanic, when an unknown key is created, It automatically creates an empty value with type entered as parameter
            # helps me to get rid long lines when I try to fill dictionaries with try-except
            within = defaultdict(list)  # there are empty dictionaries
            between = defaultdict(list)
        
            for pair in sampled_dict_of_pairs[category]:                 
                pos_name1 = pair[0][pair[0].find(self.dates[0]) + len(self.dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
                chain_name1 = pos_name1.split("-")[0] # returns cagdas
                
                pos_name2 = pair[1][pair[1].find(self.dates[0]) + len(self.dates[0])+1: pair[1].find(category)-1]
                chain_name2 = pos_name2.split("-")[0]
                
                # since we already calculated averages, we read the data directly and start merging both pos in the pair
                try:    
                    pos_1 = pd.read_pickle(self.input_directory + "\\" + pos_name1+"_"+category+ ".pkl")   
                    pos_2 = pd.read_pickle(self.input_directory + "\\" + pos_name2+"_"+category+ ".pkl")
                except:
                    continue
                final_merge_for_pair = pd.merge(pos_1,right=pos_2,on = "Code") # inner merge on code 
                final_merge_for_pair["absolute_difference"] = np.abs(final_merge_for_pair["Mean_log_price_x"] - final_merge_for_pair["Mean_log_price_y"]) # abs difference of mean prices
                final_merge_for_pair.rename(columns = {"Name_x" : "Name" },inplace = True)
                final_merge_for_pair.drop(axis=1,columns=["Mean_log_price_x","Mean_log_price_y","Name_y"],inplace=True)                  
                
                # decide if this pair is a within or between
                if  chain_name1 == chain_name2:
                    # within chain pair.
                    if len(within[chain_name1]) == 0:                    
                        within[chain_name1] = final_merge_for_pair
                        within[chain_name1]["pair_count"] = 1
                    
                    else:
                        within[chain_name1] = pd.merge(within[chain_name1], final_merge_for_pair,how= "outer",on="Code")
                       
                        within[chain_name1]["pair_count"].fillna(0,inplace=(True))
                        within[chain_name1]["pair_count"] += 1
                        
                        within[chain_name1].loc[within[chain_name1]['absolute_difference_y'].isnull(), 'pair_count'] -= 1
                        
                        check_for_nan = within[chain_name1]['Name_x'].isnull()
                        within[chain_name1].loc[within[chain_name1]['Name_x'].isnull(),"Name_x"] = within[chain_name1]["Name_y"].loc[check_for_nan] 
                        
                        check_for_nan = within[chain_name1]['Name_y'].isnull()
                        within[chain_name1].loc[within[chain_name1]['Name_y'].isnull(),"Name_y"] = within[chain_name1]["Name_x"].loc[check_for_nan] 
                        
                        within[chain_name1].fillna(0,inplace=True)
                        within[chain_name1]["absolute_difference"] = within[chain_name1]["absolute_difference_x"] + within[chain_name1]["absolute_difference_y"]
                        
                        within[chain_name1].drop(labels = ["absolute_difference_x","absolute_difference_y","Name_y"],axis = 1,inplace=True)
                        within[chain_name1].rename(columns = {"Name_x":"Name"},inplace = True)
                    
                else: # between chain pair
                    if len(between[chain_name1]) == 0:                    
                        between[chain_name1] = final_merge_for_pair
                        between[chain_name1]["pair_count"] = 1
                    
                    else:
                        between[chain_name1] = pd.merge(between[chain_name1], final_merge_for_pair,how= "outer",on="Code")
                       
                        between[chain_name1]["pair_count"].fillna(0,inplace=(True))
                        between[chain_name1]["pair_count"] += 1
                        
                        between[chain_name1].loc[between[chain_name1]['absolute_difference_y'].isnull(), 'pair_count'] -= 1
                        
                        check_for_nan = between[chain_name1]['Name_x'].isnull()
                        between[chain_name1].loc[between[chain_name1]['Name_x'].isnull(),"Name_x"] = between[chain_name1]["Name_y"].loc[check_for_nan] 
                        
                        check_for_nan = between[chain_name1]['Name_y'].isnull()
                        between[chain_name1].loc[between[chain_name1]['Name_y'].isnull(),"Name_y"] = between[chain_name1]["Name_x"].loc[check_for_nan] 
                        
                        between[chain_name1].fillna(0,inplace=True)
                        between[chain_name1]["absolute_difference"] = between[chain_name1]["absolute_difference_x"] + between[chain_name1]["absolute_difference_y"]
                        
                        between[chain_name1].drop(labels = ["absolute_difference_x","absolute_difference_y","Name_y"],axis = 1,inplace=True)
                        between[chain_name1].rename(columns = {"Name_x":"Name"},inplace = True)
                
            
            #Report between and within chain data
            between_df = pd.DataFrame()
            within_df = pd.DataFrame()
            
            # NOW IN LONG FORMAT: df.columns = name,code,pair_count,between_abs_difference,chain_name  
            for chain in between.keys():
                if len(between_df) == 0:
                    between_df=  between[chain]
                    between_df["between_abs_difference"] = between_df["absolute_difference"] / between_df["pair_count"] # abs_difference is divided to pair count, reported under chain name
                    between_df.drop(["absolute_difference"],axis =1, inplace = True)
                    between_df["chain_name"] = chain
                else:
                    df = between[chain] 
                    df["between_abs_difference"] = df["absolute_difference"] / df["pair_count"]
                    #between_df = pd.merge(between_df,df[["Code","Name","between_abs_difference"]],on = ["Name","Code"],how = "outer")
                    df.drop(["absolute_difference"],axis =1, inplace = True)
                    df["chain_name"] = chain
                    between_df = between_df.append(df)
            for chain in within.keys():
                if len(within_df) == 0:
                    within_df=  within[chain]
                    within_df["within_abs_difference"] = within_df["absolute_difference"] / within_df["pair_count"] # abs_difference is divided to pair count, reported under chain name
                    within_df.drop(["absolute_difference"],axis =1, inplace = True)
                    within_df["chain_name"] = chain
                else:
                    df = within[chain] 
                    df["within_abs_difference"] = df["absolute_difference"] / df["pair_count"]
                    #within_df = pd.merge(within_df,df[["Code","Name","within_abs_difference"]],on = ["Name","Code"],how = "outer")
                    df.drop(["absolute_difference"],axis =1, inplace = True)
                    df["chain_name"] = chain
                    within_df = within_df.append(df)       
                    
            between_df.to_csv(self.output_directory + "\\" + category + "_between.csv" )  # report as csv ex: deodorant-parfum_between.csv
            within_df.to_csv(self.output_directory + "\\" + category + "_within.csv" )  # report as csv ex:deodorant-parfum_within.csv
            end_category = time.time()
            print( category + " evaluated in: " + str(end_category-start_category) + " seconds")
                    
                
                
                
