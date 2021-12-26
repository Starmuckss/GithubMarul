# -*- coding: utf-8 -*-
"""
Find average log prices of point of sale data at product level (done in pre_abs_log_prc_diff.py)
and take the absolute difference with other PoS, for different districts (ilçe) 
Second metric in Uniform Pricing in US Retail Chains@author: HP
"""
import pandas as pd
import os
import time
import numpy as np
from collections import defaultdict

root_directory = "C:\\Users\\HP\\Desktop\\11" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path+"\\semt_log_average_abs_difference" # Data will be printed out here
if not os.path.exists(output_directory): # create the folder if not exists already
    os.mkdir(output_directory)

input_directory =dir_path + "\\pre_semt_log_average_difference" # Preprocessed data will be recorded here
if not os.path.exists(input_directory): # create the folder if not exists already
    os.mkdir(input_directory) 

categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

category_select = category_list  #for all categories use category_list
sampled_dict_of_pairs = np.load(input_directory + "\\sampled_dict_of_pairs_for_semt_log_average_difference.npy",allow_pickle='TRUE').item()

#%%
for category in category_select:
    start_category = time.time()
    district_data_list = defaultdict(list) # Data for each district will be stored here
    
    for district in sampled_dict_of_pairs[category]:                 
        for pair in sampled_dict_of_pairs[category][district]:
            pos_name1 = pair[0][pair[0].find(dates[0]) + len(dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
            # chain_name1 = pos_name1.split("-")[0] # returns cagdas
        
            pos_name2 = pair[1][pair[1].find(dates[0]) + len(dates[0])+1: pair[1].find(category)-1]
            chain_name2 = pos_name2.split("-")[0]
        
        # since we already calculated averages, we read the data directly and start merging both pos in the pair
            try:    
                pos_1 = pd.read_pickle(input_directory + "\\" + pos_name1+"_"+category+ ".pkl")   
                pos_2 = pd.read_pickle(input_directory + "\\" + pos_name2+"_"+category+ ".pkl")
            except:
                continue
            final_merge_for_pair = pd.merge(pos_1,right=pos_2,on = "Code") # inner merge on code 
            final_merge_for_pair["absolute_difference"] = np.abs(final_merge_for_pair["Mean_log_price_x"] - final_merge_for_pair["Mean_log_price_y"]) # abs difference of mean prices
            final_merge_for_pair.rename(columns = {"Name_x" : "Name" },inplace = True)
            final_merge_for_pair.drop(axis=1,columns=["Mean_log_price_x","Mean_log_price_y","Name_y"],inplace=True)                  
            
        # decide if this pair is a within or between
       
            if len(district_data_list[district]) == 0:                    
                district_data_list[district] = final_merge_for_pair
                district_data_list[district]["pair_count"] = 1
            
            else:
                district_data_list[district] = pd.merge(district_data_list[district], final_merge_for_pair,how= "outer",on="Code")
               
                district_data_list[district]["pair_count"].fillna(0,inplace=(True))
                district_data_list[district]["pair_count"] += 1
                
                district_data_list[district].loc[district_data_list[district]['absolute_difference_y'].isnull(), 'pair_count'] -= 1
                
                check_for_nan = district_data_list[district]['Name_x'].isnull()
                district_data_list[district].loc[district_data_list[district]['Name_x'].isnull(),"Name_x"] = district_data_list[district]["Name_y"].loc[check_for_nan] 
                
                check_for_nan = district_data_list[district]['Name_y'].isnull()
                district_data_list[district].loc[district_data_list[district]['Name_y'].isnull(),"Name_y"] = district_data_list[district]["Name_x"].loc[check_for_nan] 
                
                district_data_list[district].fillna(0,inplace=True)
                district_data_list[district]["absolute_difference"] = district_data_list[district]["absolute_difference_x"] + district_data_list[district]["absolute_difference_y"]
                
                district_data_list[district].drop(labels = ["absolute_difference_x","absolute_difference_y","Name_y"],axis = 1,inplace=True)
                district_data_list[district].rename(columns = {"Name_x":"Name"},inplace = True)
                
                
    
    #Report district data
    category_df = pd.DataFrame()
    
    for district in district_data_list.keys():
        district_df =  district_data_list[district]
        district_df[district] =  district_df["absolute_difference"] / district_df["pair_count"]
        district_df = district_df[["Name","Code",district]]
        
        if len(category_df) == 0:
            category_df =  district_df
            # abs_difference is divided to pair count, reported under chain name
            
        else:
            category_df = pd.merge(category_df,district_df,how="outer")
            
    try:
        category_df[["Name","Code"]+list(district_data_list.keys())].to_pickle(output_directory + "\\" + category + "_semt.pkl" )  # report as csv ex: deodorant-parfum_between.csv
    except KeyError:
       continue
    end_category = time.time()
    print( category + " evaluated in: " + str(end_category-start_category) + " seconds")
            
            
            
            