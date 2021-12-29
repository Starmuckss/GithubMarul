# -*- coding: utf-8 -*-
"""
Created on Thu Aug 19 17:01:45 2021

@author: HP
"""
import pandas as pd
import os
import numpy as np
from functools import reduce
import time

from itertools import combinations

root_directory = "C:\\Users\\HP\\Desktop\\11" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path + "\\" + "data\\semt_Share_of_identical_prices" # Data will be printed out here.

if not os.path.exists(output_directory):
    os.mkdir(output_directory)


input_directory = dir_path + "\\data\\pre_semt_correlation"  # Correlation and Share of identical prices both use the same data

sampled_dict_of_pairs = np.load(input_directory + "\\sampled_dict_of_pairs_for_correlation.npy",allow_pickle='TRUE').item()    
   
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)


for category in sampled_dict_of_pairs.keys():
    category_df = pd.DataFrame()
    category_start = time.time()    
    districts_present_in_data = set()
    for district in sampled_dict_of_pairs[category]:
        district_df = pd.DataFrame()
        for pair in sampled_dict_of_pairs[category][district]:
            pos_name1 = pair[0][pair[0].find(dates[0]) + len(dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
            chain_name1 = pos_name1.split("-")[0] # returns cagdas
            district_name1 = pos_name1.split("-")[-1] # returns 4890
            
            pos_name2 = pair[1][pair[1].find(dates[0]) + len(dates[0])+1: pair[1].find(category)-1]
            chain_name2 = pos_name2.split("-")[0]
            district_name2 = pos_name2.split("-")[-1]
    
            try:    
                pos_1 = pd.read_pickle(input_directory + "\\" + pos_name1+"_"+category+ ".pkl")   
                pos_2 = pd.read_pickle(input_directory + "\\" + pos_name2+"_"+category+ ".pkl")
            except:
                continue
            
        
            merged = pd.merge(pos_1,pos_2,on=["Name","Code"]) # Merge two point of sale data
            
            col_1 = [x for x  in merged.columns if "_x" in x] # columns of pos 1 in merged
            col_2 = [x for x  in merged.columns if "_y" in x] # columns of pos 2 in merged
            
            columns_1 = [x[0:x.find("_")] for x in col_1] # columns without "_x",required for processing
            columns_2 = [x[0:x.find("_")] for x in col_2] # columns without "_y",required for processing
            
            #Split merged dataframe to make subtraction  
            df_1 = merged[["Name","Code"]+col_1] 
            df_2 = merged[["Name","Code"]+col_2]
            
            df_1.columns = ["Name","Code"]+ columns_1
            df_2.columns = ["Name","Code"] + columns_2
            
            # Find absolute difference and classify whether it is a shared price or not
            difference = df_1[columns_1] - df_2[columns_2]            
            abs_difference = abs(difference)
            share_of_identical_price = np.where(abs_difference < 0.01 , 1, 0)
            share_of_identical_price = pd.DataFrame(share_of_identical_price)
            
            pair_df = pd.concat([df_1[["Name","Code"]],share_of_identical_price],axis=1)            

            pair_df['days'] = [len(v[pd.notna(v)].tolist()) for v in share_of_identical_price.values]
            pair_df['sum'] = [sum(v[pd.notna(v)].tolist()) for v in share_of_identical_price.values]
            
            # Here we can implement a day condition
            pair_df["share_of_identical_price"] = pair_df["sum"] / pair_df["days"]
            pair_df = pair_df[["Name","Code","share_of_identical_price"]]
            
            # Calculations are pair are done. Now place the results in district dataframe.
            if len(district_df) == 0:
                district_df = pair_df.copy()
                district_df["pair_count"] = 1
            else:
                district_df = pd.merge(district_df, pair_df,how= "outer",on="Code")
                
                district_df["pair_count"].fillna(0,inplace=(True))
                district_df["pair_count"] += 1
                
                district_df.loc[district_df['share_of_identical_price_y'].isnull(), 'pair_count'] -= 1
                
                check_for_nan = district_df['Name_x'].isnull()
                district_df.loc[district_df['Name_x'].isnull(),"Name_x"] = district_df["Name_y"].loc[check_for_nan] 
                
                check_for_nan = district_df['Name_y'].isnull()
                district_df.loc[district_df['Name_y'].isnull(),"Name_y"] = district_df["Name_x"].loc[check_for_nan] 
                
                district_df.fillna(0,inplace=True)
                district_df["share_of_identical_price"] = district_df["share_of_identical_price_x"] + district_df["share_of_identical_price_y"]
                
                district_df.drop(labels = ["share_of_identical_price_x","share_of_identical_price_y","Name_y"],axis = 1,inplace=True)
                district_df.rename(columns = {"Name_x":"Name"},inplace = True)
            
        if len(district_df) > 0:
            
            district_df[district] = district_df["share_of_identical_price"] / district_df["pair_count"]
            districts_present_in_data.add(district)
            # pair count elimination here!     
            district_df.drop(labels = ["share_of_identical_price","pair_count"],axis = 1,inplace=True)
        if len(category_df) == 0:
            try:
                category_df = district_df.copy()
            except KeyError:
                pass
        else:
            # merge approach
            try:
                category_df = pd.merge(category_df,district_df,on=["Name","Code"],how="outer")
            except KeyError:
                pass
    if len(category_df) > 0:
        category_df = category_df[["Name","Code"]+list(districts_present_in_data)] # eliminate unnecessary columns
    category_df.to_pickle(output_directory+"//"+category +"_"+"semt_share_of_identical_prices.pkl") #Save file
           
                
                
                