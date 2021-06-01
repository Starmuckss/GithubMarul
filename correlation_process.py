# -*- coding: utf-8 -*-
"""
Created on Thu May 27 09:21:17 2021

@author: HP
"""
import pandas as pd
import os
import time
import numpy as np
import statistics
from collections import defaultdict

root_directory = "C:\\Users\\HP\\Desktop\\11" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path+"\\correlation" # Data will be printed out here
if not os.path.exists(output_directory): # create the folder if not exists already
    os.mkdir(output_directory)
    
input_directory =dir_path + "\\pre_correlation" # Preprocessed data will be recorded here

categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

category_select = category_list #for all categories use category_list
sampled_dict_of_pairs = np.load(input_directory + "\\sampled_dict_of_pairs_for_correlation.npy",allow_pickle='TRUE').item()    
category_results = dict()
within_results= dict()
between_results = dict()


for category in category_select:
    start_category = time.time()
    within_results[category] = pd.DataFrame()
    between_results[category] = pd.DataFrame()
    for pair in sampled_dict_of_pairs[category]:                 
        # Take the point of sale name and chain names for both stores
        pos_name1 = pair[0][pair[0].find(dates[0]) + len(dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
        chain_name1 = pos_name1.split("-")[0] # returns cagdas
        
        pos_name2 = pair[1][pair[1].find(dates[0]) + len(dates[0])+1: pair[1].find(category)-1]
        chain_name2 = pos_name2.split("-")[0]
        
        # since we already calculated the prices for a whole month, we read the data directly and start merging both pos in the pair
        try:    
            pos_1 = pd.read_pickle(input_directory + "\\" + pos_name1+"_"+category+ ".pkl")   
            pos_2 = pd.read_pickle(input_directory + "\\" + pos_name2+"_"+category+ ".pkl")
        except:
            continue
        
        final_merge_for_pair = pd.merge(pos_1,right=pos_2,on = "Code") # inner merge on code 
        # after merging 2 pairs, there will be columns named like this: "01_x","01_y","13_x","13_y". The columns that have "_x" in them are the first store's prices
        # and "_y" are the second store' prices. I first differentiate them, then I take the correlation for each row. (rows have the products)  
        cols1 = [x for x in final_merge_for_pair.columns if "_x" in x] 
        cols2 = [x for x in final_merge_for_pair.columns if "_y" in x]
        
        df1 = final_merge_for_pair[cols1].copy()
        df1.drop(labels = ["Name_x"],axis=1,inplace=True)
        df1.columns = [x[:2] for x in df1.columns]
        
        df2 = final_merge_for_pair[cols2].copy()
        df2.drop(labels = ["Name_y"],axis=1,inplace=True)
        df2.columns = [x[:2] for x in df2.columns]

        correlation =  df1.corrwith(df2, axis = 1) # IF all the prices are same, the function returns NAN, probably it thinks the prices as a constant
        correlation = np.nan_to_num(correlation, copy=True, nan=1, posinf=None, neginf=None)  # make nans into 1, if all prices are the same, corr is  (my assumption)       
        
        result = pd.DataFrame() # a new dataframe to show results
        
        result["Name"] = final_merge_for_pair["Name_x"].copy()# take product names as a column 
        result["Code"] = final_merge_for_pair["Code"].copy() # take product codes as a column
        result[pos_name1+"_"+pos_name2] = correlation # save the correlations to result under the name of the pair for ex: "seyhanlar-market-erenkoy-7502_seyhanlar-market-sultanbeyli-4831"
        # this thing prevents double counting I guess. I think we double count when we talk about sampling. I took a note about this issue.
        
        if chain_name1 == chain_name2: # If the chains are the same, the pair is a within pair
            if len(within_results[category]) == 0: #
                within_results[category] = result
            else:
                within_results[category] = pd.merge(within_results[category],right=result, on=["Name","Code"],how = "outer")

        else:
            if len(between_results[category]) == 0:
                between_results[category] = result
            else:
                between_results[category] = pd.merge(between_results[category],right=result, on=["Name","Code"],how = "outer")

        
        
    
    within_results[category]["Mean_correlation"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in within_results[category][within_results[category].columns[2:]].values]    
    between_results[category]["Mean_correlation"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in between_results[category][between_results[category].columns[2:]].values]    
    
    within_results[category]["pair_count"] = [len(v[pd.notna(v)].tolist()) for v in within_results[category][within_results[category].columns[2:]].values]    
    between_results[category]["pair_count"] = [len(v[pd.notna(v)].tolist()) for v in between_results[category][between_results[category].columns[2:]].values]    
    
    
    try:
        within_results[category][["Name","Code","Mean_correlation","pair_count"]].to_csv(output_directory+"\\correlation_for_within"+category+".csv")
        between_results[category][["Name","Code","Mean_correlation","pair_count"]].to_csv(output_directory+"\\correlation_for_between"+category+".csv")
    except KeyError:
        print(category+ " is empty!")
    
    end_category = time.time()
    print(category + " took "+str(end_category-start_category) + " seconds")    
    
        