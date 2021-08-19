# -*- coding: utf-8 -*-
"""
Created on Tue Jun 29 11:10:18 2021

@author: HP
"""
import pandas as pd
import os
import time
import numpy as np
from collections import defaultdict

root_directory = "C:\\Users\\HP\\Desktop\\11" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path+"\\semt_correlation" # Data will be printed out here
if not os.path.exists(output_directory): # create the folder if not exists already
    os.mkdir(output_directory)
    
input_directory = dir_path + "\\pre_semt_correlation" # Preprocessed data will be recorded here

categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

category_select = category_list #for all categories use category_list
sampled_dict_of_pairs = np.load(input_directory + "\\sampled_dict_of_pairs_for_correlation.npy",allow_pickle='TRUE').item()    

dictionary_of_chain_names = defaultdict(set)
results = defaultdict()
for category in sampled_dict_of_pairs:
    start_category = time.time()
    category_results = defaultdict(lambda:pd.DataFrame())
    for district in sampled_dict_of_pairs[category]:

        for pair in sampled_dict_of_pairs[category][district]:
            start = time.time()
            pos_name1 = pair[0][pair[0].find(dates[0]) + len(dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
            chain_name1 = pos_name1.split("-")[0] # returns cagdas
            district_name1 = pos_name1.split("-")[-1]
            
            pos_name2 = pair[1][pair[1].find(dates[0]) + len(dates[0])+1: pair[1].find(category)-1]
            chain_name2 = pos_name2.split("-")[0]
            district_name2 = pos_name2.split("-")[-1]

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
            #correlation = np.nan_to_num(correlation, copy=True, nan=1, posinf=None, neginf=None)  # make nans into 1, if all prices are the same, corr is  (my assumption)       
            # Line above is commented. Keep the NaNs as they are
            
            result = pd.DataFrame() # a new dataframe to show results
            
            result["Name"] = final_merge_for_pair["Name_x"].copy()# take product names as a column 
            result["Code"] = final_merge_for_pair["Code"].copy() # take product codes as a column
            result[pos_name1+"_"+pos_name2] = correlation # save the correlations to result to column has the name of the pair for ex: "seyhanlar-market-erenkoy-7502_seyhanlar-market-sultanbeyli-4831"
            
            if len(category_results[district]) == 0: 
                category_results[district] = result
            else:
                category_results[district] = pd.merge(category_results[district],right=result, on=["Name","Code"],how = "outer")
            end = time.time()
            #print(end-start)
    
    
    
        pair_columns = category_results[district].columns.copy()    
        pair_columns = pair_columns.drop(["Name","Code"])
        
        # statistics.mean() function failed. So I first summed the correlations that are not NaNs. Then I find the pair_count
        # The pair count is the count of not NaN values in correlation data. Then I calculated sum / pair_count
        # The products that have NaN correlation for all pairs will have sum = 0 and pair count = 0. Since 0/0 is NaN, we have what we want in the end.
        category_results[district]["Sum"] = [sum(v[pd.notna(v)].tolist()) for v in category_results[district][pair_columns].values]    
        category_results[district]["pair_count"] = [len(v[pd.notna(v)].tolist()) for v in category_results[district][pair_columns].values]
        category_results[district]["Mean_correlation"] = category_results[district]["Sum"] /category_results[district]["pair_count"]  
        #category_results[district][["Name","Code","Mean_correlation","pair_count"]].to_csv(output_directory+"\\"+"correlation_"+district+"_"+category+".csv")
    results[category] = category_results    
            
            
            
            
            
            
            