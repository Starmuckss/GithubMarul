# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 12:08:51 2021

@author: HP
"""
# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np
from functools import reduce
import time
from itertools import combinations

#root_directory = "C:\\Users\\HP\\Desktop\\10" # root directory contains everything
#dates = os.listdir(root_directory) # Assumed date files are directly under root directory
#dir_path = os.path.dirname(os.path.realpath(__file__))
#output_directory = dir_path + "\\" + "Share_of_identical_prices" # Data will be printed out here, You have to create this directory before using it.


root_directory = r"D:\moving_files_here_to_there\ist_anadolu\12" # root directory contains everything
#root_directory = r"C:\Users\mehmeto\Desktop\temp_marul\temp12"
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
output_directory = r"C:\Users\mehmeto\Desktop\temp_marul\results" # Data will be printed out here, You have to create this directory before using it.

if not os.path.exists(output_directory):
    os.mkdir(output_directory)
    
categories = pd.read_excel("categories_temp.xlsx")
category_list = list(categories.category)

def check_pairs_daily2(dataframe,pair):
    """
    Find if a pair has shared identical price or not.
    Shared identical price exists if: abs(log(price1) - log(price2)) < 0.01
    """
    df = dataframe.copy()
    
    
    df.dropna()    
    df[pair[0]] = np.log(dataframe[pair[0]])
    df[pair[1]] = np.log(dataframe[pair[1]])
    abs_difference =  abs(df[pair[0]]-df[pair[1]])
    df["abs_difference"] = abs_difference
    

    df.drop(pair,axis =1,inplace = True)
    df["indicator"] = np.where(df['abs_difference'] <0.01 , 1, 0)
    return df[["Names","Code","indicator"]] # date column has identical shared prices under it

def markets(directory):
    categories = pd.read_excel("categories_temp.xlsx")
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
    
def get_category_prices_marketwise(sube_list,directory,category):
    category_dataframes = []
    sube_names = []
    failed_merge = pd.DataFrame()
    
    for sube in sube_list:
        str_data = directory +"\\"+ sube + "\\" + category + ".pkl"
        df = pd.read_pickle(str_data)
        if not (df.isnull().values.all()) and " bu category de urun yok" not in df.Price.values:
            df.drop(["Link","Date","Price_old"],axis=1,inplace=True)
            sube_name = sube
            #sube_location = sube_name.split("-")[-1] implement location later,I dont know how i will do it now
            sube_names.append(sube_name)   
            df.columns = ["Name",sube_name,"Code"]    
            #df["Location"] = sube_location
            df[sube_name].replace(to_replace=",",value =".",regex = True, inplace = True)
            df[sube_name].replace(to_replace=" TL",value ="",regex = True, inplace = True)
            df[sube_name] = df[sube_name].astype(float) # these three lines cast prices from str into double
            df = df[df[sube_name] != 0] # Drop 0's. Maybe there is a better way to fix this problem
            df.drop_duplicates("Code",inplace=True) # maybe drop both of the products which have the same code, since it is a rare thing to see a code afflicted with 2 product names
            category_dataframes.append(df)          # we wouldnt lose much data, also we would be on the safe side. 
        del(df)
    if len(category_dataframes) == 1: 
        one_sube = category_dataframes[0]
        one_sube.rename(columns={'Name': 'Names'})
        return failed_merge
    try:
        final_category_dataframe = reduce(lambda left,right: pd.merge(left,right,on='Code',how="outer"),category_dataframes)    
    except TypeError:
        return failed_merge
    if len(final_category_dataframe) > 0: # I added this if and else, if final_category_dataframe is empty, dont use string functions
        try:
            cols = ["Name","Name_x","Name_y"]
            final_category_dataframe["Names"] = final_category_dataframe[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
            final_category_dataframe["Names"] = final_category_dataframe["Names"].str.split(",").str.get(0) # there will be multiple names, take the first.
            final_category_dataframe.drop(cols,axis="columns",inplace=(True)) #Drop all other name columns.
        
        except: # For certain categories, the column "Name" drops, only Name_x and Name_y is left.This try-except solves this. 
            cols = ["Name_x","Name_y"]
            final_category_dataframe["Names"] = final_category_dataframe[cols].apply(lambda x: ','.join(x.dropna()), axis=1) 
            final_category_dataframe["Names"] = final_category_dataframe["Names"].str.split(",").str.get(0) 
            final_category_dataframe.drop(cols,axis="columns",inplace=(True))
            
        colums_to_show = ["Names","Code"] + sube_names
        return final_category_dataframe[colums_to_show]   
    else:
        return failed_merge
market_dictionary = markets(root_directory+"\\"+dates[2]) # burası problemli, pairler günlere göre değişiyor   

#category_select = ["cay-kahve-2"] 
category_select = category_list


for category in category_select:
    category_df = pd.DataFrame()
    category_start = time.time()    
    for market in market_dictionary.keys(): 
        
        iterating_market_df = pd.DataFrame()
        
        sube_names = market_dictionary[market]
        market_pairs_final_list = []
        
        market_shared_prices = {}
        list_of_pairs = []
        pairs = combinations(sube_names, 2)
        
        for pair in pairs:  # pairs consists of tuples, make each pair a list.
            list_of_pairs.append([*pair])
            
        if len(list_of_pairs) == 0:
            print("no pairs in " + market)
            continue
        
        for pair in list_of_pairs:
            # start = time.time()
            
            iterating_df_of_a_pair = pd.DataFrame()
 
            for date in dates: 
                directory = root_directory+"\\"+date # Change directory to get a different day
                if os.path.exists(directory +"\\"+ pair[0] + "\\" + category + ".pkl") and os.path.exists(directory +"\\"+ pair[1] + "\\" + category + ".pkl"): #Check if these pair exists in this date
                    pair_prices = get_category_prices_marketwise(pair, directory, category)
                    if len(pair_prices) != 0:
                        
                        if len(iterating_df_of_a_pair) == 0:    
                            iterating_df_of_a_pair = check_pairs_daily2(pair_prices, pair)
                            iterating_df_of_a_pair["Tot_days"] = 1 
                        else:    
                            temp_df = check_pairs_daily2(pair_prices, pair)
                            iterating_df_of_a_pair = pd.merge(iterating_df_of_a_pair,temp_df,how="outer",on="Code")
                            
                            iterating_df_of_a_pair["Tot_days"].fillna(0,inplace=True)
                            iterating_df_of_a_pair["Tot_days"] += 1
                            
                            
                            iterating_df_of_a_pair.loc[iterating_df_of_a_pair['indicator_y'].isnull(), 'Tot_days'] -= 1
                            # check_for_nan = iterating_df_of_a_pair['indicator_y'].isnull()
                            # iterating_df_of_a_pair["Tot_days"].loc[check_for_nan] -= 1 
                            
                            check_for_nan = iterating_df_of_a_pair['Names_x'].isnull()
                            iterating_df_of_a_pair.loc[iterating_df_of_a_pair['Names_x'].isnull(),"Names_x"] = iterating_df_of_a_pair["Names_y"].loc[check_for_nan] 
                            # iterating_df_of_a_pair["Names_x"].loc[check_for_nan] = iterating_df_of_a_pair["Names_y"].loc[check_for_nan] 
                            
                            check_for_nan = iterating_df_of_a_pair['Names_y'].isnull()
                            iterating_df_of_a_pair.loc[iterating_df_of_a_pair['Names_y'].isnull(),"Names_y"] = iterating_df_of_a_pair["Names_x"].loc[check_for_nan] 
                            # iterating_df_of_a_pair["Names_y"].loc[check_for_nan] = iterating_df_of_a_pair["Names_x"].loc[check_for_nan] 
                            
                            
                            iterating_df_of_a_pair.fillna(0,inplace=True)
                            

                            iterating_df_of_a_pair["indicator"] = iterating_df_of_a_pair["indicator_x"] + iterating_df_of_a_pair["indicator_y"]
                           
                            iterating_df_of_a_pair.drop(labels = ["indicator_x","indicator_y","Names_y"],axis = 1,inplace=True)
                            iterating_df_of_a_pair.rename(columns = {"Names_x":"Names"},inplace = True)
                            
                    
            if len(iterating_df_of_a_pair) > 0:
                iterating_df_of_a_pair = iterating_df_of_a_pair[iterating_df_of_a_pair['Tot_days'] >= len(dates)*0.5] # uncomment to filter days
                iterating_df_of_a_pair["share_of_identical_price"] = iterating_df_of_a_pair["indicator"] / iterating_df_of_a_pair["Tot_days"]
                iterating_df_of_a_pair.drop(labels = ["Tot_days","indicator"],axis=1,inplace=True)
            else:
                continue
                
            
            if len(iterating_market_df) == 0:
                iterating_market_df = iterating_df_of_a_pair.copy()
                iterating_market_df["pair_count_within"] = 1
            else:
                iterating_market_df = pd.merge(iterating_market_df, iterating_df_of_a_pair,how= "outer",on="Code")
                
                
                iterating_market_df["pair_count_within"].fillna(0,inplace=(True))
                iterating_market_df["pair_count_within"] += 1
                
                iterating_market_df.loc[iterating_market_df['share_of_identical_price_y'].isnull(), 'pair_count_within'] -= 1
                
                check_for_nan = iterating_market_df['Names_x'].isnull()
                iterating_market_df.loc[iterating_market_df['Names_x'].isnull(),"Names_x"] = iterating_market_df["Names_y"].loc[check_for_nan] 
                
                check_for_nan = iterating_market_df['Names_y'].isnull()
                iterating_market_df.loc[iterating_market_df['Names_y'].isnull(),"Names_y"] = iterating_market_df["Names_x"].loc[check_for_nan] 
                
                iterating_market_df.fillna(0,inplace=True)
                iterating_market_df["share_of_identical_price"] = iterating_market_df["share_of_identical_price_x"] + iterating_market_df["share_of_identical_price_y"]
                
                iterating_market_df.drop(labels = ["share_of_identical_price_x","share_of_identical_price_y","Names_y"],axis = 1,inplace=True)
                iterating_market_df.rename(columns = {"Names_x":"Names"},inplace = True)
                    
                    
            
            
            # end = time.time()    
            # print("pair evaluated at: " + str(end-start)+" seconds") # see how much time it takes to evaluate one pair
        
        if len(iterating_market_df) > 0: 
            # iterating_market_df = iterating_market_df[iterating_market_df['pair_count_within'] >= len(list_of_pairs)*0.8] # uncomment to filter pair count
            iterating_market_df["Share_of_identical_within"] = iterating_market_df["share_of_identical_price"] / iterating_market_df["pair_count_within"]
            iterating_market_df["Chain_name"] = market
            
            iterating_market_df = iterating_market_df[["Names","Code","Share_of_identical_within","pair_count_within","Chain_name"]]
        
    
        if len(category_df) == 0:
            category_df = iterating_market_df.copy()
        else:
            category_df = category_df.append(iterating_market_df)
    category_df.to_csv(output_directory+"//"+category +"_"+"within_chain_identical_prices.csv")
    category_end = time.time()    
    print("category evaluated at: " + str(category_end-category_start)+" seconds")
    


    