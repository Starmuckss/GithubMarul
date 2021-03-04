# -*- coding: utf-8 -*-
"""
Created on Sun Feb 28 14:25:45 2021

@author: HP
"""
import pandas as pd
import os
import numpy as np
from functools import reduce
import statistics
import time
from itertools import combinations
import glob


root_directory = "C:\\Users\\HP\\Desktop\\10" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path+"\\Log Average" # Data will be printed out here, on the same folder where code is.

if not os.path.exists(output_directory):
    os.mkdir(output_directory)

def single_branch_prices(directory,branch_name,date):
    """
    Not used in this script,a specific version used in name single_branch_prices_category
    """
    all_branch_prices = pd.DataFrame()
    branch_directory = os.path.join(directory, branch_name)
    for file in os.listdir(branch_directory):
        data_directory = os.path.join(directory, branch_name,file)
        df = pd.read_pickle(data_directory)
        df.drop(labels = ["Price_old","Link","Date"],axis=1,inplace=True)
        df.dropna(subset=["Name"],inplace=True)
        
        df["Price"].replace(to_replace=",",value =".",regex = True, inplace = True)
        df["Price"].replace(to_replace=" TL",value ="",regex = True, inplace = True)
        df["Price"] = df["Price"].astype(float)
        df= df[df['Price'] != 0]
        df[date] = np.log10(df["Price"]) 
        
         # drop 0 prices
        
        all_branch_prices = all_branch_prices.append(df)
        
    all_branch_prices.drop_duplicates("Code",inplace=True)
    return all_branch_prices[["Name","Code",date]]    

def single_branch_prices_category(directory,branch_name,date,category):
    """
    Parameters
    ----------
    directory : String
        directory of where whole data is in the memory.
    branch_name : String
        branch(sube) name.
    date : String
        The day which data is collected.
    category : String
        The category under branch(Sube) data. The data is collected category based. Under *category.pkl,
        there are individual product data.

    Returns
    -------
    df : pd.DataFrame
        The dataframe consists of the products:"Name","Code","price" 
        and a "Date" column which is the logarithm(10)_of_price_column

    """    

    data_directory = os.path.join(directory, branch_name) + "\\" + category + ".pkl" 
    df = pd.read_pickle(data_directory)
    df.drop(labels = ["Price_old","Link","Date"],axis=1,inplace=True)
    df.dropna(subset=["Name"],inplace=True)
    
    df["Price"].replace(to_replace=",",value =".",regex = True, inplace = True)
    df["Price"].replace(to_replace=" TL",value ="",regex = True, inplace = True)
    df["Price"] = df["Price"].astype(float) #format prices as float
    df= df[df['Price'] != 0] # drop 0 prices
    df[date] = np.log10(df["Price"])# LOG10 of price column
    
    # drop duplicates of the code column, if we don't drop duplicates, merge function goes crazy
    df.drop_duplicates("Code",inplace=True)
                                        
    
    return df
        
       

def markets(directory):
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

market_dictionary = markets(root_directory+"\\"+dates[2])
category_select = category_list
for category in category_select:
    report_dataframe = pd.DataFrame()
    report = []
    markets_in_analysis = []
    for market in market_dictionary.keys():   
        if len(market_dictionary[market]) >1:    
             markets_in_analysis += [market] #  keep track of each market occurs in analysis
             sube_names = market_dictionary[market]
             market_df = pd.DataFrame()
             pairs = combinations(sube_names, 2)
             
             
             for pair in pairs:
                 start = time.time()
                 
                 # pair has 2 branches inside, first and second
                 first = [] 
                 second = []
                 pair = [*pair]
                 dates_in_analysis= [] # keep track of dates which the pairs have sold items
                 
                 for date in dates:
                     directory = root_directory+"\\"+date
                     try:
                         # populate first and second with data 
                         first.append(single_branch_prices_category(directory, pair[0],date,category)) # maybe if empty drop it?
                         second.append(single_branch_prices_category(directory, pair[1],date,category))
                         dates_in_analysis.append(date)
    
                     except FileNotFoundError:
                         break
                 
                 # inner merge first and second, then we will have the products sold by branches EVERYDAY   
                 if len(dates_in_analysis) > 0:  
                     first_merge = reduce(lambda left,right: pd.merge(left,right,on='Code',how="inner",suffixes=("","_y")),first) # merge inner, yani hergünde olan ürünler var!
                     second_merge = reduce(lambda left,right: pd.merge(left,right,on='Code',how="inner",suffixes=("","_y")),second)
                 else:
                     break
                 #drop duplicate columns
                 first_merge = first_merge.loc[:,~first_merge.columns.duplicated()]
                 second_merge = second_merge.loc[:,~second_merge.columns.duplicated()]
                 
                 #drop duplicate columns
                 if len(first) >1:
                     first_merge.drop(["Name_y"],axis=1,inplace = True)
                 if len(second) >1:
                     second_merge.drop(["Name_y"],axis=1,inplace = True)
                 
                 # find the mean of log price   
                 first_merge["Mean_log_price"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in first_merge[dates_in_analysis].values] # Mean_price as a column
                 second_merge["Mean_log_price"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in second_merge[dates_in_analysis].values] # Mean_price as a column
        
                 # merge the pair.inner merge, both have the products at the end of merge
                 final_merge_for_pair = pd.merge(first_merge[["Name","Code" ,"Mean_log_price"]],right=second_merge[["Name","Code" ,"Mean_log_price"]],on = "Code")
                 final_merge_for_pair["absolute_difference"] = np.abs(final_merge_for_pair["Mean_log_price_x"] -final_merge_for_pair["Mean_log_price_y"])
                 final_merge_for_pair.rename(columns = {"Name_x" : "Name" },inplace = True)
                 final_merge_for_pair.drop(axis=1,columns=["Mean_log_price_x","Mean_log_price_y","Name_y"],inplace=True)
                 
                 # pair is evaluated. Start to populate report for a market
                 if len(market_df) == 0: 
                    market_df = final_merge_for_pair.copy()
                    market_df["pair_count"] = 1
                 else:
                    market_df = pd.merge(market_df, final_merge_for_pair,how= "outer",on="Code")
                    
                    market_df["pair_count"].fillna(0,inplace=(True))
                    market_df["pair_count"] += 1
                    
                    market_df.loc[market_df['absolute_difference_y'].isnull(), 'pair_count'] -= 1
                    
                    check_for_nan = market_df['Name_x'].isnull()
                    market_df.loc[market_df['Name_x'].isnull(),"Name_x"] = market_df["Name_y"].loc[check_for_nan] 
                    
                    check_for_nan = market_df['Name_y'].isnull()
                    market_df.loc[market_df['Name_y'].isnull(),"Name_y"] = market_df["Name_x"].loc[check_for_nan] 
                    
                    market_df.fillna(0,inplace=True)
                    market_df["absolute_difference"] = market_df["absolute_difference_x"] + market_df["absolute_difference_y"]
                    
                    market_df.drop(labels = ["absolute_difference_x","absolute_difference_y","Name_y"],axis = 1,inplace=True)
                    market_df.rename(columns = {"Name_x":"Name"},inplace = True)
                 
                 end = time.time()
                 print("pair evaluated at: " + str(end-start) +" seconds" )        
             
             if len(market_df) > 0:
                 market_df[market] = market_df["absolute_difference"] / market_df["pair_count"]                
                 report.append(market_df[["Name","Code",market]]) 
             else:
                 markets_in_analysis.remove(market)
        else:
            continue
    if len(report)>1:
        report_dataframe = reduce(lambda left,right: pd.merge(left,right,on='Code',how="outer"),report) 
    #drop unnecessary name columns
        try:
            cols = ["Name","Name_x","Name_y"]
            report_dataframe["Names"] = report_dataframe[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
            report_dataframe["Names"] = report_dataframe["Names"].str.split(",").str.get(0) # there will be multiple names, take the first.
            report_dataframe.drop(cols,axis="columns",inplace=(True))
        except KeyError:
            cols = ["Name_x","Name_y"]
            report_dataframe["Names"] = report_dataframe[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
            report_dataframe["Names"] = report_dataframe["Names"].str.split(",").str.get(0) # there will be multiple names, take the first.
            report_dataframe.drop(cols,axis="columns",inplace=(True))
            
    #report the final results
        report_dataframe  = report_dataframe[["Names","Code"]+markets_in_analysis]
        report_dataframe.to_excel(output_directory+"\\"+category+".xlsx")
    elif len(report) == 1:
        report_dataframe.to_excel(output_directory+"\\"+category+".xlsx")