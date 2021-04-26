# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 15:33:05 2021
Find average log prices of point of sale data for both between and within. Second metric in Uniform Pricing in US Retail Chains 
@author: HP
rename the file: abs_log_prc_diff
"""
import pandas as pd
import os
from functools import reduce
import statistics
import time
from itertools import combinations
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict

root_directory = "C:\\Users\\HP\\Desktop\\11" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path+"\\log_average" # Data will be printed out here, You have to create this directory before using it.
if not os.path.exists(output_directory):
    os.mkdir(output_directory)
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

def find_distinct_pos(directory):
    """
    Get only one point of sale from redundant point of sales
    Farkli yerlere satis yapan bakkallar icin sadece 1 bolgeye olan satislari tutar
    boylece her pos bir kere dataya girer
    """
    distinct_pos = {}
    check_distinct  = {}
    for subdir, dirs, files in os.walk(directory):    # Pos dictionary, keys: Categories, values: File paths(eg.: ...aypa/peynirler.pkl)
        for category in category_list:
            check_distinct[category] = []
            for direc in dirs:
                splitted_pos_name =  direc.split("-")
                pos_name = "-".join(splitted_pos_name[0:len(splitted_pos_name)-1])
                check_distinct[category] += [pos_name] 
                if(check_distinct[category].count(pos_name) == 1):     
                    try:
                        distinct_pos[category] += [(directory+"\\"+direc +"\\"+ category+".pkl")]
                    except:
                        distinct_pos[category] = [(directory+"\\"+direc +"\\"+ category+".pkl")]
                        # e.g.: 
    return distinct_pos 

def get_pairs(all_pos):
    """
    all_pos is a list paths, all_pos is returned by function find_distinct_pos
    this func returns a dictionary of all pairs of pos (both within and between)  
    """
    all_pairs = [combinations(pos_category,2) for pos_category in all_pos.values()]

    dict_of_pairs = defaultdict(list)
    for pos_category in all_pairs:
        for pair in pos_category:
            dict_of_pairs[pair[0][pair[0].rfind("\\")+1:-4]] += [pair]
    return dict_of_pairs   

def single_branch_prices_category(directory,date):
    """
    reorganizes the input data (daily) & takes the lof of prices
    Parameters
    ----------
    directory : String
        directory of where whole data is in the drive.
    date : String
        The day which data is collected.
    Returns
    -------
    df : pd.DataFrame
        The dataframe consists of the products:"Name","Code","price" 
        and a "Date" column which is the logarithm(N)_of_price_column
        
    """    
    # Burada path i modifiye ediyorum. çünkü pairleri sadece bir kere bulmuştum
    # path'de sadece gün kısmını değiştirerek bütün günlerin verisini toplayabiliyorum
    data_directory = directory[:len(root_directory)+1] + date + directory[len(root_directory) + 3:]
    
    df = pd.read_pickle(data_directory)
    df.drop(labels = ["Price_old","Link","Date"],axis=1,inplace=True)
    df.dropna(subset=["Name"],inplace=True)
    
    df["Price"].replace(to_replace=",",value =".",regex = True, inplace = True)
    df["Price"].replace(to_replace=" TL",value ="",regex = True, inplace = True)
    df["Price"] = df["Price"].astype(float) #format prices as float
    df= df[df['Price'] != 0] # drop 0 prices
    df[date] = np.log(df["Price"])# LN of price column
    
    # drop duplicates of the code column, if we don't drop duplicates, merge function goes crazy
    df.drop_duplicates("Code",inplace=True)
                                        
    
    return df[["Name","Code",date]]

category_select = ["sut","peynirler","online-meyve-siparisi"] # input categories
all_pos0 = find_distinct_pos(root_directory+"\\"+dates[0]) # All pos has keys:categories, values:true paths for each point of sale for that category
all_posM = find_distinct_pos(root_directory+"\\"+dates[ len(dates)//2 ])
all_posL = find_distinct_pos(root_directory+"\\"+dates[-1])

# merge all_pos0 all_posM all_posL and drop duplicates. The result should be named all_pos


dict_of_pairs = get_pairs(all_pos) # pairs 

# BElow in the for loop: we create a panel data (all products in a category & prices observed in several days)
# than we take average over days
# calculate the difference of averages, across pairs

for category in category_select:
    start_category = time.time()
    
    
    # first thing is to calculate and store average of log prices and get rid of dates
    # then use these averages for within and between estimations
    # use up to 200 pairs   
    
    
    # If pair is within, its dataframe will be stored in within, same for between
    # defaultdict is a dictionary with a useful mechanic, when an unknown key is created, It automatically creates an empty value with type entered as parameter
    # helps me to get rid long lines when I try to fill dictionaries with try-except
    within = defaultdict(list)  # there are empty dictionaries
    between = defaultdict(list)

    for pair in dict_of_pairs[category]:                 
        # pair has 2 branches inside, pos_1 and pos_2, both empty lists will be populated by daily dataframes
        pos_1 = [] 
        pos_2 = []
        dates_in_analysis = list()

        pair = [*pair]
        try:
            for date in dates:
                pos_1.append(single_branch_prices_category(pair[0],date))   # time-series price data from pos_1 pos of the list
                pos_2.append(single_branch_prices_category(pair[1],date))
                dates_in_analysis.append(date)

        except FileNotFoundError:
            pass 
        
        # BURASI degismeli. inner merge gerek yok. Ama gunlerin yuzde 50 sinden fazla fiyati NaN olan urunu atalim.
        # inner merge pos_1 and pos_2, then we will have the products sold by branches EVERYDAY   
        if len(dates_in_analysis) > 0:  
            pos_1_merge = reduce(lambda left,right: pd.merge(left,right,on='Code',how="inner",suffixes=("","_y")),pos_1) # merge inner, yani her günde olan ürünler var!
            pos_2_merge = reduce(lambda left,right: pd.merge(left,right,on='Code',how="inner",suffixes=("","_y")),pos_2)
        else:
            continue
        
        #drop duplicate columns
        pos_1_merge = pos_1_merge.loc[:,~pos_1_merge.columns.duplicated()]
        pos_2_merge = pos_2_merge.loc[:,~pos_2_merge.columns.duplicated()]
        
        pos_1_merge["Mean_log_price"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in pos_1_merge[dates_in_analysis].values] # Mean_price as a column
        pos_2_merge["Mean_log_price"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in pos_2_merge[dates_in_analysis].values] # Mean_price as a column
   
        # merge the pair.inner merge, both have the products at the end of merge
        final_merge_for_pair = pd.merge(pos_1_merge[["Name","Code" ,"Mean_log_price"]],right=pos_2_merge[["Name","Code" ,"Mean_log_price"]],on = "Code")
        final_merge_for_pair["absolute_difference"] = np.abs(final_merge_for_pair["Mean_log_price_x"] -final_merge_for_pair["Mean_log_price_y"])
        final_merge_for_pair.rename(columns = {"Name_x" : "Name" },inplace = True)
        final_merge_for_pair.drop(axis=1,columns=["Mean_log_price_x","Mean_log_price_y","Name_y"],inplace=True)
                 
        # Get market name, from .pkl path. 
        pos_name1 = pair[0][pair[0].find(dates[0]) + len(dates[0])+1: pair[0].find(category)-1] # returns cagdas-market-xxxx
        market_name1 = pos_name1.split("-")[0] # returns cagdas
        
        pos_name2 = pair[1][pair[1].find(dates[0]) + len(dates[0])+1: pair[1].find(category)-1]
        market_name2 = pos_name2.split("-")[0]
        
        # decide if this pair is a within or between
        if  market_name1 == market_name2:
            # within chain pair.
            if len(within[market_name1]) == 0:                    
                within[market_name1] = final_merge_for_pair
                within[market_name1]["pair_count"] = 1
            
            else:
                within[market_name1] = pd.merge(within[market_name1], final_merge_for_pair,how= "outer",on="Code")
               
                within[market_name1]["pair_count"].fillna(0,inplace=(True))
                within[market_name1]["pair_count"] += 1
                
                within[market_name1].loc[within[market_name1]['absolute_difference_y'].isnull(), 'pair_count'] -= 1
                
                check_for_nan = within[market_name1]['Name_x'].isnull()
                within[market_name1].loc[within[market_name1]['Name_x'].isnull(),"Name_x"] = within[market_name1]["Name_y"].loc[check_for_nan] 
                
                check_for_nan = within[market_name1]['Name_y'].isnull()
                within[market_name1].loc[within[market_name1]['Name_y'].isnull(),"Name_y"] = within[market_name1]["Name_x"].loc[check_for_nan] 
                
                within[market_name1].fillna(0,inplace=True)
                within[market_name1]["absolute_difference"] = within[market_name1]["absolute_difference_x"] + within[market_name1]["absolute_difference_y"]
                
                within[market_name1].drop(labels = ["absolute_difference_x","absolute_difference_y","Name_y"],axis = 1,inplace=True)
                within[market_name1].rename(columns = {"Name_x":"Name"},inplace = True)
            
        else: # between chain pair
            if len(between[market_name1]) == 0:                    
                between[market_name1] = final_merge_for_pair
                between[market_name1]["pair_count"] = 1
            
            else:
                between[market_name1] = pd.merge(between[market_name1], final_merge_for_pair,how= "outer",on="Code")
               
                between[market_name1]["pair_count"].fillna(0,inplace=(True))
                between[market_name1]["pair_count"] += 1
                
                between[market_name1].loc[between[market_name1]['absolute_difference_y'].isnull(), 'pair_count'] -= 1
                
                check_for_nan = between[market_name1]['Name_x'].isnull()
                between[market_name1].loc[between[market_name1]['Name_x'].isnull(),"Name_x"] = between[market_name1]["Name_y"].loc[check_for_nan] 
                
                check_for_nan = between[market_name1]['Name_y'].isnull()
                between[market_name1].loc[between[market_name1]['Name_y'].isnull(),"Name_y"] = between[market_name1]["Name_x"].loc[check_for_nan] 
                
                between[market_name1].fillna(0,inplace=True)
                between[market_name1]["absolute_difference"] = between[market_name1]["absolute_difference_x"] + between[market_name1]["absolute_difference_y"]
                
                between[market_name1].drop(labels = ["absolute_difference_x","absolute_difference_y","Name_y"],axis = 1,inplace=True)
                between[market_name1].rename(columns = {"Name_x":"Name"},inplace = True)
        
    
    #Report between and within chain data
    between_df = pd.DataFrame()
    within_df = pd.DataFrame()
    for market in between.keys():
        if len(between_df) == 0:
            between_df=  between[market]
            between_df[market] = between_df["absolute_difference"] / between_df["pair_count"]
            between_df.drop(["absolute_difference","pair_count"],axis =1, inplace = True)
        else:
            df = between[market] 
            df[market] = df["absolute_difference"] / df["pair_count"]
            between_df = pd.merge(between_df,df[["Code","Name",market]],on = ["Name","Code"],how = "outer")
    
    for market in within.keys():
        if len(within_df) == 0:
            within_df=  within[market]
            within_df[market] = within_df["absolute_difference"] / within_df["pair_count"]
            within_df.drop(["absolute_difference","pair_count"],axis =1, inplace = True)

        else:
            df = within[market] 
            df[market] = df["absolute_difference"] / df["pair_count"]
            within_df = pd.merge(within_df,df[["Code","Name",market]],on = ["Name","Code"],how = "outer")        
            
    between_df.to_csv(output_directory + "\\" + category + "_between.csv" )  
    within_df.to_csv(output_directory + "\\" + category + "_within.csv" )  
    end_category = time.time()
    print( category + " evaluated in: " + str(end_category-start_category) + " seconds")
            
            
            
            
