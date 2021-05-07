# -*- coding: utf-8 -*-
"""
Created on Mon Apr 19 15:33:05 2021
Find average log prices of point of sale data at product level
 and take the absolute difference with other PoS, for both between and within. 
 within: PoS from the same chain
 between: PoS from different chains
Second metric in Uniform Pricing in US Retail Chains 
@author: Sefa
rename the file: abs_log_prc_diff
"""
import pandas as pd
import os
from functools import reduce
import statistics
import time
from itertools import combinations
import numpy as np
from collections import defaultdict

root_directory = "C:\\Users\\HP\\Desktop\\11" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path+"\\abs_log_prc_diff" # Data will be printed out here
if not os.path.exists(output_directory): # create the folder if not exists already
    os.mkdir(output_directory)

intermediate_output_directory = dir_path + "\\temp_abs_log_prc_diff" # Preprocessed data will be recorded here
if not os.path.exists(intermediate_output_directory): # create the folder if not exists already
    os.mkdir(intermediate_output_directory) 

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

category_select = ["sut"]
# find pos from 3 different days, then merge them
all_posF = find_distinct_pos(root_directory+"\\"+dates[0]) # first day 
all_posM = find_distinct_pos(root_directory+"\\"+dates[ len(dates)//2 ]) #middle day
all_posL = find_distinct_pos(root_directory+"\\"+dates[-1]) # last day

all_pos_total = defaultdict(list)

for category in all_posL.keys():
    all_pos_total[category] = all_posF[category] + all_posM[category] + all_posL[category] # combine all
    
    pos_dates_fixed = [ pos[:len(root_directory)+1] + dates[0] + pos[len(root_directory) + 3:]  for pos in all_pos_total[category]] #fix the date in first date so
                                                                                                                                    #we can find duplicates
    all_pos_total[category] = list(set(pos_dates_fixed)) #  drop duplicates by taking the set first then turning it into a regular list



dict_of_pairs = get_pairs(all_pos_total) # pairs 

count_pairs_within = defaultdict(list) # Allows us to count how many pairs for a particular chain
count_pairs_between = defaultdict(list)

sample_dict_of_pairs = defaultdict(list) # preprocessed data will be saved here keys:category values: list contains pairs
 
# Below in the for loop: we create a panel data (all products in a category & prices observed in several days)
# than we take average over days
# calculate the difference of averages, across pairs
# save the averages in pkl files.
start_preprocessing = time.time()
for category in category_select:
    for pair in dict_of_pairs[category]:                 
        pair = [*pair]
        pos_name_1 = pair[0][pair[0].find(dates[0]) + len(dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
        chain_name_1 = pos_name_1.split("-")[0] # returns cagdas
        
        pos_name_2 = pair[1][pair[1].find(dates[0]) + len(dates[0])+1: pair[1].find(category)-1] # returns cagdas-chain-xxxx
        chain_name_2 = pos_name_2.split("-")[0]
        
        chain_names_of_pair = [chain_name_1,chain_name_2]
        
        # Check if we reached maximum pair count. If we are already at max, skip the preprocessing step
        if chain_name_1 == chain_name_2:
            if len(count_pairs_within[chain_names_of_pair[0]+"_"+category])>=200: 
                continue
        else:
            if len(count_pairs_between[chain_names_of_pair[0]+"_"+category])>=200 and len(count_pairs_between[chain_names_of_pair[1]+"_"+category])>=200: 
                continue # I used "and" to be safe. I have the concern for ex. pair : cagdas-aypa
                         # cagdas has 200 pairs but aypa has 100, in order not to limit aypa, I took this pair into consideration but with that, cagdas has more than 200
                         # burada belki farklı bir yol izlenebilir, konuşabiliriz
        
        for pos in pair:
            # Get chain name, from .pkl path. 

            pos_name = pos[pos.find(dates[0]) + len(dates[0])+1: pos.find(category)-1] # returns cagdas-chain-xxxx
            chain_name = pos_name.split("-")[0]
            log_average_df_directory = intermediate_output_directory + "\\" + pos_name+"_"+category+ ".pkl"

            if not os.path.exists(log_average_df_directory):
                pos_daily = [] # pos_daily will be populated by daily dataframes for that point of sale
                dates_in_analysis = list() # dates the pos has data in that category
    
                try:
                    for date in dates:
                        pos_daily.append(single_branch_prices_category(pos,date))   # time-series price data from pos_1 pos of the list
                        dates_in_analysis.append(date)
    
                except FileNotFoundError:
                    pass 
            
            
                if len(dates_in_analysis) > 0:  
                    pos_merge = reduce(lambda left,right: pd.merge(left,right,on='Code',how="outer",suffixes=("","_y")),pos_daily) # OUTER merge
                else:
                    continue
            
            #drop duplicate columns
                pos_merge = pos_merge.loc[:,~pos_merge.columns.duplicated()]
                
                pos_merge["Num_days_sold"] = [len(v[pd.notna(v)].tolist()) for v in pos_merge[dates_in_analysis].values] # day count of a particular product sold
                pos_merge["Mean_log_price"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in pos_merge[dates_in_analysis].values] # Mean_price as a column
                pos_merge = pos_merge[pos_merge["Num_days_sold"] > len(dates)//2] # take products if product has been sold at least 50% of days
                
                # Output the preprocessed data
                pos_merge[["Name","Code" ,"Mean_log_price"]].to_pickle(log_average_df_directory) 
        
        # count the pairs
        if chain_names_of_pair[0] == chain_names_of_pair[1]:
            count_pairs_within[chain_names_of_pair[0]+"_"+category] += [pair]
        else:
            count_pairs_between[chain_names_of_pair[0]+"_"+category] += [pair]
            count_pairs_between[chain_names_of_pair[1]+"_"+category] += [pair]
        sample_dict_of_pairs[category] += [pair]
       
end_preprocessing = time.time()

print("preprocessing took " + str(end_preprocessing-start_preprocessing)+" seconds")   
#%%
for category in category_select:
    start_category = time.time()
    # If pair is within, its dataframe will be stored in within, same for between
    # defaultdict is a dictionary with a useful mechanic, when an unknown key is created, It automatically creates an empty value with type entered as parameter
    # helps me to get rid long lines when I try to fill dictionaries with try-except
    within = defaultdict(list)  # there are empty dictionaries
    between = defaultdict(list)

    for pair in sample_dict_of_pairs[category]:                 
        pos_name1 = pair[0][pair[0].find(dates[0]) + len(dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
        chain_name1 = pos_name1.split("-")[0] # returns cagdas
        
        pos_name2 = pair[1][pair[1].find(dates[0]) + len(dates[0])+1: pair[1].find(category)-1]
        chain_name2 = pos_name2.split("-")[0]
        
        # since we already calculated averages, we read the data directly and start merging both pos in the pair
        pos_1= pd.read_pickle(intermediate_output_directory + "\\" + pos_name1+"_"+category+ ".pkl")   
        pos_2 = pd.read_pickle(intermediate_output_directory + "\\" + pos_name2+"_"+category+ ".pkl")
        
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
    
    for chain in between.keys():
        if len(between_df) == 0:
            between_df=  between[chain]
            between_df[chain] = between_df["absolute_difference"] / between_df["pair_count"] # abs_difference is divided to pair count, reported under chain name
            between_df.drop(["absolute_difference","pair_count"],axis =1, inplace = True)
        else:
            df = between[chain] 
            df[chain] = df["absolute_difference"] / df["pair_count"]
            between_df = pd.merge(between_df,df[["Code","Name",chain]],on = ["Name","Code"],how = "outer")
    
    for chain in within.keys():
        if len(within_df) == 0:
            within_df=  within[chain]
            within_df[chain] = within_df["absolute_difference"] / within_df["pair_count"]
            within_df.drop(["absolute_difference","pair_count"],axis =1, inplace = True)

        else:
            df = within[chain] 
            df[chain] = df["absolute_difference"] / df["pair_count"] 
            within_df = pd.merge(within_df,df[["Code","Name",chain]],on = ["Name","Code"],how = "outer")        
            
    between_df.to_csv(output_directory + "\\" + category + "_between.csv" )  # report as csv ex: deodorant-parfum_within.csv
    within_df.to_csv(output_directory + "\\" + category + "_within.csv" )  # report as csv ex:deodorant-parfum_within.csv
    end_category = time.time()
    print( category + " evaluated in: " + str(end_category-start_category) + " seconds")
            
            
            
            
