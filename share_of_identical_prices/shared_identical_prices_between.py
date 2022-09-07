# -*- coding: utf-8 -*-
"""
Created on Sat Mar 13 21:21:58 2021

@author: HP
"""
import pandas as pd
import os
import numpy as np
from functools import reduce
import statistics
import time
from itertools import combinations
from collections import Counter
import random

class shared_identical_prices_between_chain():
            
    def __init__(self,data_directory,year,month,output_directory,category_list):
    
        self.root_directory = data_directory
        self.year = year
        self.month = month
        
        if not os.path.exists(os.path.join(output_directory,year)): # create the folder if not exists already
            os.mkdir(os.path.join(output_directory,year)) 
        if not os.path.exists(os.path.join(output_directory,year,month)): # create the folder if not exists already
            os.mkdir(os.path.join(output_directory,year,month)) 
        self.output_directory = os.path.join(output_directory,year,month) 

        #categories = pd.read_excel("categories.xlsx")
        #category_list = list(categories.category)
        self.category_list = category_list # hangi categoriler var bunun oldugu liste
        self.dates = os.listdir(data_directory) # Assumed date files are directly under root directory

    def check_pairs_daily(self,dataframe,pair):
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
    
    def markets(self,directory):
      
        market_dictionary = {}
        check_distinct  = {}
        for subdir, dirs, files in os.walk(directory):   
            for category in self.category_list:
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
        
    def get_category_prices_marketwise(self,sube_list,directory,category):
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
    
    
    def process(self):    
        market_dictionary = self.markets(self.root_directory+"\\" + self.dates[2]) # burası problemli, pairler günlere göre değişiyor   
        
        all_sube = [] # all branches, not regarding markets.
        all_within_chain_pairs = [] # within-chain pairs. Every pair has the same market in the start of its sube_name
        for market in market_dictionary.keys(): #Get all the within_chain pairs. 
             all_sube += market_dictionary[market]
             
             temp_combination = combinations(market_dictionary[market], 2)
             temp_combination_list= []
             for pair in temp_combination:
                 temp_combination_list.append([*pair])
             all_within_chain_pairs += temp_combination_list
        
        between_chain_pairs = combinations(all_sube, 2)
        between_chain_pairs_list =[]
        counter_dict= {}
        max_chain_pair = 30 # Max number of a market duo considered. A duo is 2 market pairs. For ex. "ecemar-sariyer" is a duo
        for pair in between_chain_pairs:
            chain_pair = pair[0].split("-")[0]+"-"+ pair[1].split("-")[0]       #pair[0][0:pair[0].find("-")]+"-"+pair[1][0:pair[1].find("-")]
            if chain_pair.split("-")[0] != chain_pair.split("-")[1]:    
                try:
                    counter_dict[chain_pair] += [pair]
                except KeyError:
                    counter_dict[chain_pair] = [pair] 
        
        for chain_pair in counter_dict.keys():    
            if len(counter_dict[chain_pair]) > max_chain_pair:
                sample = random.sample(counter_dict[chain_pair],max_chain_pair)
                counter_dict[chain_pair] = sample
            
            for pair in counter_dict[chain_pair]:
                between_chain_pairs_list.append([*pair])
        
        dict_of_pairs = {} # dictionary of pairs. Keys: market name, values: pairs of that market
        for market in market_dictionary.keys():
            
            pairs_of_a_market =  [x for x in between_chain_pairs_list if market in x[0] or market in x[1]] # This might be a problem, I double-count
            dict_of_pairs[market] = pairs_of_a_market
    
        for category in self.category_list:
            category_df = pd.DataFrame()
            category_start = time.time()    
            for market in dict_of_pairs.keys():    
                market_df = pd.DataFrame()
                for pair in dict_of_pairs[market]:
                    start = time.time()
                    
                    iterating_df_of_a_pair = pd.DataFrame()
                    for date in self.dates: 
                        directory = self.root_directory+"\\"+date # Change directory to get a different day
                        if os.path.exists(directory +"\\"+ pair[0] + "\\" + category + ".pkl") and os.path.exists(directory +"\\"+ pair[1] + "\\" + category + ".pkl"): #Check if these pair exists in this date
                            pair_prices = self.get_category_prices_marketwise(pair, directory, category)
                            if len(pair_prices) != 0:
                                
                                if len(iterating_df_of_a_pair) == 0:    
                                    iterating_df_of_a_pair = self.check_pairs_daily(pair_prices, pair)
                                    iterating_df_of_a_pair["Tot_days"] = 1 
                                else:    
                                    temp_df = self.check_pairs_daily(pair_prices, pair)
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
                                    
                            
                            else:
                                continue
                        else:
                            break 
                    if len(iterating_df_of_a_pair) != 0:
                        #iterating_df_of_a_pair = iterating_df_of_a_pair[iterating_df_of_a_pair['Tot_days'] >= len(dates)*0.8] # uncomment to filter days
                        iterating_df_of_a_pair["share_of_identical_price"] = iterating_df_of_a_pair["indicator"] / iterating_df_of_a_pair["Tot_days"]
                        iterating_df_of_a_pair.drop(labels = ["Tot_days","indicator"],axis=1,inplace=True)
            
                    else:
                        break
                    
                    if len(market_df) == 0:
                        market_df = iterating_df_of_a_pair.copy()
                        market_df["pair_count"] = 1
                    else:
                        market_df = pd.merge(market_df, iterating_df_of_a_pair,how= "outer",on="Code")
                        
                        
                        market_df["pair_count"].fillna(0,inplace=(True))
                        market_df["pair_count"] += 1
                        
                        market_df.loc[market_df['share_of_identical_price_y'].isnull(), 'pair_count'] -= 1
                        
                        check_for_nan = market_df['Names_x'].isnull()
                        market_df.loc[market_df['Names_x'].isnull(),"Names_x"] = market_df["Names_y"].loc[check_for_nan] 
                        
                        check_for_nan = market_df['Names_y'].isnull()
                        market_df.loc[market_df['Names_y'].isnull(),"Names_y"] = market_df["Names_x"].loc[check_for_nan] 
                        
                        market_df.fillna(0,inplace=True)
                        market_df["share_of_identical_price"] = market_df["share_of_identical_price_x"] + market_df["share_of_identical_price_y"]
                        
                        market_df.drop(labels = ["share_of_identical_price_x","share_of_identical_price_y","Names_y"],axis = 1,inplace=True)
                        market_df.rename(columns = {"Names_x":"Names"},inplace = True)
                            
                            
                    
                    
                    end = time.time()    
                    #print("pair evaluated at: " + str(end-start)+" seconds") # see how much time it takes to evaluate one pair
                if len(market_df) > 0:
                    
                    market_df[market] = market_df["share_of_identical_price"] / market_df["pair_count"]
                    market_df.drop(labels = ["share_of_identical_price"],axis = 1,inplace=True)
                    
                    if len(category_df) == 0:
                        category_df = market_df.copy()
                    else:
                        category_df = pd.merge(category_df,market_df,how="outer",on=["Names","Code"])
                        category_df["pair_count_x"].fillna(0,inplace=True)
                        category_df["pair_count_y"].fillna(0,inplace=True)
            
                        category_df["pair_count"] = category_df["pair_count_x"] + category_df["pair_count_y"]
                        
                        category_df.drop(labels = ["pair_count_x","pair_count_y"],axis = 1,inplace=True)
        
            try:
                # category_df = category_df[category_df['pair_count'] >= len(dict_of_pairs[market])*0.8] # uncomment to filter pair count
                
                category_df.to_csv(self.output_directory+"//"+category +"_"+"between_chain_identical_prices.csv")
                # category_df[["Names","Code","Share_of_identical_price"]].to_csv(output_directory+"//"+category +"_"+"between_chain_identical_prices.csv")
            except KeyError:
                print("no pairs in " + category) 
            
            category_end = time.time()    
            print(category+" evaluated at: " + str(category_end-category_start)+" seconds")
            