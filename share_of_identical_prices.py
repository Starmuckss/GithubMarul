# -*- coding: utf-8 -*-
"""
Created on Sun Feb 21 15:18:10 2021

@author: HP
"""
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 17 09:18:35 2021

@author: HP
"""
# -*- coding: utf-8 -*-
"""
Created on Wed Feb  3 11:26:10 2021

@author: HP
"""
# -*- coding: utf-8 -*-
import pandas as pd
import os
import numpy as np
from functools import reduce
import statistics
import time
from itertools import combinations

root_directory = "C:\\Users\\HP\\Desktop\\10" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
output_directory = "C:\\Users\\HP\\Desktop\\anadolu10_results" # Data will be printed out here, You have to create this directory before using it.
categories = pd.read_excel("categories.xlsx")
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

def get_statistics(market_category_prices):
    sube_names = market_dictionary[market]
    market_category_prices['Unique_prices'] = [len(set(v[pd.notna(v)].tolist())) for v in market_category_prices[sube_names].values] # Unique price count is a column now
    market_category_prices["Number_of_vendors"] = [len(v[pd.notna(v)].tolist()) for v in market_category_prices[sube_names].values] # Total number of subeler who sells the product
    market_category_prices["Max_price"] = [max(v[pd.notna(v)].tolist()) for v in market_category_prices[sube_names].values]  # Max_price as a column
    market_category_prices["Min_price"] = [min(v[pd.notna(v)].tolist()) for v in market_category_prices[sube_names].values] # Min_price as a column
    market_category_prices["Median_price"] = [statistics.median(v[pd.notna(v)].tolist()) for v in market_category_prices[sube_names].values] # Median_price as a column
    market_category_prices["Mean_price"] = [statistics.mean(v[pd.notna(v)].tolist()) for v in market_category_prices[sube_names].values] # Mean_price as a column
    market_category_prices["Variance"] = [statistics.pvariance(v[pd.notna(v)].tolist()) for v in market_category_prices[sube_names].values]
    
    
    #Prices of all products a list of lists
    all_prices_in_category =  [(v[pd.notna(v)].tolist()) for v in market_category_prices[sube_names].values]
    
    all_prices_log = [np.log(x) for x in all_prices_in_category] # Bir problem var, nan valueler var sanırım,bunu düzelt


    market_category_prices["dominant_price"] = [statistics.mode(x) for x in all_prices_in_category] # multimod'a da bak!
    vendors_using_dominant_price = [x.count(statistics.mode(x)) for x in all_prices_in_category]
    vendors_not_using_dominant_price = market_category_prices["Number_of_vendors"] - vendors_using_dominant_price
    market_category_prices["vendors_not_using_dominant_price"] = vendors_not_using_dominant_price        
    market_category_prices["unique_p_ratio"] = market_category_prices.Unique_prices / market_category_prices.Number_of_vendors
    market_category_prices["dispersion_measure"] = (market_category_prices.Max_price-market_category_prices.Min_price) / market_category_prices.Mean_price

    pstdev = [statistics.pstdev(x) for x in all_prices_in_category] # pop stdev for each product        
    market_category_prices["Stdev/Average"]= pstdev / market_category_prices.Mean_price
    
    log_variance = [statistics.pvariance(x) for x in all_prices_log] 
    market_category_prices["log10_Variance"] = log_variance


    report = market_category_prices.drop(sube_names,axis =1)        
    path = os.path.join(output_directory,market)
    try:  # report
    
        os.mkdir(path)
    
        if len(report) !=0: 
            report.to_pickle(path + "\\" + market + "_" + category + "_" + date +".pkl")      
    except:
        if len(report) !=0:     
            report.to_pickle(path + "\\" + market + "_" + category + "_" + date +".pkl")  
  



market_dictionary = markets(root_directory+"\\"+dates[2]) # burası problemli, pairler günlere göre değişiyor   

category_select = category_list 
category_select = ["kitap"]
for category in category_select:
    category_start = time.time()    
    final_rep = [] #list of final report, will be filled with market dataframes
    markets_in_analysis = []  
    for market in market_dictionary.keys():
        
        sube_names = market_dictionary[market]
        market_pairs_final_list = []
        
        market_shared_prices = {}
        list_of_pairs = []
        pairs = combinations(sube_names, 2)
        
        for pair in pairs:  # pairs consists of tuples, make each pair a list.
            list_of_pairs.append([*pair])
            
        for pair in list_of_pairs:
            start = time.time()
            
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
                            
                    
                    else:
                        break
                else:
                    break 
            try:
                iterating_df_of_a_pair["share_of_identical_price"] = iterating_df_of_a_pair["indicator"] / iterating_df_of_a_pair["Tot_days"]
                iterating_df_of_a_pair.drop(labels = ["Tot_days","indicator"],axis=1,inplace=True)
                market_pairs_final_list.append(iterating_df_of_a_pair)

            except:
                print("x")
            
            end = time.time()    
            print("pair evaluated at: " + str(end-start)+" seconds") # see how much time it takes to evaluate one pair
            
        if len(market_pairs_final_list) > 1:            
            market_outer = reduce(lambda left,right: pd.merge(left,right,on='Code',how="outer"),market_pairs_final_list)
            markets_in_analysis.append(market)
            try:
                cols = ["Names","Names_x","Names_y"]
                market_outer["Name"] = market_outer[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
                market_outer["Name"] = market_outer["Name"].str.split(",").str.get(0) # there will be multiple names, take the first.
                market_outer.drop(cols,axis="columns",inplace=(True))
            except KeyError:
                cols = ["Names_x","Names_y"]
                market_outer["Name"] = market_outer[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
                market_outer["Name"] = market_outer["Name"].str.split(",").str.get(0) # there will be multiple names, take the first.
                market_outer.drop(cols,axis="columns",inplace=(True))
            
            pair_columns = market_outer.columns[1:-2] # bunu düzelt, problem çıkarabilir
            market_statistics = [(v[pd.notna(v)].tolist()) for v in market_outer[pair_columns].values] 
            market_outer["identical_prices"] = [sum(v) for v in market_statistics]
            market_outer["number_of_pairs_sold"] = [len(v) for v in market_statistics]
    
            market_outer[market] = market_outer["identical_prices"] /  market_outer["number_of_pairs_sold"]
            
            final_rep.append(market_outer[["Name","Code",market]])
        elif len(market_pairs_final_list) == 1:
            markets_in_analysis.append(market)
            market_outer = market_pairs_final_list[0]
            market_outer.columns = ["Name","Code",market]
            final_rep.append(market_outer)
            
        else:
            print("No pairs in "+ market)

    if len(final_rep) != 0:    
        try:
            final_outer_merge = reduce(lambda left,right: pd.merge(left,right,on='Code',how="outer"),final_rep)
            try:
                cols = ["Name","Name_x","Name_y"]
                final_outer_merge["Name"] = final_outer_merge[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
                final_outer_merge["Name"] = final_outer_merge["Name"].str.split(",").str.get(0) # there will be multiple names, take the first.
                final_outer_merge.drop(cols,axis="columns",inplace=(True))
            except KeyError:
                cols = ["Name_x","Name_y"]
                final_outer_merge["Names"] = final_outer_merge[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
                final_outer_merge["Names"] = final_outer_merge["Names"].str.split(",").str.get(0) # there will be multiple names, take the first.
                final_outer_merge.drop(cols,axis="columns",inplace=(True))
            
            column_names = ["Names","Code"] + list(set(markets_in_analysis))
            final_outer_merge = final_outer_merge.reindex(columns=column_names)

            final_outer_merge.to_excel(category +"_"+"identical_prices.xlsx")
        except TypeError as e:
            print(e)
    else:
        print("Empty Category")
        continue
    category_end = time.time()    
    print("category evaluated at: " + str(category_end-category_start)+" seconds")


    