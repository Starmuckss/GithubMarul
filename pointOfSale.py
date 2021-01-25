# -*- coding: utf-8 -*-
"""
Created on Sun Jan 24 15:01:27 2021

@author: HP
"""
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  8 11:52:23 2020

@author: HP
"""
import pandas as pd
import os
from functools import reduce
import statistics
import time
import matplotlib.pyplot as plt
import numpy as np



root_directory = "C:\\Users\\HP\\Desktop\\anadolu10" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
output_directory = "C:\\Users\\HP\\Desktop\\anadolu10_results" # Data will be printed out here, You have to create this directory before using it.
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)


def find_distinct_pos(directory):
    """
    Get only one point of sale from redundant point of sales
    Farkli yerlere satis yapan bakkallari eler
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
    return distinct_pos 

def get_category_prices(all_pos_list):
    """
    takes in a list as a parameter, that list contains "category".pkl
    data from all the points of sales.
    The parameter list is the value of a dictionary key, with the key 
    category name.
    """
    category_dataframes = []
    pos_names = []
    failed_merge = pd.DataFrame() # Return this if category is empty 
    for point_of_sale in all_pos_list:
        str_data = point_of_sale  
        df = pd.read_pickle(str_data)
        if not (df.isnull().values.all()) and " bu category de urun yok" not in df.Price.values:
            
            df.drop(["Link","Date","Price_old"],axis=1,inplace=True)
            pos_name = point_of_sale.split("\\")[-2]
            pos_names.append(pos_name) 
            df.columns = ["Name",pos_name,"Code"]        
            df[pos_name].replace(to_replace=",",value =".",regex = True, inplace = True)
            df[pos_name].replace(to_replace=" TL",value ="",regex = True, inplace = True)
            df[pos_name] = df[pos_name].astype(float) # these three lines cast prices from str into double
            df = df[df[pos_name] != 0] # Drop 0's. Maybe there is a better way to fix this problem
            df.drop_duplicates("Code",inplace=True)
            category_dataframes.append(df)            
        del(df)
    try:
        final_category_dataframe = reduce(lambda left,right: pd.merge(left,right,on='Code',how="outer"),category_dataframes)#inner doesn't work    
    except TypeError:
        return failed_merge
        
    # Get all the names in a single column
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
        
    colums_to_show = ["Names","Code"] + pos_names
    return final_category_dataframe[colums_to_show]   

for date in dates: 
    start = time.time()

    directory = root_directory + "\\" + date # Change directory to get a different day
    pos_dictionary = find_distinct_pos(directory) # Pos dictionary, keys: Categories, values: File paths(eg.: ...aypa/peynirler.pkl)

    for category in pos_dictionary.keys():
        single_category_prices = get_category_prices(pos_dictionary[category])
        pos_names = single_category_prices.columns[2:]                                                   
        all_prices_in_category =  [(v[pd.notna(v)].tolist()) for v in single_category_prices[pos_names].values]

        single_category_prices['Unique_prices'] = [len(set(v)) for v in all_prices_in_category] # Unique price count is a column now
        single_category_prices["Number_of_vendors"] = [len(v) for v in all_prices_in_category] # Total number of subeler who sells the product
        single_category_prices["Max_price"] = [max(v) for v in all_prices_in_category]  # Max_price as a column
        single_category_prices["Min_price"] = [min(v) for v in all_prices_in_category] # Min_price as a column
        single_category_prices["Median_price"] = [statistics.median(v) for v in all_prices_in_category] # Median_price as a column
        single_category_prices["Mean_price"] = [statistics.mean(v) for v in all_prices_in_category] # Mean_price as a column
        single_category_prices["unique_p_ratio"] = single_category_prices.Unique_prices / single_category_prices.Number_of_vendors
        single_category_prices["dispersion_measure"] = (single_category_prices.Max_price-single_category_prices.Min_price) / single_category_prices.Mean_price
        single_category_prices["Variance"] = [statistics.pvariance(v) for v in all_prices_in_category]

        
        
        
        single_category_prices["dominant_price"] = [statistics.mode(x) for x in all_prices_in_category]
        vendors_using_dominant_price = [x.count(statistics.mode(x)) for x in all_prices_in_category]
        vendors_not_using_dominant_price = single_category_prices["Number_of_vendors"] - vendors_using_dominant_price
        single_category_prices["vendors_not_using_dominant_price"] = vendors_not_using_dominant_price
        
        pstdev = [statistics.pstdev(x) for x in all_prices_in_category] # pop stdev for each product        
        single_category_prices["Stdev/Average"]= pstdev / single_category_prices.Mean_price
        
        all_prices_ln = [np.log(x) for x in all_prices_in_category] 
        log_variance = [statistics.pvariance(x) for x in all_prices_ln]  
        single_category_prices["log10_Variance"] = log_variance
    
           
        report = single_category_prices.drop(pos_names,axis = 1) # a category's prices are still in memory
        
        path = os.path.join(output_directory, category)
        
        try: 
            os.mkdir(path)
            report.to_pickle(path + "\\" + category + "_" + date +".pkl")      
        except:
            report.to_pickle(path + "\\" + category + "_" + date +".pkl")  
        del(single_category_prices)
    end = time.time()    
    print("Runtime " + str(end-start)+" seconds") 