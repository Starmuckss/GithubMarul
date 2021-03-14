# -*- coding: utf-8 -*-
"""
Created on Thu Mar  4 23:53:06 2021

@author: HP
"""
# -*- coding: utf-8 -*-

import pandas as pd
import os
import numpy as np
from functools import reduce
import statistics
import time
import matplotlib.pyplot as plt
import matplotlib

root_directory = "C:\\Users\\HP\\Desktop\\10" # root directory contains everything
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)
dir_path = os.path.dirname(os.path.realpath(__file__))
output_directory = dir_path+"\\data_for_plots" # Data will be printed out here, on the same folder where code is.

def single_branch_prices(directory,branch_name,date):
    """
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
        df[branch_name] = np.log10(df["Price"])  # log price yerine sube ismiyle report et!
        
         # drop 0 prices
        
        all_branch_prices = all_branch_prices.append(df)
        
    all_branch_prices.drop_duplicates("Code",inplace=True)
    return all_branch_prices[["Name","Code",branch_name]]
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
        and a "Date" column which is the logarithm(e)_of_price_column

    """    

    data_directory = os.path.join(directory, branch_name) + "\\" + category + ".pkl" 
    df = pd.read_pickle(data_directory)
    df.drop(labels = ["Price_old","Link","Date"],axis=1,inplace=True)
    df.dropna(subset=["Name"],inplace=True)
    
    df["Price"].replace(to_replace=",",value =".",regex = True, inplace = True)
    df["Price"].replace(to_replace=" TL",value ="",regex = True, inplace = True)
    df["Price"] = df["Price"].astype(float) #format prices as float
    df= df[df['Price'] != 0] # drop 0 prices
    df[branch_name] = df["Price"].copy()# LN of price column
    
    
    # drop duplicates of the code column, if we don't drop duplicates, merge function goes crazy
    df.drop_duplicates("Code",inplace=True)
                                        
    
    return df[["Name","Code",branch_name]]
            
def get_product(directory,market_dictionary,category,code):
    product_dataframe = pd.DataFrame()
    #root_directory = "C:\\Users\\HP\\Desktop\\10"
    dates = os.listdir(root_directory)
    for date in dates:
        for market in market_dictionary.keys():
            for sube in market_dictionary[market]:
                str_data = os.join(root_directory,date,sube) + category + ".pkl"
                df = pd.read_pickle(str_data)
                product_data = df[df["Code"] == code]
                product_data["Date"] = date
                product_data["Market"] = market
                product_data["Branch"] = sube
                product_dataframe.append(product_data)
                        
    return get_product    
    
market_dictionary = markets(root_directory+"\\"+dates[0])
all_data = pd.DataFrame()
market_select = ["oruc"]
category_select = ["bakliyat-makarna"]

for market in market_select:
    if len(market_dictionary[market]) > 1:
        if not os.path.exists(output_directory+"\\" +market):
                os.mkdir(output_directory+"\\" +market)
        for category in category_select:
            
            market_category_df = pd.DataFrame()
            sube_names = market_dictionary[market]
            market_df = pd.DataFrame()
            
            for date in dates:
                directory = root_directory+"\\"+date
                date_df = pd.DataFrame()    
    
                for branch_name in market_dictionary[market]:
                    try: 
                       a = single_branch_prices_category(directory,branch_name,date,category)
                       if len(date_df) == 0:
                           date_df = a.copy() 
                       else:
                           date_df = pd.merge(date_df,a,on = ["Name","Code"]) 
                       
                    except FileNotFoundError:
                        pass
                date_df["Date"] = date
                market_category_df = market_category_df.append(date_df)    
                
            
            
            v = market_category_df.Code.value_counts()
            s = market_category_df[market_category_df.Code.isin(v.index[v.gt(len(dates)*0.8)])]
            if not os.path.exists(output_directory+"\\" +market):
                os.mkdir(output_directory+"\\" +market)
            s.to_pickle(output_directory +"\\" +market + "\\" + category+".pkl")
       