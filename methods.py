# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 13:13:11 2021

@author: HP
"""
import pandas as pd
import os
import numpy as np
import copy
from functools import reduce
import statistics
import time
import matplotlib 
from matplotlib import cm
import matplotlib.pyplot as plt
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

def get_product(root_directory,market_dictionary,category,code):
    product_dataframe = pd.DataFrame()
    #root_directory = "C:\\Users\\HP\\Desktop\\10"
    dates = os.listdir(root_directory)
    for date in dates:
        for market in market_dictionary.keys():
            for sube in market_dictionary[market]:
                try:
                    str_data = os.path.join(root_directory,date,sube,category) + ".pkl"
                    df = pd.read_pickle(str_data)
                    df.drop(["Price_old","Link"],inplace = True,axis="columns")
                    product_data = df[df["Code"] == code].copy()
                    product_data["Date"] = date
                    product_data["Market"] = market
                    product_data["Branch"] = sube
                    product_dataframe = product_dataframe.append(product_data)
                except FileNotFoundError:
                    continue
    product_dataframe["Price"].replace(to_replace=",",value =".",regex = True, inplace = True)
    product_dataframe["Price"].replace(to_replace=" TL",value ="",regex = True, inplace = True)
    product_dataframe["Price"] = product_dataframe["Price"].astype(float) # these three lines cast prices from str into double
    return product_dataframe    