# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 16:31:54 2022

@author: HP
"""
import os
import pandas as pd
import time
from correlation_preprocess import correlation_preprocess
from correlation_process import correlation_process 
from correlation_histogram import correlation_histogram 


root_directory = "D:" #root directory contains everything
year = "2020"
month = "08"

data_directory = os.path.join(root_directory,year,month) # data for 1 month
dates = os.listdir(root_directory) # Assumed date files are directly under root directory
dir_path = os.path.dirname(os.path.realpath(__file__))

categories = pd.read_excel("categories.xlsx")
category_list = list(categories.category)

#%%
preprocess_output_directory ="D:\\Marul\\correlation_preprocess" # Data will be printed out here
if not os.path.exists(preprocess_output_directory): # create the folder if not exists already
    os.mkdir(preprocess_output_directory)

data_prep = correlation_preprocess(data_directory=data_directory, year=year, month=month,
                       output_directory = preprocess_output_directory, category_list = category_list )
data_prep.preprocess()

time.sleep(5)
#%%

result_directory = "D:\\Marul\\results" # Data will be printed out here

if not os.path.exists(result_directory): # create the folder if not exists already
    os.mkdir(result_directory)
metric = correlation_process(data_directory=data_directory,year=year, month=month,
                       input_directory = preprocess_output_directory, output_directory = result_directory,
                       category_list =  category_list )
metric.process()
#%%
histograms_directory = "D:\\Marul\\histograms"
    
if not os.path.exists(histograms_directory): # create the folder if not exists already
    os.mkdir(histograms_directory)    

histograms = correlation_histogram(data_directory=data_directory,year=year,month=month,input_directory=result_directory,
                                   output_directory=histograms_directory,category_list=category_list)
histograms.create_histograms()