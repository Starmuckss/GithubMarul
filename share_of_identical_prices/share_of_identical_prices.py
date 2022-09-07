# -*- coding: utf-8 -*-
"""
Created on Wed Sep  7 09:36:47 2022

@author: HP
"""
# -*- coding: utf-8 -*-
"""
Definition:
    Pipeline for share_of_identical_prices metric
@author: HP
"""
import os
import pandas as pd
import time
from shared_identical_prices_within import shared_identical_prices_within_chain
from shared_identical_prices_between import shared_identical_prices_between_chain 
from shared_identical_prices_visuals import shared_identical_prices_visuals 
import yaml
from pathlib import Path
# BETWEEN CHAIN data yanlis formatta !!!!!

d = Path(__file__).resolve().parents[1] # Path('/home/kristina/desire-directory')
dir_path = os.path.dirname(os.path.realpath(__file__))
config = yaml.safe_load(open(os.path.join(str(d),"config.yml")))

root_directory = config['root_directory'] #root directory contains everything (all data)
metric_name = "share_of_identical_prices"

for year in ['2020']:
    for month in ["11"]:
        start = time.time()
        data_directory = os.path.join(root_directory,year,month) # data for 1 month
        dates = os.listdir(root_directory) # Assumed date files are directly under root directory
        dir_path = os.path.dirname(os.path.realpath(__file__))
        
        categories = pd.read_excel("categories.xlsx")
        category_list = list(categories.category)

        selected_categories = ['unlar','yaglar']
        
        #%%
        result_directory = os.path.join(config['result_directory'])  # Data will be printed out here

        if not os.path.exists(result_directory): # create the folder if not exists already
            os.mkdir(result_directory)
        
        result_directory = os.path.join(config['result_directory'],metric_name)  # Data will be printed out here

        if not os.path.exists(result_directory): # create the folder if not exists already
            os.mkdir(result_directory)    
            
        metric = shared_identical_prices_within_chain(data_directory=data_directory, year=year, month=month,
                               output_directory = result_directory, category_list = selected_categories ) # for category_list parameter, use ['sut'] for faster results

        metric.process()

        #%%

        result_directory = os.path.join(config['result_directory'])  # Data will be printed out here

        if not os.path.exists(result_directory): # create the folder if not exists already
            os.mkdir(result_directory)
        
        result_directory = os.path.join(config['result_directory'],metric_name)  # Data will be printed out here

        if not os.path.exists(result_directory): # create the folder if not exists already
            os.mkdir(result_directory)    
            
        metric = shared_identical_prices_between_chain(data_directory=data_directory, year=year, month=month,
                               output_directory = result_directory, category_list = selected_categories ) # for category_list parameter, use ['sut'] for faster results

        metric.process()
        #%%
        visuals_directory = os.path.join(config['visuals_directory'])

        if not os.path.exists(visuals_directory): # create the folder if not exists already
            os.mkdir(visuals_directory)
        
        visuals_directory = os.path.join(config['visuals_directory'],metric_name)

        if not os.path.exists(visuals_directory): # create the folder if not exists already
            os.mkdir(visuals_directory)

        histograms = shared_identical_prices_visuals(data_directory=data_directory,year=year,month=month,input_directory=result_directory,
                                           output_directory=visuals_directory,category_list=selected_categories)
        histograms.create_visuals()
        
        end = time.time()
        print(year,month,"completed in",str(end-start))