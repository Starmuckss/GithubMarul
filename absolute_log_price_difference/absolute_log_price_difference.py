# -*- coding: utf-8 -*-
"""
Definition:
    Pipeline for absolute_log_price_difference metric
@author: HP
"""
import os
import pandas as pd
import time
from pre_abs_log_prc_diff import absolute_log_price_difference_preprocess
from abs_log_prc_diff import absolute_log_price_difference_process 
from abs_log_prc_diff_histogram import absolute_log_price_difference_visuals 
import yaml
from pathlib import Path

d = Path(__file__).resolve().parents[1] # Path('/home/kristina/desire-directory')
dir_path = os.path.dirname(os.path.realpath(__file__))
config = yaml.safe_load(open(os.path.join(str(d),"config.yml")))

root_directory = config['root_directory'] #root directory contains everything (all data)
metric_name = "absolute_log_price_difference"

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
        preprocess_output_directory = os.path.join(config['preprocess_data_directory']) 
        
        if not os.path.exists(preprocess_output_directory): # create the folder if not exists already
            os.mkdir(preprocess_output_directory)
        
        preprocess_output_directory = os.path.join(config['preprocess_data_directory'],metric_name)
        
        if not os.path.exists(preprocess_output_directory): # create the folder if not exists already
            os.mkdir(preprocess_output_directory)

        
        data_prep = absolute_log_price_difference_preprocess(data_directory=data_directory, year=year, month=month,
                               output_directory = preprocess_output_directory, category_list = selected_categories ) # for category_list parameter, use ['sut'] for faster results

        data_prep.preprocess()

        time.sleep(5)
        #%%

        result_directory = os.path.join(config['result_directory'])  # Data will be printed out here

        if not os.path.exists(result_directory): # create the folder if not exists already
            os.mkdir(result_directory)
        
        result_directory = os.path.join(config['result_directory'],metric_name)  # Data will be printed out here

        if not os.path.exists(result_directory): # create the folder if not exists already
            os.mkdir(result_directory)    
            
        metric = absolute_log_price_difference_process(data_directory=data_directory,year=year, month=month,
                               input_directory = preprocess_output_directory, output_directory = result_directory,
                               category_list =  selected_categories )
        metric.process()
        #%%
        visuals_directory = os.path.join(config['visuals_directory'])

        if not os.path.exists(visuals_directory): # create the folder if not exists already
            os.mkdir(visuals_directory)
        
        visuals_directory = os.path.join(config['visuals_directory'],metric_name)

        if not os.path.exists(visuals_directory): # create the folder if not exists already
            os.mkdir(visuals_directory)

        histograms = absolute_log_price_difference_visuals(data_directory=data_directory,year=year,month=month,input_directory=result_directory,
                                           output_directory=visuals_directory,category_list=selected_categories)
        histograms.create_visuals()
        
        end = time.time()
        print(year,month,"completed in",str(end-start))