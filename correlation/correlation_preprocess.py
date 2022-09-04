# -*- coding: utf-8 -*-
"""
Preprocess data to calculate correlation between pairs. Almost same with pre_abs_log_prc_diff 
The difference is now we don't take the average of prices, but we keep the whole month's prices.
Note: There is a thing we need to discuss about sampling
Created on Thu May 27 07:54:44 2021

@author: HP
"""
import pandas as pd
import os
from functools import reduce
import statistics
import time
from itertools import combinations
import numpy as np
from collections import defaultdict
import random


class correlation_preprocess():
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
    
    def find_distinct_pos(self,directory):
        """
        Get only one point of sale from redundant point of sales
        Farkli yerlere satis yapan bakkallar icin sadece 1 bolgeye olan satislari tutar
        boylece her pos bir kere dataya girer
        """
        distinct_pos = {}
        check_distinct  = {}
        for subdir, dirs, files in os.walk(directory):    # Pos dictionary, keys: Categories, values: File paths(eg.: ...aypa/peynirler.pkl)
            for category in self.category_list:
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
    
    def get_pairs(self,all_pos):
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
    
    def single_branch_prices_category(self,directory,date):
        """
        reorganizes the input data (daily) & takes the log of prices
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
        data_directory = directory[:len(self.root_directory)+1] + date + directory[len(self.root_directory) + 3:]
        
        df = pd.read_pickle(data_directory)
        df.drop(labels = ["Price_old","Link","Date"],axis=1,inplace=True)
        df.dropna(subset=["Name"],inplace=True)
        
        df["Price"].replace(to_replace=",",value =".",regex = True, inplace = True)
        df["Price"].replace(to_replace=" TL",value ="",regex = True, inplace = True)
        df["Price"] = df["Price"].astype(float) #format prices as float
        df= df[df['Price'] != 0] # drop 0 prices
        df[date] = np.log(df["Price"]) # Take the log of the prices and save it in the column has the name of that day eg:08 = [1.84,2.23,22.3...] 
        
        # drop duplicates of the code column, if we don't drop duplicates, merge function goes crazy
        df.drop_duplicates("Code",inplace=True)
                                            
        return df[["Name","Code",date]]
    
   
    def preprocess(self):
   
        category_select = self.category_list  #for all categories use category_list
        # find pos from 3 different days, then merge them
        all_posF = self.find_distinct_pos(self.root_directory+"\\"+self.dates[0]) # first day 
        all_posM = self.find_distinct_pos(self.root_directory+"\\"+self.dates[ len(self.dates)//2 ]) #middle day
        all_posL = self.find_distinct_pos(self.root_directory+"\\"+self.dates[-1]) # last day
        
        all_pos_total = defaultdict(list)
        
        for category in all_posL.keys():
            all_pos_total[category] = all_posF[category] + all_posM[category] + all_posL[category] # combine all
            
            pos_dates_fixed = [ pos[:len(self.root_directory)+1] + self.dates[0] + pos[len(self.root_directory) + 3:]  for pos in all_pos_total[category]] #fix the date in first date so
                                                                                                                                            #we can find duplicates (all dates are 01 now)
            all_pos_total[category] = list(set(pos_dates_fixed)) #  drop duplicates by taking the set first then turning it into a regular list
        
        dict_of_pairs = self.get_pairs(all_pos_total) # pairs 
        
        count_pairs_within = defaultdict(list) # Allows us to count how many pairs for a particular chain
        count_pairs_between = defaultdict(list) # keys: "chain_name+"_"+category"("sariyer_sut"), values: list of pairs for that chain and category
        
        sampled_dict_of_pairs = defaultdict(list) # preprocessed data will be saved here keys:category values: list contains pairs
        
        
        # the for loop below:
            # count all the pairs in chains with respect to category using count_pairs_between and count_pairs_within
            # if count is greater than 200, take a random sample of size 200 and save to sample_dict_of_pairs
            # else leave it as it is and save it to sample_dict_of_pairs
        start_preprocessing = time.time()
        for category in category_select:
            chain_name_within = set()
            chain_name_between = set()
            for pair in dict_of_pairs[category]:
                pos_name_1 = pair[0][pair[0].find(self.dates[0]) + len(self.dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
                chain_name_1 = pos_name_1.split("-")[0] # returns cagdas
                
                pos_name_2 = pair[1][pair[1].find(self.dates[0]) + len(self.dates[0])+1: pair[1].find(category)-1] # returns cagdas-chain-xxxx
                chain_name_2 = pos_name_2.split("-")[0]
                
                if chain_name_1 == chain_name_2: # if chain_names are same, pair is within
                    count_pairs_within[chain_name_1+"_"+category] += [pair] # "sariyer_sut" : [("C:\Users\HP\Desktop\11\01\seyhanlar-market-erenkoy-7502\peynirler.pkl",C:\Users\HP\Desktop\11\01\sariyer-market-acarkent-4823\peynirler.pkl)]
                    chain_name_within.add(chain_name_1) #
                
                else: # if chain_names are different, pair is between 
                    count_pairs_between[chain_name_1+"_"+category] += [pair] # "oruc_sut" : ["C:\Users\HP\Desktop\11\01\oruc-market-icmeler-5012\peynirler.pkl",C:\Users\HP\Desktop\11\01\sariyer-market-acarkent-4823\peynirler.pkl]
                    count_pairs_between[chain_name_2+"_"+category] += [pair] # "sariyer_sut" : ["C:\Users\HP\Desktop\11\01\oruc-market-icmeler-5012\peynirler.pkl",C:\Users\HP\Desktop\11\01\sariyer-market-acarkent-4823\peynirler.pkl]
                    chain_name_between.add(chain_name_1)
                    chain_name_between.add(chain_name_2)
            
            
            # For both of the within and between
            # IF chain has more than 200 pairs in that category, sample it, then put them into sampled_dict_of_pairs for further use
            # if chain has less than 200 pairs in that category, put the pairs without change into sampled_dict_of_pairs for further use 
            
            for chain_name in chain_name_within: 
                if len(count_pairs_within[chain_name+"_"+category])>=200:  
                    sample = random.sample(count_pairs_within[chain_name+"_"+category],200)
                    sampled_dict_of_pairs[category] += sample
                else:
                    sampled_dict_of_pairs[category] += count_pairs_within[chain_name+"_"+category]
            
            for chain_name in chain_name_between:
                if len(count_pairs_between[chain_name+"_"+category])>=200: 
                    sample = random.sample(count_pairs_between[chain_name+"_"+category],200)
                    sampled_dict_of_pairs[category] += sample
                else:
                    sampled_dict_of_pairs[category] += count_pairs_between[chain_name+"_"+category]
                    
        for category in category_select: # PROBLEM IN SAMPLING 
            for pair in sampled_dict_of_pairs[category]:                 
                for pos in pair: # pos is pair[0] or pair[1]
                    # Get chain name, from .pkl path. 
        
                    pos_name = pos[pos.find(self.dates[0]) + len(self.dates[0])+1: pos.find(category)-1] # returns cagdas-chain-xxxx
                    chain_name = pos_name.split("-")[0]
                    correlation_directory = self.output_directory + "\\" + pos_name + "_" + category+ ".pkl"
        
                    if not os.path.exists(correlation_directory):
                        pos_daily = [] # pos_daily will be populated by daily dataframes for that point of sale
                        dates_in_analysis = list() # dates the pos has data in that category
            
                        try:
                            for date in self.dates:
                                daily_dataframe = self.single_branch_prices_category(pos,date)
                                if len(daily_dataframe) > 0: # AttributeError: Can only use .str accessor with string values! solves this!!!
                                    pos_daily.append(daily_dataframe)   # time-series price data from pos_1 pos of the list
                                    dates_in_analysis.append(date)
                                
                        except FileNotFoundError:
                            pass 
                    
                    
                        if len(dates_in_analysis) > 0 :  
                            pos_merge = reduce(lambda left,right: pd.merge(left,right,on='Code',how="outer",suffixes=("","_y")),pos_daily) 
                            cols = ["Name","Name_y"]
                            pos_merge["Names"] = pos_merge[cols].apply(lambda x: ','.join(x.dropna()), axis=1)  # Join all the names in one column named "Names"
                            pos_merge["Names"] = pos_merge["Names"].str.split(",").str.get(0) # there will be multiple names, take the first.
                            pos_merge.drop(cols,axis="columns",inplace=(True))
                            pos_merge.rename(columns={"Names":"Name"},inplace = True)
                        else:
                            continue
                    
                    #drop duplicate columns
                        pos_merge = pos_merge.loc[:,~pos_merge.columns.duplicated()] 
                        
                        pos_merge["Num_days_sold"] = [len(v[pd.notna(v)].tolist()) for v in pos_merge[dates_in_analysis].values] # day count of a particular product sold
                        pos_merge = pos_merge[pos_merge["Num_days_sold"] > len(self.dates)//2] # take products if product has been sold at least 50% of days
                        pos_merge.drop(labels = ["Num_days_sold"],inplace=True,axis=1)
                        # Output the preprocessed data
                        pos_merge.to_pickle(correlation_directory) 
                
        np.save(self.output_directory + "\\sampled_dict_of_pairs_for_correlation.npy",sampled_dict_of_pairs) 
        
                
        end_preprocessing = time.time()
        print("preprocessing took "+str(end_preprocessing - start_preprocessing) + " seconds")       
                
            
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        