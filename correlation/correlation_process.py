# -*- coding: utf-8 -*-
"""
Created on Thu May 27 09:21:17 2021
Calculates correlation of a product's prices in two pairs. Takes in the data from correlation_preprocess.py. 
This is the final metric for QJE article.
@author: HP
"""
import pandas as pd
import os
import time
import numpy as np
from collections import defaultdict



class correlation_process():
    def __init__(self,data_directory,year,month,input_directory,output_directory,category_list):
    
        self.data_directory = data_directory
        self.year = year
        self.month = month
        
        if not os.path.exists(os.path.join(input_directory,year)): # create the folder if not exists already
            os.mkdir(os.path.join(input_directory,year)) 
        if not os.path.exists(os.path.join(input_directory,year,month)): # create the folder if not exists already
            os.mkdir(os.path.join(input_directory,year,month)) 
        
        self.input_directory = os.path.join(input_directory,year,month)   
       
        if not os.path.exists(os.path.join(output_directory,year)): # create the folder if not exists already
            os.mkdir(os.path.join(output_directory,year)) 
        if not os.path.exists(os.path.join(output_directory,year,month)): # create the folder if not exists already
            os.mkdir(os.path.join(output_directory,year,month)) 
        self.output_directory = os.path.join(output_directory,year,month)        
            
        #categories = pd.read_excel("categories.xlsx")
        #category_list = list(categories.category)
        self.category_list = category_list # hangi categoriler var bunun oldugu liste
        self.dates = os.listdir(data_directory)
    
    def process(self):
        
        category_select = self.category_list #for all categories use category_list
        sampled_dict_of_pairs = np.load(self.input_directory + "\\sampled_dict_of_pairs_for_correlation.npy",allow_pickle='TRUE').item()    
        within_results= dict()
        between_results = dict()
        dictionary_of_chain_names = defaultdict(set)
        
        # Suppose a pair: (sariyer-123 - aypa-234). this pair is a member of both sariyer's pairs and aypa's pairs. Therefore we double count them.
        # But since we don't differentiate chains when we report correlation, we can drop the pairs that occur more than once in a category.
        # This will decrease the runtime of the code.When we want to check chains seperately, we can use the regular sampled_dict_of_pairs 
        # We are not going to do this.
        # sampled_dict_of_pairs_reduced = dict() 
        # for category in sampled_dict_of_pairs:
        #     sampled_dict_of_pairs_reduced[category] = list(set(sampled_dict_of_pairs[category]))
        for category in category_select:
            start_category = time.time()
            
            within = defaultdict(lambda:pd.DataFrame())  # will have the following shape -->{"sariyer":DataFrame,"aypa":Dataframe...}
            between = defaultdict(lambda:pd.DataFrame()) # will have the following shape -->{"sariyer":DataFrame,"aypa":Dataframe...}
            
            within_results[category] = within # within_results will have this format --> {"online-meyve-siparisi":{"sariyer":DataFrame,"aypa":Dataframe...}}
                                                                                         #{"sebzeler":{"sariyer":DataFrame,"aypa":Dataframe}"       
            between_results[category] = between # same format with within_results
            
            within_chains = set()
            between_chains = set()
            
            for pair in sampled_dict_of_pairs[category]:                 
                # Take the point of sale name and chain names for both stores
                pos_name1 = pair[0][pair[0].find(self.dates[0]) + len(self.dates[0])+1: pair[0].find(category)-1] # returns cagdas-chain-xxxx
                chain_name1 = pos_name1.split("-")[0] # returns cagdas
                
                pos_name2 = pair[1][pair[1].find(self.dates[0]) + len(self.dates[0])+1: pair[1].find(category)-1]
                chain_name2 = pos_name2.split("-")[0]
                
                # since we already calculated the prices for a whole month, we read the data directly and start merging both pos in the pair
                try:    
                    pos_1 = pd.read_pickle(self.input_directory + "\\" + pos_name1+"_"+category+ ".pkl")   
                    pos_2 = pd.read_pickle(self.input_directory + "\\" + pos_name2+"_"+category+ ".pkl")
                except:
                    continue
                
                final_merge_for_pair = pd.merge(pos_1,right=pos_2,on = "Code") # inner merge on code 
                # after merging 2 pairs, there will be columns named like this: "01_x","01_y","13_x","13_y". The columns that have "_x" in them are the first store's prices
                # and "_y" are the second store' prices. I first differentiate them, then I take the correlation for each row. (rows have the products)  
                cols1 = [x for x in final_merge_for_pair.columns if "_x" in x] 
                cols2 = [x for x in final_merge_for_pair.columns if "_y" in x]
                
                df1 = final_merge_for_pair[cols1].copy()
                df1.drop(labels = ["Name_x"],axis=1,inplace=True)
                df1.columns = [x[:2] for x in df1.columns]
                
                df2 = final_merge_for_pair[cols2].copy()
                df2.drop(labels = ["Name_y"],axis=1,inplace=True)
                df2.columns = [x[:2] for x in df2.columns]
        
                correlation =  df1.corrwith(df2, axis = 1) # IF all the prices are same, the function returns NAN, probably it thinks the prices as a constant
                #correlation = np.nan_to_num(correlation, copy=True, nan=1, posinf=None, neginf=None)  # make nans into 1, if all prices are the same, corr is  (my assumption)       
                # Line above is commented. Keep the NaNs as they are
                
                result = pd.DataFrame() # a new dataframe to show results
                
                result["Name"] = final_merge_for_pair["Name_x"].copy()# take product names as a column 
                result["Code"] = final_merge_for_pair["Code"].copy() # take product codes as a column
                result[pos_name1+"_"+pos_name2] = correlation # save the correlations to result to column has the name of the pair for ex: "seyhanlar-market-erenkoy-7502_seyhanlar-market-sultanbeyli-4831"
                
                # Save the correlation calculation of one pair to within_result or between_result, same things I did before, but now I differentiate the chains in another dictionary
                if chain_name1 == chain_name2: # If the chains are the same, the pair is a within pair
                    if len(within_results[category][chain_name1]) == 0: #
                        within_results[category][chain_name1] = result
                        within_chains.add(chain_name1)
                        dictionary_of_chain_names["within"].add(chain_name1)
                    else:
                        within_results[category][chain_name1] = pd.merge(within_results[category][chain_name1],right=result, on=["Name","Code"],how = "outer")
                else:
                    if len(between_results[category][chain_name1]) == 0:
                        between_results[category][chain_name1] = result
                        between_chains.add(chain_name1)
                    
                    elif len(between_results[category][chain_name2]) == 0:
                        between_results[category][chain_name2] = result
                        between_chains.add(chain_name2)
                        dictionary_of_chain_names["between"].add(chain_name1)
        
                    else:
                        between_results[category][chain_name1] = pd.merge(between_results[category][chain_name1],right=result, on=["Name","Code"],how = "outer")
                        between_results[category][chain_name2] = pd.merge(between_results[category][chain_name2],right=result, on=["Name","Code"],how = "outer")
        
            # We calculated all pairs. Now we will take the mean of correlations across all pairs.
            for chain in within_chains:
                try: 
                    # within_(between)_pair_columns = the columns that has the correlation datas
                    within_pair_columns = within_results[category][chain].columns.copy()    
                    within_pair_columns = within_pair_columns.drop(["Name","Code"])
                    
                    
                    
                    # statistics.mean() function failed. So I first summed the correlations that are not NaNs. Then I find the pair_count
                    # The pair count is the count of not NaN values in correlation data. Then I calculated sum / pair_count
                    # The products that have NaN correlation for all pairs will have sum = 0 and pair count = 0. Since 0/0 is NaN, we have what we want in the end.
                    
                    within_results[category][chain]["Sum"] = [sum(v[pd.notna(v)].tolist()) for v in within_results[category][chain][within_pair_columns].values]    
                    
                    within_results[category][chain]["pair_count"] = [len(v[pd.notna(v)].tolist()) for v in within_results[category][chain][within_pair_columns].values]    
                    
                    within_results[category][chain]["Mean_correlation"]  = within_results[category][chain]["Sum"] / within_results[category][chain]["pair_count"] 
                    
                    within_results[category][chain][["Name","Code","Mean_correlation","pair_count"]].to_csv(self.output_directory+"\\"+"correlation_within_"+chain+"_"+category+".csv")
                
                except KeyError: # If the result dataframe (within_results[category] or between_results[category]) is empty raises KeyError
                    print(chain,category+ " is empty!")
            for chain in between_chains:
                
                try:
                    between_pair_columns = between_results[category][chain].columns.copy()    
                    between_pair_columns = between_pair_columns.drop(["Name","Code"])
                    between_results[category][chain]["Sum"] = [sum(v[pd.notna(v)].tolist()) for v in between_results[category][chain][between_pair_columns].values]    
                        
                    between_results[category][chain]["pair_count"] = [len(v[pd.notna(v)].tolist()) for v in between_results[category][chain][between_pair_columns].values]    
                    
                    between_results[category][chain]["Mean_correlation"]  = between_results[category][chain]["Sum"] / between_results[category][chain]["pair_count"] 
                    
                    between_results[category][chain][["Name","Code","Mean_correlation","pair_count"]].to_csv(self.output_directory+"\\"+"correlation_between_"+chain+"_"+category+".csv")
                except KeyError: # If the result dataframe (within_results[category] or between_results[category]) is empty raises KeyError
                    print(chain, category+ " is empty!")
            
        
            end_category = time.time()
            print(category + " took "+str(end_category-start_category) + " seconds")    
            
                