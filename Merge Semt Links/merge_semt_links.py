# -*- coding: utf-8 -*-
"""
Created on Sun Dec 26 15:08:21 2021

@author: HP
"""

import pandas as pd

target = pd.read_pickle("new_semtler.pkl") # DELETE 
old_links = pd.read_pickle("marul_tum_links_old.pkl")
links= pd.read_pickle("marul_tum_links.pkl")

links_istanbul = links[links['City'].str.contains("istanbul")]
old_links_istanbul = old_links[old_links['City'].str.contains("istanbul")]

links_ilce_and_mahalle_kodu = links_istanbul[["Ilce-Semt","Mahalle_kodu"]]
old_links_ilce_and_mahalle_kodu = old_links_istanbul[["Ilce-Semt","Mahalle_kodu"]]

links_df = links_ilce_and_mahalle_kodu.drop_duplicates(["Mahalle_kodu"])
old_links_df = old_links_ilce_and_mahalle_kodu.drop_duplicates(["Mahalle_kodu"])

outer_join = pd.merge(links_df,old_links_df,how="outer")

outer_join["Ilce-Semt"] = outer_join["Ilce-Semt"].str.split('-').str[0]
outer_join["Ilce-Semt"] = outer_join["Ilce-Semt"].str.capitalize()

outer_join.to_pickle("new_semtler.pkl")