import numpy as np
import os as os
import pandas as pd
import xarray as xr
import dask
from dask.distributed import Client
import intake
import glob
import time
import yaml
from collections import ChainMap

### unsure which of the imports will actually be necessary, double check later

def get_filename(path):
    """
    Grabs just the file name given a path...maybe there's a built in function for this?
    """
    path_list = path.split('/')
    no_ext_list = path_list[-1].split('.')[0:-1]
    name_only = "".join([str(item) for item in no_ext_list])
    return name_only

def make_filename(cache_path, diag_path, catalog_path):
    """
    Creates the name for the netcdf and sidecar yaml file, if it's determined that a new one needs to be made
    Note there's no extension; either .nc or .yml should be added
    """
    ### include diag_name, catalog_path, and if already exists, some int += 1
    base_filename = cache_path + "/" + get_filename(catalog_path) + "_" + get_filename(diag_path)
    res = glob.glob(base_filename + "*")
    
    if len(res)==0:
        new_filename = base_filename + "_0"
    else:
        int_list = []
        for i in res:
            int_list.append(int(i.split('_')[-1].split('.')[0]))
            ### should have error handling for case where files have wrong format
        max_int = max(int_list)
        new_filename = base_filename + "_" + str(max_int+1)
    
    return new_filename
        
def make_sidecar_entry(cache_path, diag_path, catalog_path, first_subset={}, second_subset={}, params={}, save=True, query=False):    
    metadata_dict = dict()
    
    metadata_dict["diag_path"] = diag_path
    metadata_dict["cat_path"] = catalog_path
    metadata_dict["params"] = params
    
    ### check when catalog was last updated
    cat_last_updated = time.asctime(time.localtime(os.path.getmtime(catalog_path)))
    metadata_dict["cat_last_updated"] = cat_last_updated
    
    ### check when diag was last updated
    diag_last_updated = time.asctime(time.localtime(os.path.getmtime(diag_path)))
    metadata_dict["diag_last_updated"] = diag_last_updated
    
    
    ### combining the two rounds of subsetting
    ### with this function, if any of the keys are the same, the second subset takes priority
    full_subset_dict = dict(ChainMap(second_subset, first_subset))
    metadata_dict["subset"] = full_subset_dict
        
    ### this is not very flexible, more of a placeholder for now...
    filename = make_filename(cache_path, diag_path, catalog_path)
    
    if not query:
        metadata_dict["result_path"] = filename + ".nc"
        
        ### get current time (to purge old runs if necessary later?)
        current_datetime = time.asctime(time.localtime())
        metadata_dict["time_executed"] = current_datetime
        
    if save:
        with open(filename + ".yml", 'w') as file:
            sidecar = yaml.dump(metadata_dict, file)
    else:
        sidecar = yaml.dump(metadata_dict)
        
    ### haven't decided yet if this should return the path to the result
    # result_path = None 
    # return result_path

    return sidecar

def make_all_yamls_into_df(cache_path):
    ### glob all .yml files in cache_path
    
    all_files = glob.glob(cache_path + "/*.yml")
    
    df_ls=[]

    for filename in all_files:
        with open(filename,'r') as fh:
            # the only way i could figure out to get it to not split the
            # lower dictionary levels into separate columns
            # having them all be in one makes it easier to check if the cache matches
            df = pd.json_normalize(yaml.safe_load(fh),max_level=0)
        df_ls.append(df)

    new_df = pd.concat(df_ls)
    new_df["subset"] = new_df["subset"].apply(str)
    new_df["params"] = new_df["params"].apply(str)
    
    return new_df

def gen_df_query(cache_path, diag_path, catalog_path, first_subset={}, second_subset={}, params={}):
    ### loading all available sidecar yamls into a dataframe, removing time executed metadata because
    ### the time executed is irrelevant to the computation
    
    df_all = make_all_yamls_into_df(cache_path)
    
    new_yml = make_sidecar_entry(cache_path, diag_path, catalog_path, first_subset, second_subset, params, save=False, query=True)
        
    df_query = pd.json_normalize(yaml.safe_load(new_yml), max_level=0)
    df_query["subset"] = df_query["subset"].apply(str)
    df_query["params"] = df_query["params"].apply(str)
        
    relevant_cols = df_query.columns.values.tolist()
    # relevant_cols.remove("result_path")
    # relevant_cols.remove("time_executed")
    
    print(type(relevant_cols))
        
    df = pd.merge(df_all, df_query, how='inner', on=relevant_cols)
        
    if df.empty:
        return False
    else:
        ### grabbing one of rows. should later make sure it's the most recent one
        return df
    
def overall_logic(query_dict):
    result_path = gen_df_query(**query_dict)
    
    if result_path:
        # access cache
        return result_path
    else:
        # do the requested computation
        result_path = make_sidecar_entry(**query_dict)
        return result_path
        

def clean_cache(cache_path, startdate=None):
    ### looks at created date of all the yaml files and 
    return None


    


