#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 18 15:51:19 2022

@author: aina
"""

#initial package installs for Malieka
#pip install pyopenssl
#pip install requests
#pip install pandas 


#allows us to make API calls
import requests
#allows us to manipulate data as a dataframe
import pandas as pd


#define list of all the years that we want data 
#years = ['2000', '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', '2011','2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021', '2022']

years2 = ['2000', '2001']

#define function to make API call, clean data, edit fapri nomenclature and merge with psd gr_data
def call_clean_merge_grdata(specific_year):
    
    #define function to create dataframe of the fapri commodities we're interested in 
    def pull_commodities():
        #pull list of all commodity codes 
        commodities = requests.get("https://apps.fas.usda.gov/PSDOnlineDataServices/api/LookupData/GetCommodities","https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/docs/v1").json()
        
        #create df to store all commodity codes
        all_commodities = pd.DataFrame(columns=["CommodityCode","CommodityName"])
        
        #for each commodity code in commodities, append it to the dataframe, "commodities" 
        for record in commodities:
            CommodityCode2 = record['CommodityCode']
            CommodityName2 = record['CommodityName']
            all_commodities = all_commodities.append({'CommodityCode':CommodityCode2,'CommodityName':CommodityName2},
                                   ignore_index=True)
        
        #identify commodities that I need
        commodities = ['Barley                        ','Corn                          ',
                       'Cotton                        ', 'Sorghum                       ',
                       'Wheat                         ','Oil, Palm                     ',
                       'Oilseed, Rapeseed             ',
                       'Oilseed, Soybean              ',
                       'Oilseed, Sunflowerseed        ',
                       'Meal, Rapeseed                ',
                       'Meal, Soybean                 ', 
                       'Meal, Sunflowerseed           ',
                       'Oil, Rapeseed                 ',
                       'Oil, Soybean                  ',
                       'Oil, Sunflowerseed            ']
        
        #only keep commodities that are explicitly modeled
        fapri_commodities = all_commodities.loc[all_commodities.CommodityName.isin(commodities)]
        
        del all_commodities, commodities, CommodityCode2, CommodityName2, record
        
        return fapri_commodities
    
    #run function to pull commodity codes 
    fapri_commodities = pull_commodities()

    #define function to pull a list of commodities for numerous years for explicitly modeled countries and world
    def pullData2(year, country_code_list):
    
        #pull data for individual countries
        countries_df = pd.DataFrame(columns=["CommodityCode","CountryCode","AttributeID","UnitID","Value"])
    
        #for each commodity code in the list, perform the following functions:
        for commodity_code in fapri_commodities.CommodityCode:
                for country_code in country_code_list:
                
                    #construct API URL by pasting together several parameters
                    url = 'https://apps.fas.usda.gov/OpenData/api/psd/commodity/'+commodity_code+'/country/'+country_code+'/year/'+year
            
                    #make API call for commodity data
                    response = requests.get(url,'https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/docs/v1').json()
                    
                    #For each of the records, I want to extract the elements that I need. 
                    for record in response:
                            CommodityCode = record['commodityCode']
                            CountryCode = record['countryCode']
                            AttributeID = record['attributeId']
                            UnitID = record['unitId']
                            Value = record['value']
                            
                            #save each record in newdf
                            countries_df = countries_df.append({'CommodityCode':CommodityCode,'CountryCode':CountryCode,'AttributeID':AttributeID,
                                                  'UnitID':UnitID,'Value':Value}, 
                                                 ignore_index=True)
        
        #pull data for world
        world_df = pd.DataFrame(columns=["CommodityCode","CountryCode","AttributeID","UnitID","Value"])
        
        #for each commodity code in the list, perform the following functions:
        for commodity_code in fapri_commodities.CommodityCode:
                
                #construct API URL by pasting together several parameters
                url = 'https://apps.fas.usda.gov/OpenData/api/psd/commodity/'+commodity_code+'/world/year/'+year
        
                #make API call for commodity data
                response = requests.get(url,'https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/docs/v1').json()
                
                
                #For each of the records, I want to extract the elements that I need. 
                for record in response:
                    CommodityCode = record['commodityCode']
                    CountryCode = record['countryCode']
                    AttributeID = record['attributeId']
                    UnitID = record['unitId']
                    Value = record['value']
                            
                    #save each record in newdf
                    world_df = world_df.append({'CommodityCode':CommodityCode,'CountryCode':CountryCode,'AttributeID':AttributeID,
                                        'UnitID':UnitID,'Value':Value}, ignore_index=True)
        
        #rename country code in world_df, this will be helpful in merging later on 
        world_df['CountryCode'] = "WORLD"
                    
        all_df =  countries_df.append(world_df)
        
        del AttributeID, commodity_code, CommodityCode, countries_df, country_code, CountryCode, record, response, UnitID, url, Value, world_df, year

        return all_df
    
    #we are only interested in 12 countries + world total
    countries = ['AR','AS','BR','CA','CH','E4','IN','ID','MY','MX','RS','UP','US','KZ']
    
    #test the function for a specific year 
    psd2 = pullData2(year, countries)
    
    #define function to split psd into gr_data and os_data
    def clean_psd_filter_gr_os(psd2):
        
        #strip white space from column with commodity names in order to merge in next step 
        fapri_commodities.CommodityName = fapri_commodities.CommodityName.str.strip()
        
        #merge psd with descriptions for commodities
        psd2 = psd2.merge(fapri_commodities, how = 'left')
        
        #create dataframe for matching country codes and names 
        country_codes = pd.DataFrame({'CountryCode':['AR','AS','BR','CA','CH','E4','IN','ID','MY','MX','RS','UP','US','KZ','WORLD'], 
                                      'CountryName':['Argentina','Australia','Brazil','Canada','China','EuropeanUnion','India',
                                                     'Indonesia','Malaysia','Mexico','Russia','Ukraine','UnitedStates', 'Kazakhstan','WORLD']})
        
        #merge psd with country names 
        psd2 = psd2.merge(country_codes, how = 'left')
        
        #make API call for attribute ID data
        atts_IDs_api = requests.get('https://apps.fas.usda.gov/OpenData/api/psd/commodityAttributes','https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/docs/v1').json()
        
        att_IDs = pd.DataFrame(columns=["AttributeID", "AttributeDescription"])
        
        #For each of the records, I want to extract the elements that I need. 
        for record in atts_IDs_api:
            AttributeDescription = record['attributeName']
            AttributeID = record['attributeId']
            
            #save each record in newdf
            att_IDs = att_IDs.append({'AttributeID':AttributeID,'AttributeDescription':AttributeDescription},ignore_index=True)
              
        #merge psd with attribute descriptions
        psd2 = psd2.merge(att_IDs, how = 'left')
        
        #make API call for unit ID data
        unit_IDs_api = requests.get('https://apps.fas.usda.gov/OpenData/api/psd/unitsOfMeasure','https://apps.fas.usda.gov/PSDOnlineDataServices/swagger/docs/v1').json()
        
        unit_IDs = pd.DataFrame(columns=["UnitID", "UnitDescription"])
        
        #For each of the records, I want to extract the elements that I need. 
        for record in unit_IDs_api:
            UnitID = record['unitId']
            UnitDescription = record['unitDescription']
            
            #save each record in newdf
            unit_IDs = unit_IDs.append({'UnitID':UnitID,'UnitDescription':UnitDescription},ignore_index=True)
              
        #merge psd with attribute descriptions
        psd2 = psd2.merge(unit_IDs, how = 'left')
        
        #drop unnecessary columns
        psd2 = psd2[['CommodityName','AttributeDescription','UnitDescription','CountryName','Value']]
        
        #combine all columns into one, we will need this to match with fapri data later 
        psd2['Description'] = psd2[['CommodityName','AttributeDescription','UnitDescription','CountryName']].agg(' '.join, axis=1)
    
        #strip all spaces and commas from "combined" column, this will make it easier to merge with fapri data later
        psd2['Description'] = psd2['Description'].str.replace(' ', '')
        psd2['Description'] = psd2['Description'].str.replace(',', '')

        #separate grains from psd and save as "gr_data2" dataframe
        gr_data2 = psd2.loc[(psd2.CommodityName == "Barley") | (psd2.CommodityName == "Corn") | (psd2.CommodityName == "Sorghum")  
                        | (psd2.CommodityName == "Cotton") | (psd2.CommodityName == "Wheat") ]
        
        #separate grains from psd and save as "os_data2" dataframe
        os_data2 = psd2.merge(gr_data2, indicator = True, how='left')
        
        os_data2 = os_data2[os_data2._merge == 'left_only']
        
        gr_data2 = gr_data2[gr_data2.AttributeDescription != "Yield"]
        
        #clean up variable explorer
        del att_IDs, AttributeDescription, AttributeID, atts_IDs_api, record, country_codes
        
        return gr_data2, os_data2
    
    gr_data, os_data = clean_psd_filter_gr_os(psd2)
    
    #define function to upload & clean fapri data, we'll use this later on for merging with psd 
    def upload_fapri_data():
        
        #create list called "fapri" and import grdata and osdata from ICM spreadsheet
        fapri = []
        fapri.append(pd.read_excel("/Users/aina/Documents/RAProjects/FAPRI/icm aug22.xlsx", "GRData"))
        fapri.append(pd.read_excel("/Users/aina/Documents/RAProjects/FAPRI/icm aug22.xlsx", "OSData"))
        
        #delete unnecessary columns from both dfs
        for item in range(len(fapri)): 
            fapri[item] = fapri[item][['Variable','Description']]
            fapri[item].Description = fapri[item].Description.str.replace(' ', '')
            fapri[item].Description = fapri[item].Description.str.replace(',', '')
               
        return fapri
    
    fapri = upload_fapri_data()
        
    #define function to edit fapri nomenclature and merge with psd gr_data
    def fapri_renaming_merge_grdata():
        
        fapri[0].Description = fapri[0].Description.str.replace('MYImports','Imports')
        fapri[0].Description = fapri[0].Description.str.replace('MYExports','Exports')
        fapri[0].Description = fapri[0].Description.str.replace('MYExports','Exports')
        fapri[0].Description = fapri[0].Description.str.replace('World', 'WORLD')
        fapri[0].Description = fapri[0].Description.str.replace('BarleyFoodseed&industrial', 'BarleyFSIConsumption(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('Cornexports1000mt', 'CornExports(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('Cornfoodseed&industrialuse1000mt', 'CornFSIConsumption(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('Cornendingstocks1000mt', 'CornEndingStocks(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('CottonUse1000480lb.Bales', 'CottonUseDom.Consumption1000480lb.Bales')
        fapri[0].Description = fapri[0].Description.str.replace('CottonLoss1000480lb.Bales', 'CottonLossDom.Consumption1000480lb.Bales')
        fapri[0].Description = fapri[0].Description.str.replace('EU-27', 'EuropeanUnion')
        fapri[0].Description = fapri[0].Description.str.replace('Wheatareaharvested1000ha', 'WheatAreaHarvested(1000HA)')
        fapri[0].Description = fapri[0].Description.str.replace('Wheatproduction1000mt', 'WheatProduction(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('Wheatimports1000mt', 'WheatImports(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('Wheatexports1000mt', 'WheatExports(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('Wheatfoodseed&industrialuse1000mt', 'WheatFSIConsumption(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('WheatTotalDomesticUse1000mtChina', 'WheatDomesticConsumption(1000MT)China')
        fapri[0].Description = fapri[0].Description.str.replace('Wheatendingstocks1000mt', 'WheatEndingStocks(1000MT)')
        fapri[0].Description = fapri[0].Description.str.replace('feed&residualuse1000mt', 'FeedDom.Consumption(1000MT)', regex=False)
        fapri[0].Description = fapri[0].Description.str.replace('FeedandResidual(1000MT)', 'FeedDom.Consumption(1000MT)', regex=False)
        fapri[0].Description = fapri[0].Description.str.replace('FeedandResidual(1000MT)', 'FeedDom.Consumption(1000MT)', regex=False)
        
        #merge PSD data with fapri data variable names 
        newdf = gr_data.merge(fapri[0], how = 'right', on = ['Description'])
        
        #rearrange columns
        newdf = newdf[['Variable','Description','Value']]
                
        current_year = str(specific_year)
        
        newdf = newdf.rename(columns={'Value':current_year})
        
        newdf = newdf[newdf['Description'].notna()]
        
        newdf = newdf[newdf['Variable'] != "COIMPCHUS"]
        newdf = newdf[newdf['Variable'] != "WHIMPCHUS"]
        newdf = newdf[newdf['Variable'] != "CTIMPCHUS"]

        return newdf

    merged_gr_data = fapri_renaming_merge_grdata()
    
    #define function to edit fapri nomenclature and merge with psd gr_data
    def fapri_renaming_merge_osdata():
        
        #rename fapri data to match PSD data to merge later on 
        fapri[1].Description = fapri[1].Description.str.replace('PalmOil', 'OilPalm')
        fapri[1].Description = fapri[1].Description.str.replace('EU', 'EuropeanUnion')
        fapri[1].Description = fapri[1].Description.str.replace('US', 'UnitedStates')
        fapri[1].Description = fapri[1].Description.str.replace('EuropeanUnion-27', 'EuropeanUnion')
        fapri[1].Description = fapri[1].Description.str.replace('EuropeanUnion-28', 'EuropeanUnion')
        fapri[1].Description = fapri[1].Description.str.replace('World', 'WORLD')
        fapri[1].Description = fapri[1].Description.str.replace('AreaHarvestedChina(1000HA)', 'AreaHarvested(1000HA)China', regex=False)
        fapri[1].Description = fapri[1].Description.str.replace('AreaHarvestedEuropeanUnion(1000HA)', 'AreaHarvested(1000HA)EuropeanUnion', regex=False)
        fapri[1].Description = fapri[1].Description.str.replace('AreaHarvestedIndia(1000HA)', 'AreaHarvested(1000HA)India', regex=False)
        fapri[1].Description = fapri[1].Description.str.replace('AreaHarvestedIndonesia(1000HA)', 'AreaHarvested(1000HA)Indonesia', regex=False)
        fapri[1].Description = fapri[1].Description.str.replace('AreaHarvestedMalaysia(1000HA)', 'AreaHarvested(1000HA)Malaysia', regex=False)
        fapri[1].Description = fapri[1].Description.str.replace('AreaHarvestedUnitedStates(1000HA)', 'AreaHarvested(1000HA)UnitedStates', regex=False)
        fapri[1].Description = fapri[1].Description.str.replace('AreaHarvestedWORLD(1000HA)', 'AreaHarvested(1000HA)WORLD', regex=False)
        fapri[1].Description = fapri[1].Description.str.replace('MYImports', 'Imports')
        fapri[1].Description = fapri[1].Description.str.replace('MYExports', 'Exports')
        fapri[1].Description = fapri[1].Description.str.replace('Food&OtherUse', 'FoodUseDom.Cons.')
        fapri[1].Description = fapri[1].Description.str.replace('FeedandWaste', 'FeedWasteDom.Cons.')
        fapri[1].Description = fapri[1].Description.str.replace('TotalDom.Cons.', 'DomesticConsumption')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedAreaHarvested', 'OilseedRapeseedAreaHarvested')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedProduction', 'OilseedRapeseedProduction')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedImports', 'OilseedRapeseedImports')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedExports', 'OilseedRapeseedExports')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedCrush', 'OilseedRapeseedCrush')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedFoodUseDom.Cons.', 'OilseedRapeseedFoodUseDom.Cons.')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedFeedWasteDom.Cons.', 'OilseedRapeseedFeedWasteDom.Cons.')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedDomesticConsumption', 'OilseedRapeseedDomesticConsumption')
        fapri[1].Description = fapri[1].Description.str.replace('RapeseedEndingStocks', 'OilseedRapeseedEndingStocks')
        fapri[1].Description = fapri[1].Description.str.replace('Rapeseedmeal', 'MealRapeseed')
        fapri[1].Description = fapri[1].Description.str.replace('Rapemeal', 'MealRapeseed')
        fapri[1].Description = fapri[1].Description.str.replace('Rapeoil', 'OilRapeseed') 
        fapri[1].Description = fapri[1].Description.str.replace('OtherUse', 'FeedWasteDom.Cons.') 
        fapri[1].Description = fapri[1].Description.str.replace('Soybeans', 'OilseedSoybean')
        fapri[1].Description = fapri[1].Description.str.replace('Soybeanmeal', 'MealSoybean')
        fapri[1].Description = fapri[1].Description.str.replace('Soymeal', 'MealSoybean')
        fapri[1].Description = fapri[1].Description.str.replace('SoybeanOil', 'OilSoybean')
        fapri[1].Description = fapri[1].Description.str.replace('SoyOil', 'OilSoybean')
        fapri[1].Description = fapri[1].Description.str.replace('Soyoil', 'OilSoybean')
        fapri[1].Description = fapri[1].Description.str.replace('Food&OtherDom.Cons.', 'FoodUseDom.Cons.')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedAreaHarvested', 'OilseedSunflowerseedAreaHarvested')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedProduction', 'OilseedSunflowerseedProduction')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedImports', 'OilseedSunflowerseedImports')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedExports', 'OilseedSunflowerseedExports')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedCrush', 'OilseedSunflowerseedCrush')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedFoodUseDom.Cons.', 'OilseedSunflowerseedFoodUseDom.Cons.')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedFeedWasteDom.Cons.', 'OilseedSunflowerseedFeedWasteDom.Cons.')
        fapri[1].Description = fapri[1].Description.str.replace('SunflowerseedEndingStocks', 'OilseedSunflowerseedEndingStocks')
        fapri[1].Description = fapri[1].Description.str.replace('Sunflowerseedmeal', 'MealSunflowerseed')
        fapri[1].Description = fapri[1].Description.str.replace('Sunflowerseedoil', 'OilSunflowerseed')
        
        #merge PSD data with fapri data variable names 
        newdf = os_data.merge(fapri[1], how = 'right', on = ['Description'])
        
        #rearrange columns
        newdf = newdf[['Variable','Description','Value']]
        
        current_year = str(specific_year)
        
        newdf = newdf.rename(columns={'Value':current_year})

        newdf = newdf[newdf['Description'].notna()]
        
        newdf = newdf[newdf.Variable != "VOUDTCH"]
        newdf = newdf[newdf.Variable != "VOUDTIN"]
        newdf = newdf[newdf.Variable != "UOUINROW2"]
        newdf = newdf[newdf.Variable != "UOFODKZ"]
        
        return newdf

    merged_os_data = fapri_renaming_merge_osdata()
    
    return merged_gr_data, merged_os_data

#create list to store dfs for each year
all_years = []

#for each year, run the function and append the 3 dfs for each year to the list all_years
for year in years2:
    new_year = call_clean_merge_grdata(year)
    all_years.append(new_year)

#create list to store just the final gr data for each year
merged_gr_data = []

#for each gr_data df, delete unnecessary rows and append it to merged_gr_data list
for df in range(len(all_years)):
    new_gr = all_years[df][0]
    merged_gr_data.append(new_gr)
    new_gr = []
  
#merge the first two years, 2000 and 2001
gr_data = merged_gr_data[0].merge(merged_gr_data[1], how = 'inner', on = ['Variable','Description'])

#loop through each year to merge into 1 large dataframe
for df in range(2,(len(all_years)),1):
    gr_data = gr_data.merge(merged_gr_data[df], how = 'inner', on = ['Variable','Description'])
    

#export the merged gr_data to excel
gr_data.to_excel("/Users/aina/Desktop/gr_data_Aug2022.xlsx")





#create list to store just the final os data for each year
merged_os_data = []

#for each os_data df, append it to merged_os_data list
for df in range(len(all_years)):
    new_gr = all_years[df][1]
    merged_os_data.append(new_gr)
    new_gr = []
    

#merge the first two years, 2000 and 2001
os_data = merged_os_data[0].merge(merged_os_data[1], how = 'inner', on = ['Variable','Description'])

#loop through each year to merge into 1 large dataframe
for df in range(2,(len(all_years)),1):
    os_data = os_data.merge(merged_os_data[df], how = 'inner', on = ['Variable','Description'])
    


#export the merged gr_data to excel
os_data.to_excel("/Users/aina/Desktop/os_data_Aug2022.xlsx")



