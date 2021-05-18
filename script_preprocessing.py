"""
Data Preprocessing:
To deal with the high amount of data:
- Firstly we split the data set into 18 parts as it contained a total of 182.342.304 rows - to make its preprocessing first steps in a manageable way for our computacional resources.
- Afterwards, we removed the string part of the variables ProductFamily_ID, ProductCategory_ID, ProductBrand_ID, ProductName_ID, ProductPackSKU_ID and Sale_ID - keeping only the number part and converting the variables to integer type.
- Then, we checked for missing values - it didn't contain any.
- Our dataset contained duplicated rows whose only diference between them was: one had the value for the category 'Sell-out units' and another for 'Sell-out values' - to avoid having unnecessary repeated information, we collapsed these informations creating two new variables ('Sell-out units' and 'Sell-out values'), which allowed us to reduce our datasets size to around half of its original size.
- Next, we concatened our datasets into a single one - with this process we managed to reduce the df size from ~18GB to 4.6GB
"""
import pandas as pd
import swifter
import time
import os
from pathlib import Path


for i in range(9,18):
    start_time = time.time()
    if (i==0):
        df=pd.read_csv('NOVAIMS_MAA_2020e21_BusinessCasesDataScience_MindOverData_RetailChallenge.csv', nrows=10000000)
    elif (i==17):
        df=pd.read_csv('NOVAIMS_MAA_2020e21_BusinessCasesDataScience_MindOverData_RetailChallenge.csv', nrows=12342304, skiprows = (range(1,17*10000000)))
    else:
        df=pd.read_csv('NOVAIMS_MAA_2020e21_BusinessCasesDataScience_MindOverData_RetailChallenge.csv', nrows=10000000, skiprows = (range(1,(i)*10000000)))
    
    df['ProductFamily_ID'] = df['ProductFamily_ID'].swifter.apply(lambda x: int(x[7:]))
    df['ProductCategory_ID'] = df['ProductCategory_ID'].swifter.apply(lambda x: int(x[9:]))
    df['ProductBrand_ID'] = df['ProductBrand_ID'].swifter.apply(lambda x: int(x[13:]))
    df['ProductName_ID'] = df['ProductName_ID'].swifter.apply(lambda x: int(x[12:]))
    df['ProductPackSKU_ID'] = df['ProductPackSKU_ID'].swifter.apply(lambda x: int(x[11:]))
    df['Point-of-Sale_ID'] = df['Point-of-Sale_ID'].swifter.apply(lambda x: int(x[4:]))
    
    name='df_'+str(i)+'.csv'
    if df.isna().sum().sum()!=0:
        print(name+ ' has missing values!')
        display(df.isna().sum())
    
    df.reset_index(inplace=True)
    df = df.iloc[:,:8].merge(df.pivot(index='index',columns=['Measures'], values="Value"),on='index')
    df[['Sell-out units','Sell-out values']] = df[['Sell-out units','Sell-out values']].fillna(0)
    df = df.groupby(by=['ProductFamily_ID','ProductCategory_ID','ProductBrand_ID','ProductName_ID','ProductPackSKU_ID','Point-of-Sale_ID','Date'])[['Sell-out units','Sell-out values']].sum()
    df.reset_index(inplace=True)
    df.to_csv(name)
    print(name+' done!'+ ' In '+"--- %s seconds ---" % (time.time() - start_time))
    
PROJECT_ROOT = r"C:\Users\Utilizador\OneDrive - NOVAIMS\2º Semester\Business Cases with Data Science\PROJETOS\Business_Case_5"
#Path(os.path.abspath('')).resolve().parents[0]

files = ['df_'+str(i)+'.csv' for i in range(18)]    
df_preprocessed = pd.DataFrame()

for i,file in zip(range(18), files):
    df = pd.read_csv(os.path.join(PROJECT_ROOT, 'dfs', file))
    df.drop(columns=['Unnamed: 0'], inplace=True)
    print(file + ' read!')
    df_preprocessed = pd.concat([df_preprocessed, df])
    print(file + ' updated on df_preprocessed!')
    
df_preprocessed.to_csv('df_preprocessed.csv')
