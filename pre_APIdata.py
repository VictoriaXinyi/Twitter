#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# notes:
# conda stopped automatically setting environments up as jupyter kernels. You need to manually add kernels for each environment
# ------------------ code in terminal --------------------:
#source activate myenv
#python -m ipykernel install --user --name myenv --display-name "Python (myenv)"


# In[ ]:


#conda install -c conda-forge cartopy


# In[ ]:


#conda install geopandas


# In[ ]:


#conda install pyproj


# In[1]:


import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import geopandas
import csv
import os
import pandas as pd
import json
import numpy as np
import pandas as pd
import re
import warnings
from shapely.geometry import Point
import geopandas as gpd
from geopandas import GeoDataFrame
from pyproj import CRS


# In[ ]:


#fig = plt.figure()
#ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
#ax.coastlines()
#plt.show()


# In[2]:


mydata = pd.read_csv('mydata.csv',dtype="a", encoding='utf-8')


# In[3]:


# remove geo NAN cells
print(len(mydata.index))
nan_value = float("NaN")
mydata.replace("", nan_value, inplace=True)
mydata.dropna(subset = ["geo.geo.bbox"], inplace=True)
print(len(mydata.index))


# In[4]:


mydata['geo.geo.bbox'] = mydata['geo.geo.bbox'].astype(str)
mydata['geo.geo.bbox'] =  mydata['geo.geo.bbox'].apply(lambda x: x.replace('[','').replace(']','')) 


# In[5]:


# split 'geo.geo.bbox' , keep only one pair of lon & lat
mydata[['lon','lat','lon1','lat2']] = mydata['geo.geo.bbox'].str.split(',',expand=True)


# In[6]:


mydata.lat.dtype


# In[ ]:


# errors occur : this raw dataset has one line lack of "created info", 
# causing data Indent Forward, lon&lat contents wrong
# check position of a given cell value, find error in lat:"' CA'" - row 103355 - delete


# In[ ]:


"""
def getIndexes(dfObj, value):
      
    # Empty list
    listOfPos = []
      
    # isin() method will return a dataframe with boolean values, True at the positions where element exists
    result = dfObj.isin([value])
      
    # any() method will return 
    # a boolean series
    seriesObj = result.any()
  
    # Get list of column names where element exists
    columnNames = list(seriesObj[seriesObj == True].index)
     
    # Iterate over the list of columns and extract the row index where element exists
    for col in columnNames:
        rows = list(result[col][result[col] == True].index)
  
        for row in rows:
            listOfPos.append((row, col))
              
    # This list contains a list tuples with the index of element in the dataframe
    return listOfPos
  
# Calling getIndexes() function to get the index positions of all occurrences of "value" in the dataframe
listOfPositions = getIndexes(mydata, ' CA')
print('Index positions of " CA" in Dataframe : ')
  
# Printing the position
for i in range(len(listOfPositions)):
    print( listOfPositions[i])
    
"""


# In[ ]:


# check dataset
# mydata.loc[[410755]]


# In[ ]:


#i = mydata[((mydata.lat == ' CA') &( mydata.lon == 'San Francisco'))].index 
#mydata=mydata.drop(i)
#print(len(mydata.index))


# In[ ]:


mydata[['geo.full_name']].head(10)


# In[7]:


mydata['lon'] = mydata['lon'].astype(float)


# In[8]:


mydata['lat'] = mydata['lat'].astype(float, errors = 'raise')


# In[9]:


geometry = [Point(xy) for xy in zip(mydata['lon'], mydata['lat'])]


# In[10]:


df1 = mydata.iloc[:23109,:]
df2 = mydata.iloc[23109:34953,:]
df3 = mydata.iloc[34953:49136,:]
df4 = mydata.iloc[49136:59880,:]
df5 = mydata.iloc[59880:86704,:]
df6 = mydata.iloc[86704:101974,:]
df7 = mydata.iloc[101974:118201,:]
df8 = mydata.iloc[118201:167648,:]
df9 = mydata.iloc[167648:185461,:]
df10 = mydata.iloc[185461:194728,:]
df11 = mydata.iloc[194728:234272,:]
df12 = mydata.iloc[234272:247020,:]
df13 = mydata.iloc[247020:261805,:]
df14 = mydata.iloc[261805:282176,:]
df15 = mydata.iloc[282176:296948,:]
df16 = mydata.iloc[296948:313534,:]
df17 = mydata.iloc[313534:323817,:]
df18 = mydata.iloc[323817:381165,:]
df19 = mydata.iloc[381165:404897,:]
df20 = mydata.iloc[404897:,:]


# In[ ]:


'''
frames = [dt1,dt2,dt3,dt4,dt5,dt6,dt7,dt8,dt9,dt10,dt11,dt12,dt13,dt14,dt15,dt16,dt17,dt18,dt19,dt20]
testdata = pd.concat(frames)
print(len(testdata.index))
'''


# In[11]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df1.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="YlOrRd", 
        title=f"topic1 5G", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[12]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df2.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="YlOrRd", 
        title=f"topic2 Pizza", 
        ax=ax)
# add grid
ax.grid(b=True, alpha=0.5)
plt.show()


# In[13]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df3.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="YlOrRd", 
        title=f"topic3 Covid/Corona", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[14]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df4.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="YlOrRd", 
        title=f"topic4 Bitcoin", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[15]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df5.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="YlOrRd", 
        title=f"topic5 Kobe", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[16]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df6.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic6 Biden", 
        ax=ax)
# add grid
ax.grid(b=True, alpha=0.5)
plt.show()


# In[17]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df7.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic7 Mars", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[18]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df8.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic8 Cat", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[19]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())
# 调用ax的方法画海岸线
ax.coastlines()

df9.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic9 PUBG", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[21]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df10.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic10 WallStreet", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[22]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df11.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic11 BlackLivesMatter", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[23]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df12.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic12 Iphone", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[24]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df13.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic13 Police", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[25]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df14.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic14 Soccer", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[26]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df15.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic15 Photography", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[27]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df16.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic16 Music", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[28]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df17.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic17 LGBT", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[29]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df18.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic18 TikTok", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[30]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df19.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic19 Animation", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[31]:


plt.figure(figsize=(120,100))
fig = plt.figure()
ax = fig.add_subplot(111, projection=ccrs.PlateCarree())

# 调用ax的方法画海岸线
ax.coastlines()
#ax.stock_img()
#ax.gridlines()

df20.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", #colormap="Blues", 
        title=f"topic20 Weather", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[ ]:


# --------------------------     USA plot     -----------------------  #


# In[32]:


#df1[['geo.full_name']].head(10)
df1.head(2)


# In[33]:


USdata1 = df1.loc[df1['geo.country_code'] == 'US']
index =USdata1.index
number_of_rows = len(index)
#find length of index
print(number_of_rows)


# In[34]:


USdata2 = df2.loc[df2['geo.country_code'] == 'US']
USdata3 = df3.loc[df3['geo.country_code'] == 'US']
USdata4 = df4.loc[df4['geo.country_code'] == 'US']
USdata5 = df5.loc[df5['geo.country_code'] == 'US']
USdata6 = df6.loc[df6['geo.country_code'] == 'US']
USdata7 = df7.loc[df7['geo.country_code'] == 'US']
USdata8 = df8.loc[df8['geo.country_code'] == 'US']
USdata9 = df9.loc[df9['geo.country_code'] == 'US']
USdata10 = df10.loc[df10['geo.country_code'] == 'US']
USdata11 = df11.loc[df11['geo.country_code'] == 'US']
USdata12 = df12.loc[df12['geo.country_code'] == 'US']
USdata13 = df13.loc[df13['geo.country_code'] == 'US']
USdata14 = df14.loc[df14['geo.country_code'] == 'US']
USdata15 = df15.loc[df15['geo.country_code'] == 'US']
USdata16 = df16.loc[df16['geo.country_code'] == 'US']
USdata17 = df17.loc[df17['geo.country_code'] == 'US']
USdata18 = df18.loc[df18['geo.country_code'] == 'US']
USdata19 = df19.loc[df19['geo.country_code'] == 'US']
USdata20 = df20.loc[df20['geo.country_code'] == 'US']


# In[35]:


USdata1.head(3)


# In[36]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata1.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic1 5G", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[ ]:


# USdata1['geo.full_name'].unique()


# In[ ]:


#test2=USdata1.loc[USdata1['geo.full_name'] != 'T-Mobile']


# In[ ]:


#test2.head(3)


# In[ ]:


## error: split 'geo.full_name' 
# test2[['geocity','geostate']] = test2['geo.full_name'].str.split(',',expand=True)


# In[37]:


USdata1['count'] = 1
dataByNeighbourhood = USdata1.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[ ]:


#conda install -c conda-forge geoplot


# In[38]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata2.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic2 pizza", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[39]:


USdata2['count'] = 1
dataByNeighbourhood = USdata2.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[40]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata3.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic3 Covid/Corona", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[41]:


USdata3['count'] = 1
dataByNeighbourhood = USdata3.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[42]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata4.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic4 Bitcoin", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()


# In[43]:


USdata4['count'] = 1
dataByNeighbourhood = USdata4.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[44]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata5.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic5 Kobe", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()
USdata5['count'] = 1
dataByNeighbourhood = USdata5.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[45]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata6.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic6 Biden", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()
USdata6['count'] = 1
dataByNeighbourhood = USdata6.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[46]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata7.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic7 Mars", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()
USdata7['count'] = 1
dataByNeighbourhood = USdata7.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[47]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata8.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic8 Cat", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()
USdata8['count'] = 1
dataByNeighbourhood = USdata8.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[48]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()

import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata9.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic9 PUBG", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()
USdata9['count'] = 1
dataByNeighbourhood = USdata9.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[50]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
#ax = plt.subplot(111, projection=ccrs.LambertConformal())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
#ax.add_feature(cfeature.OCEAN.with_scale('50m'))
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
states_provinces = cfeature.NaturalEarthFeature(
        category='cultural',
        name='admin_1_states_provinces_lines',
        scale='50m',
        facecolor='none')

ax.add_feature(states_provinces, edgecolor='gray')
USdata10.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic10 WallStreet", 
        ax=ax)
# add grid
#ax.grid(b=True, alpha=0.5)
plt.show()
USdata10['count'] = 1
dataByNeighbourhood = USdata10.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[51]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata11.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic11 BlackLivesMatter", 
        ax=ax)
plt.show()
USdata11['count'] = 1
dataByNeighbourhood = USdata11.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[52]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata12.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic12 Iphone", 
        ax=ax)
plt.show()
USdata12['count'] = 1
dataByNeighbourhood = USdata12.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[53]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata13.plot(x="lon", y="lat", kind="scatter", 
        c="lightblue", colormap="Blues", 
        title=f"topic13 Police", 
        ax=ax)
plt.show()
USdata13['count'] = 1
dataByNeighbourhood = USdata13.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[54]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata14.plot(x="lon", y="lat", kind="scatter", c="lightblue", colormap="Blues", 
              title=f"topic11 BlackLivesMatter", ax=ax)
plt.show()
USdata14['count'] = 1
dataByNeighbourhood = USdata14.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[55]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata15.plot(x="lon", y="lat", kind="scatter", c="lightblue", colormap="Blues", 
              title=f"topic15 Photography", ax=ax)
plt.show()
USdata15['count'] = 1
dataByNeighbourhood = USdata15.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[56]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata16.plot(x="lon", y="lat", kind="scatter", c="lightblue", colormap="Blues", 
              title=f"topic16 Music", ax=ax)
plt.show()
USdata16['count'] = 1
dataByNeighbourhood = USdata16.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[57]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata17.plot(x="lon", y="lat", kind="scatter", c="lightblue", colormap="Blues", 
              title=f"topic17 LGBT", ax=ax)
plt.show()
USdata17['count'] = 1
dataByNeighbourhood = USdata17.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[58]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata18.plot(x="lon", y="lat", kind="scatter", c="lightblue", colormap="Blues", 
              title=f"topic18 TikTok", ax=ax)
plt.show()
USdata18['count'] = 1
dataByNeighbourhood = USdata18.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[59]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata19.plot(x="lon", y="lat", kind="scatter", c="lightblue", colormap="Blues", 
              title=f"topic19 Animation", ax=ax)
plt.show()
USdata19['count'] = 1
dataByNeighbourhood = USdata19.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[60]:


fig = plt.figure(figsize=(12,10))
fig = plt.figure()
import cartopy.feature as cfeature
ax = fig.add_subplot(1,1,1, projection=ccrs.PlateCarree())
ax.set_extent([-130,-60,20,49])
ax.coastlines()
ax.add_feature(cfeature.LAND.with_scale('50m'))
ax.add_feature(cfeature.BORDERS.with_scale('50m'))
ax.add_feature(states_provinces, edgecolor='gray')
USdata20.plot(x="lon", y="lat", kind="scatter", c="lightblue", colormap="Blues", 
              title=f"topic20 Weather", ax=ax)
plt.show()
USdata20['count'] = 1
dataByNeighbourhood = USdata20.groupby('geo.full_name').count()[['count']].reset_index()
dataByNeighbourhood['geo.full_name'] = dataByNeighbourhood['geo.full_name'].str.lower()
dataByNeighbourhood.sort_values('count', ascending=False).head(10)


# In[ ]:


###  Create heatmap of user activity ? 


# In[ ]:


"""
def graph_heatmap(userId, num_of_tweets, utc_offset):
    index = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    cols = ["%.2d:00" %x for x in range(24)]
    df_activity = pd.DataFrame(daily_activity_matrix, index=index, columns=cols)
    axes = sns.heatmap(df_activity, annot=True)
    axes.set_title('Heatmap of @%s Twitter Activity \n Generated %s for last %s tweets' %(userId, datetime.date.today(), num_of_tweets), fontsize=14)
    plt.xlabel("Time (UTC offset in seconds: %s)" %utc_offset)
    plt.yticks(rotation=0)
    plt.savefig("graphs/" + str(userId) + ".png")
"""

