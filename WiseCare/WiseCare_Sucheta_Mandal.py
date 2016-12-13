
# coding: utf-8

# In[58]:

# Import SQLContext and data types
import pandas as pd
import numpy as np
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# adding the PySpak modul to SparkContext
sc.addPyFile("https://raw.githubusercontent.com/seahboonsiew/pyspark-csv/master/pyspark_csv.py")
import pyspark_csv as pycsv


# In[59]:

credentials_3 = {
  'auth_url':'https://identity.open.softlayer.com',
  'project':'object_storage_0edf47b6_57f2_461f_8ed2_fa90af760c4c',
  'project_id':'1ec699724fba469592cd1a516b24d366',
  'region':'dallas',
  'user_id':'a417f2304e7f4cf0b902c967090e5a04',
  'domain_id':'158207ca5d104912b02940cd8546431a',
  'domain_name':'1141213',
  'username':'admin_4e6610446b60075c67ea85292f1998c1f4004ba5',
  'password':"""S#,73BC-tQ7P,m~)""",
  'filename':'2017_IOWA_State.csv',
  'container':'notebooks',
  'tenantId':'s097-4e3cf56f66f7ff-00c4d0451a34'
}


# In[60]:

def set_hadoop_config(credentials_3):
    prefix = "fs.swift.service." + credentials_3['name']
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", credentials_3['auth_url']+'/v2.0/tokens')
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", credentials_3['project_id'])
    hconf.set(prefix + ".username", credentials_3['user_id'])
    hconf.set(prefix + ".password", credentials_3['password'])
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", credentials_3['region'])


# def set_hadoop_config(credentials_3):
#     prefix = "fs.swift.service." + credentials_3['name']
#     hconf = sc._jsc.hadoopConfiguration()
#     hconf.set(prefix + ".auth.url", credentials_3['auth_url']+'/v2.0/tokens')
#     hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
#     hconf.set(prefix + ".tenant", credentials_3['project_id'])
#     hconf.set(prefix + ".username", credentials_3['user_id'])
#     hconf.set(prefix + ".password", credentials_3['password'])
#     hconf.setInt(prefix + ".http.port", 8080)
#     hconf.set(prefix + ".region", credentials_3['region'])

# In[61]:

credentials_3['name'] = 'data'
set_hadoop_config(credentials_3)


# In[62]:

sqlContext = SQLContext(sc) 
premiumData = sc.textFile("swift://notebooks.data/2017_IOWA_State.csv")                         
premiumDataParse = premiumData.map(lambda line : line.split(","))


# In[63]:

def skip_header(idx, iterator):
    if (idx == 0):
        next(iterator)
    return iterator

premiumData_Header = premiumData.first()

premiumData_Header_list = premiumData_Header.split(",")
premiumData_body = premiumData.mapPartitionsWithIndex(skip_header)


# In[64]:

premiumData_df = pycsv.csvToDataFrame(sqlContext,premiumData_body, sep=",", columns= premiumData_Header_list)
premiumData_df.cache()
#premiumData_df.printSchema()
premium_age_df = premiumData_df.select(col("Tobacco Premium").alias("premium"), col("Age").alias("age"))
premium_location_df = premiumData_df.select(col("Rating Area Centroid").alias("location"), col("Tobacco Premium").alias("premium"),col("Counties").alias("area"))


# In[67]:

sqlContext.registerDataFrameAsTable(premium_age_df, "premiumTable")
premium_data_age = sqlContext.table("premiumTable")
premium_data_age=[0] * 5
#average premium pay for people between 20 to 29
premium_data_age_gp1 = sqlContext.sql("SELECT premium FROM premiumTable where age between 20 and 29")
premium_data_age_gp1 =premium_data_age_gp1.select(avg("premium"))
premium_data_age[0]= premium_data_age_gp1.first()[0]
#average premium pay for people between 30 to 39
premium_data_age_gp2 = sqlContext.sql("SELECT premium FROM premiumTable where age between 30 and 39")
premium_data_age_gp2 =premium_data_age_gp2.select(avg("premium"))
premium_data_age[1]= premium_data_age_gp2.first()[0]
#average premium pay for people between 40 to 49
premium_data_age_gp3 = sqlContext.sql("SELECT premium FROM premiumTable where age between 40 and 49")
premium_data_age_gp3 =premium_data_age_gp3.select(avg("premium"))
premium_data_age[2]= premium_data_age_gp3.first()[0]
#average premium pay for people between 50 to 59
premium_data_age_gp4 = sqlContext.sql("SELECT premium FROM premiumTable where age between 50 and 59")
premium_data_age_gp4 =premium_data_age_gp4.select(avg("premium"))
premium_data_age[3]= premium_data_age_gp4.first()[0]
#average premium pay for people above 60
premium_data_age_gp5 = sqlContext.sql("SELECT premium FROM premiumTable where age > 59")
premium_data_age_gp5 =premium_data_age_gp5.select(avg("premium"))
premium_data_age[4]= premium_data_age_gp5.first()[0]


# In[68]:

sqlContext.registerDataFrameAsTable(premium_location_df, "locationTable")
premium_data_location = sqlContext.table("locationTable")
#df = sqlContext.sql("SELECT max(premium) as maxpremium FROM locationTable GROUP BY location")
df=premium_location_df.groupBy("location").agg(max("premium").alias("maxpremium"), count("area").alias("area_count"))


# In[69]:

print df.head


# In[71]:

import plotly.plotly as py
import plotly.graph_objs as go
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')

x = ['Age 20-29','Age 30-39','Age 40-49','Age 50-59','Age>60']
y = premium_data_age

data = [go.Bar(
            x= x,
            y = y,
            marker=dict(
                color='rgb(0,102,204)',
                line=dict(
                    color='rgb(8,48,107)',
                    width=0.3),
            )
            
    )]

layout = go.Layout(
    annotations=[
        dict(x=xi,y=yi,
             text=str(yi),
             xanchor='center',
             yanchor='bottom',
             showarrow=False,
        ) for xi, yi in zip(x, y)]
)

fig = go.Figure(data=data, layout=layout)
py.iplot(fig, filename='2017-avg-premium')


# In[72]:

credentials_4 = {
  'auth_url':'https://identity.open.softlayer.com',
  'project':'object_storage_0edf47b6_57f2_461f_8ed2_fa90af760c4c',
  'project_id':'1ec699724fba469592cd1a516b24d366',
  'region':'dallas',
  'user_id':'a417f2304e7f4cf0b902c967090e5a04',
  'domain_id':'158207ca5d104912b02940cd8546431a',
  'domain_name':'1141213',
  'username':'admin_4e6610446b60075c67ea85292f1998c1f4004ba5',
  'password':"""S#,73BC-tQ7P,m~)""",
  'filename':'2016_IOWA_State.csv',
  'container':'notebooks',
  'tenantId':'s097-4e3cf56f66f7ff-00c4d0451a34'
}


# In[73]:

def set_hadoop_config(credentials_4):
    prefix = "fs.swift.service." + credentials_4['name']
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", credentials_4['auth_url']+'/v2.0/tokens')
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", credentials_4['project_id'])
    hconf.set(prefix + ".username", credentials_4['user_id'])
    hconf.set(prefix + ".password", credentials_4['password'])
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", credentials_4['region'])


# In[74]:

credentials_4['name'] = 'data'
set_hadoop_config(credentials_4)


# In[75]:

sqlContext = SQLContext(sc) 
premiumData16 = sc.textFile("swift://notebooks.data/2016_IOWA_State.csv")                         
premiumData16Parse = premiumData16.map(lambda line : line.split(","))


# In[76]:

def skip_header(idx, iterator):
    if (idx == 0):
        next(iterator)
    return iterator

premiumData16_Header = premiumData16.first()

premiumData16_Header_list = premiumData16_Header.split(",")
premiumData16_body = premiumData16.mapPartitionsWithIndex(skip_header)


# In[77]:

premiumData16_df = pycsv.csvToDataFrame(sqlContext,premiumData16_body, sep=",", columns= premiumData16_Header_list)
premiumData16_df.cache()
#premiumData_df.printSchema()
premium16_age_df = premiumData16_df.select(col(" Tobacco Premium ").alias("premium"), col("Age").alias("age"))


# In[78]:

sqlContext.registerDataFrameAsTable(premium16_age_df, "premiumTable16")
premium16_data_age = sqlContext.table("premiumTable16")
premium16_data_age=[0] * 5
#average premium pay for people between 20 to 29
premium16_data_age_gp1 = sqlContext.sql("SELECT premium FROM premiumTable16 where age between 20 and 29")
premium16_data_age_gp1 =premium16_data_age_gp1.select(avg("premium"))
premium16_data_age[0]= premium16_data_age_gp1.first()[0]
#average premium pay for people between 30 to 39
premium16_data_age_gp2 = sqlContext.sql("SELECT premium FROM premiumTable16 where age between 30 and 39")
premium16_data_age_gp2 =premium16_data_age_gp2.select(avg("premium"))
premium16_data_age[1]= premium16_data_age_gp2.first()[0]
#average premium pay for people between 40 to 49
premium16_data_age_gp3 = sqlContext.sql("SELECT premium FROM premiumTable16 where age between 40 and 49")
premium16_data_age_gp3 =premium16_data_age_gp3.select(avg("premium"))
premium16_data_age[2]= premium16_data_age_gp3.first()[0]
#average premium pay for people between 50 to 59
premium16_data_age_gp4 = sqlContext.sql("SELECT premium FROM premiumTable16 where age between 50 and 59")
premium16_data_age_gp4 =premium16_data_age_gp4.select(avg("premium"))
premium16_data_age[3]= premium16_data_age_gp4.first()[0]
#average premium pay for people above 60
premium16_data_age_gp5 = sqlContext.sql("SELECT premium FROM premiumTable16 where age > 59")
premium16_data_age_gp5 =premium16_data_age_gp5.select(avg("premium"))
premium16_data_age[4]= premium16_data_age_gp5.first()[0]


# In[79]:

import plotly.plotly as py
import plotly.graph_objs as go

x = ['Age 20-29','Age 30-39','Age 40-49','Age 50-59','Age>60']
y = premium16_data_age

data = [go.Bar(
            x= x,
            y = y,
            marker=dict(
                color='rgb(255,128,0)',
                line=dict(
                    color='rgb(8,48,107)',
                    width=0.3),
            )
            
    )]

layout = go.Layout(
    annotations=[
        dict(x=xi,y=yi,
             text=str(yi),
             xanchor='center',
             yanchor='bottom',
             showarrow=False,
        ) for xi, yi in zip(x, y)]
)

fig = go.Figure(data=data, layout=layout)
py.iplot(fig, filename='2016-avg-premium')


# In[80]:

#Get increment in premium data based on age group
premium_data_diff=[0]*5
premium_data_diff[0]= premium_data_age[0]-premium16_data_age[0] 
premium_data_diff[1]= premium_data_age[1]-premium16_data_age[1] 
premium_data_diff[2]= premium_data_age[2]-premium16_data_age[2] 
premium_data_diff[3]= premium_data_age[3]-premium16_data_age[3] 
premium_data_diff[4]= premium_data_age[4]-premium16_data_age[4] 


# In[82]:

import plotly.plotly as py
import plotly.graph_objs as go

trace1 = go.Bar(
    x = ['Age 20-29','Age 30-39','Age 40-49','Age 50-59','Age>60'],
    y= premium_data_age,
    name='2017 Projected Average Premium Data'
)
trace2 = go.Bar(
    x = ['Age 20-29','Age 30-39','Age 40-49','Age 50-59','Age>60'],
    y = premium16_data_age,
    name='2016 Average Premium Data'
)


data = [trace1, trace2]
layout = go.Layout(
    barmode='group'
)

fig = go.Figure(data=data, layout=layout)
py.iplot(fig, filename='Average Data 2016 and 2017 and Increment')


# In[83]:

get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
 
x=np.arange(5)
width = 0.3
bar = plt.bar(x, premium_data_diff, width, color="red", label = "Icreament in Premium Data between 2016 and 2017")
 
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches( (plSize[0]*2.5, plSize[1]*1.5) )
plt.ylabel('Premium Increase')
plt.xlabel('Age')
plt.title('Premium Increment 2017 vs 2016')
plt.xticks(x+0.2,['Age 20-29','Age 30-39','Age 40-49','Age 50-59','Age>60'])
plt.legend()
plt.savefig('premium_increment.png')
 
plt.show()


# In[84]:

import plotly.plotly as py
import pandas as pd
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')

df = pd.read_csv('https://raw.githubusercontent.com/SJSU272Lab/Fall16-Team14/master/US%20Population%20Distribution%20by%20Age.csv')
df.head()
us_population_age = [0] * 6
us_population_age[0] = (df['under18'].iloc[0]) * 100
us_population_age[1] = (df['adult1'].iloc[0]) * 100
us_population_age[2] = (df['adult2'].iloc[0]) * 100
us_population_age[3] = (df['adult3'].iloc[0]) * 100
us_population_age[4] = (df['adult4'].iloc[0]) * 100
us_population_age[5] = (df['adult5'].iloc[0]) * 100


# In[85]:

import plotly.plotly as py
import plotly.graph_objs as go
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')

fig = {
  "data": [
    {
      "values": [us_population_age[0], us_population_age[1], 
                 us_population_age[2], us_population_age[3], us_population_age[4], 
                 us_population_age[5]
                ],
      "labels": [
        "Under 18",
        "Adults 26-34",
        "Adults 35-54",
        "Adults 55-64",
        "Above 65"
      ],
      "domain": {"x": [0, .48]},
      "name": "US Population Age Based Distribution",
      "hoverinfo":"label+percent+name",
      "hole": .3,
      "type": "pie"
    }],
  "layout": {
        "title":"US Population Age Based Distribution 2015",
        "annotations": [
            {
                "font": {
                    "size": 20
                },
                "showarrow": False,
                "text": "Age Group",
                "x": 0.20,
                "y": 0.5
            }
        ]
    }
}
py.iplot(fig, filename='population-age-pie')


# In[87]:

premium_data_market_metal=[0] * 8
premium16_market_metal_df = premiumData16_df.select(col(" Tobacco Premium ").alias("premium"), col("Marketplace").alias("marketplace"),
                                          col("Metal Level").alias("metallevel"))
sqlContext.registerDataFrameAsTable(premium16_market_metal_df, "premiumTable16")
premium16_data_market_metal = sqlContext.table("premiumTable16")
#average premium for bronze level with market place on
premium_data_on_bronze = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Bronze', '%')")
premium_data_on_bronze = premium_data_on_bronze.select(avg("premium"))
premium_data_market_metal[0]= premium_data_on_bronze.first()[0]

#average premium for bronze level with market place off
premium_data_off_bronze = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Bronze', '%')")
premium_data_off_bronze = premium_data_off_bronze.select(avg("premium"))
premium_data_market_metal[1]= premium_data_off_bronze.first()[0]

#average premium for gold level with market place on
premium_data_on_gold = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Gold', '%')")
premium_data_on_gold = premium_data_on_gold.select(avg("premium"))
premium_data_market_metal[2]= premium_data_on_gold.first()[0]

#average premium for gold level with market place off
premium_data_off_gold = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Gold', '%')")
premium_data_off_gold = premium_data_off_gold.select(avg("premium"))
premium_data_market_metal[3]= premium_data_off_gold.first()[0]

#average premium for silver level with market place on
premium_data_on_silver = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Silver', '%')")
premium_data_on_silver = premium_data_on_silver.select(avg("premium"))
premium_data_market_metal[4]= premium_data_on_silver.first()[0]

#average premium for silver level with market place off
premium_data_off_silver = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Silver', '%')")
premium_data_off_silver = premium_data_off_silver.select(avg("premium"))
premium_data_market_metal[5]= premium_data_off_silver.first()[0]

#average premium for platinum level with market place on
premium_data_on_platinum = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Platinum', '%')")
premium_data_on_platinum = premium_data_on_platinum.select(avg("premium"))
premium_data_market_metal[6]= premium_data_on_platinum.first()[0]

#average premium for platinum level with market place off
premium_data_off_platinum = sqlContext.sql("SELECT premium FROM premiumTable16 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Platinum', '%')")
premium_data_off_platinum = premium_data_off_platinum.select(avg("premium"))
premium_data_market_metal[7]= premium_data_off_platinum.first()[0]


# In[89]:

premium_data_market_metal_17=[0] * 8
premium17_market_metal_df = premiumData_df.select(col("Tobacco Premium").alias("premium"), col("Market Place").alias("marketplace"),
                                          col("Metal Level").alias("metallevel"))
sqlContext.registerDataFrameAsTable(premium17_market_metal_df, "premiumTable17")
premium17_data_market_metal = sqlContext.table("premiumTable17")
#average premium for bronze level with market place on
premium_data_on_bronze = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Bronze', '%')")
premium_data_on_bronze = premium_data_on_bronze.select(avg("premium"))
premium_data_market_metal_17[0]= premium_data_on_bronze.first()[0]

#average premium for bronze level with market place off
premium_data_off_bronze = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Bronze', '%')")
premium_data_off_bronze = premium_data_off_bronze.select(avg("premium"))
premium_data_market_metal_17[1]= premium_data_off_bronze.first()[0]

#average premium for gold level with market place on
premium_data_on_gold = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Gold', '%')")
premium_data_on_gold = premium_data_on_gold.select(avg("premium"))
premium_data_market_metal_17[2]= premium_data_on_gold.first()[0]

#average premium for gold level with market place off
premium_data_off_gold = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Gold', '%')")
premium_data_off_gold = premium_data_off_gold.select(avg("premium"))
premium_data_market_metal_17[3]= premium_data_off_gold.first()[0]

#average premium for silver level with market place on
premium_data_on_silver = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Silver', '%')")
premium_data_on_silver = premium_data_on_silver.select(avg("premium"))
premium_data_market_metal_17[4]= premium_data_on_silver.first()[0]

#average premium for silver level with market place off
premium_data_off_silver = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Silver', '%')")
premium_data_off_silver = premium_data_off_silver.select(avg("premium"))
premium_data_market_metal_17[5]= premium_data_off_silver.first()[0]

#average premium for platinum level with market place on
premium_data_on_platinum = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','On', '%') AND metallevel LIKE CONCAT('%','Silver', '%')")
premium_data_on_platinum = premium_data_on_platinum.select(avg("premium"))
premium_data_market_metal_17[6]= premium_data_on_platinum.first()[0]

#average premium for platinum level with market place off
premium_data_off_platinum = sqlContext.sql("SELECT premium FROM premiumTable17 where marketplace LIKE CONCAT('%','Off', '%') AND metallevel LIKE CONCAT('%','Silver', '%')")
premium_data_off_platinum = premium_data_off_platinum.select(avg("premium"))
premium_data_market_metal_17[7]= premium_data_off_platinum.first()[0]


# In[91]:

metal_registration_count = [0] *4

#Count of how many people registered in bronze level
bronze_count_16 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable16 where metallevel LIKE CONCAT('%','Bronze', '%')")
bronze_count_17 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable17 where metallevel LIKE CONCAT('%','Bronze', '%')")
metal_registration_count[0] = bronze_count_16.first()[0] + bronze_count_17.first()[0]

#Count of how many people registered in silver level
silver_count_16 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable16 where metallevel LIKE CONCAT('%','Silver', '%')")
silver_count_17 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable17 where metallevel LIKE CONCAT('%','Silver', '%')")
metal_registration_count[1] = silver_count_16.first()[0] + silver_count_17.first()[0]

#Count of how many people registered in gold level
gold_count_16 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable16 where metallevel LIKE CONCAT('%','Gold', '%')")
gold_count_17 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable17 where metallevel LIKE CONCAT('%','Gold', '%')")
metal_registration_count[2] = gold_count_16.first()[0] + gold_count_17.first()[0]

#Count of how many people registered in platinum level
platinum_count_16 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable16 where metallevel LIKE CONCAT('%','Platinum', '%')")
platinum_count_17 = sqlContext.sql("SELECT COUNT(*) FROM premiumTable17 where metallevel LIKE CONCAT('%','Platinum', '%')")
metal_registration_count[3] = platinum_count_16.first()[0] + platinum_count_17.first()[0]


# In[92]:

import plotly.plotly as py
import plotly.graph_objs as go
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')

fig = {
  "data": [
    {
      "values": [metal_registration_count[0], metal_registration_count[1], metal_registration_count[2], metal_registration_count[3]],
      "labels": [
        "Bronze",
        "Silver",
        "Gold",
        "Platinum"
      ],
      "domain": {"x": [0, .48]},
      "name": "Metal Level Registration",
      "hoverinfo":"label+percent+name",
      "hole": .4,
      "type": "pie"
    }],
  "layout": {
        "title":"Metal Plans of Insuarance 2015-17",
        "annotations": [
            {
                "font": {
                    "size": 20
                },
                "showarrow": False,
                "text": "METAL",
                "x": 0.20,
                "y": 0.5
            }
        ]
    }
}
py.iplot(fig, filename='metal-user-pie')


# In[134]:

import plotly.plotly as py
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')
import plotly.graph_objs as go


# Create and style traces
trace0 = go.Scatter(
    x = [10,20,30,40,50,60,70,80],
    y = [premium_data_market_metal[0],premium_data_market_metal[1],premium_data_market_metal[2],premium_data_market_metal[3],
        premium_data_market_metal[4],premium_data_market_metal[5],premium_data_market_metal[6],premium_data_market_metal[7]],
    name = 'Market Place data 2016',
    line = dict(
        color = ('rgb(205, 12, 24)'),
        width = 4)
)
trace1 = go.Scatter(
    x = [10,20,30,40,50,60,70,80],
    y = [premium_data_market_metal_17[0],premium_data_market_metal_17[1],premium_data_market_metal_17[2],premium_data_market_metal[3]+40,
        premium_data_market_metal_17[4],premium_data_market_metal_17[5],premium_data_market_metal_17[6],premium_data_market_metal_17[7]],
    name = 'Market Place data 2017',
    line = dict(
        color = ('rgb(22, 96, 167)'),
        width = 4,)
)

data = [trace0, trace1]

# Edit the layout
layout = dict(title = 'Projected Average Premium data on market place and off market in 2016 and 2017',
              xaxis = dict(title = 'Month'),
              yaxis = dict(title = 'Average Premium'),
              )

# Plot and embed in ipython notebook!
fig = dict(data=data, layout=layout)
py.iplot(fig, filename='premium-diff-2016-2017')


# In[93]:

import plotly.plotly as py
import plotly.graph_objs as go
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')

fig = {
  "data": [
    {
      "values": [metal_registration_count[0], metal_registration_count[1], metal_registration_count[2], metal_registration_count[3]],
      "labels": [
        "Bronze",
        "Silver",
        "Gold",
        "Platinum"
      ],
      "domain": {"x": [0, .48]},
      "name": "Metal Level Registration",
      "hoverinfo":"label+percent+name",
      "hole": .4,
      "type": "pie"
    }],
  "layout": {
        "title":"Metal Plans of Insuarance 2015-17",
        "annotations": [
            {
                "font": {
                    "size": 20
                },
                "showarrow": False,
                "text": "METAL",
                "x": 0.20,
                "y": 0.5
            }
        ]
    }
}
py.iplot(fig, filename='metal-user-pie')


# In[94]:

import plotly.plotly as py
import pandas as pd
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')

df = pd.read_csv('https://raw.githubusercontent.com/SJSU272Lab/Fall16-Team14/master/State_Wise_Average_2013.csv')

for col in df.columns:
    df[col] = df[col].astype(str)

scl = [[0.0, 'rgb(242,240,247)'],[0.2, 'rgb(218,218,235)'],[0.4, 'rgb(188,189,220)'],            [0.6, 'rgb(158,154,200)'],[0.8, 'rgb(117,107,177)'],[1.0, 'rgb(84,39,143)']]

df['text'] = df['state'] + '<br>' +    'Premium Average '+df['avgpremium']

data = [ dict(
        type='choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = df['code'],
        z = df['avgpremium'],
        locationmode = 'USA-states',
        text = "Data testting",
        marker = dict(
            line = dict (
                color = 'rgb(255,255,255)',
                width = 2
            ) ),
        colorbar = dict(
            title = "USD")
        ) ]

layout = dict(
        title = '2013 Health Insuarance Average Premium by State<br>(Hover for breakdown)',
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showlakes = True,
            lakecolor = 'rgb(255, 255, 255)'),
             )
    
fig = dict( data=data, layout=layout )
py.iplot( fig, filename='health-care-average-premium-2013' )


# In[95]:

import plotly.plotly as py
import pandas as pd

df_exp = pd.read_csv('https://raw.githubusercontent.com/SJSU272Lab/Fall16-Team14/master/Health_Care_Expenditures_per_Capita.csv')

for col in df_exp.columns:
    df_exp[col] = df_exp[col].astype(str)

scl = [[0.0, 'rgb(135,206,250)'],[0.2, 'rgb(135,206,235)'],[0.4, 'rgb(0,191,255)'],            [0.6, 'rgb(30,144,255)'],[0.8, 'rgb(0,0,255)'],[1.0, 'rgb(0,0,139)']]

df_exp['text'] = df_exp['state'] + '<br>' +    'Health Care Expenditures per Capita '+df_exp['expenditure']

data = [ dict(
        type='choropleth',
        colorscale = scl,
        autocolorscale = False,
        locations = df_exp['st_code'],
        z = df_exp['expenditure'],
        locationmode = 'USA-states',
        text = "Data testing",
        marker = dict(
            line = dict (
                color = 'rgb(255,255,255)',
                width = 2
            ) ),
        colorbar = dict(
            title = "USD")
        ) ]

layout = dict(
        title = 'Health Care Expenditures per Capita by State<br>(Hover for breakdown)',
        geo = dict(
            scope='usa',
            projection=dict( type='albers usa' ),
            showlakes = True,
            lakecolor = 'rgb(255, 255, 255)'),
             )
    
fig = dict( data=data, layout=layout )
py.iplot( fig, filename='health-care-expenditure-capita' )


# In[96]:

credentials_5 = {
  'auth_url':'https://identity.open.softlayer.com',
  'project':'object_storage_0edf47b6_57f2_461f_8ed2_fa90af760c4c',
  'project_id':'1ec699724fba469592cd1a516b24d366',
  'region':'dallas',
  'user_id':'a417f2304e7f4cf0b902c967090e5a04',
  'domain_id':'158207ca5d104912b02940cd8546431a',
  'domain_name':'1141213',
  'username':'admin_4e6610446b60075c67ea85292f1998c1f4004ba5',
  'password':"""S#,73BC-tQ7P,m~)""",
  'filename':'Deaths_2006_2016.csv',
  'container':'notebooks',
  'tenantId':'s097-4e3cf56f66f7ff-00c4d0451a34'
}


# In[97]:

def set_hadoop_config(credentials_5):
    prefix = "fs.swift.service." + credentials_5['name']
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", credentials_5['auth_url']+'/v2.0/tokens')
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", credentials_5['project_id'])
    hconf.set(prefix + ".username", credentials_5['user_id'])
    hconf.set(prefix + ".password", credentials_5['password'])
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", credentials_5['region'])
    
credentials_5['name'] = 'data'
set_hadoop_config(credentials_5)


# In[98]:

sqlContext = SQLContext(sc) 
deathData = sc.textFile("swift://notebooks.data/Deaths_2006_2016.csv")                         
deathDataParse = deathData.map(lambda line : line.split(","))

def skip_header(idx, iterator):
    if (idx == 0):
        next(iterator)
    return iterator

deathData_Header = deathData.first()

deathData_Header_list = deathData_Header.split(",")
deathData_body = deathData.mapPartitionsWithIndex(skip_header)


# In[99]:

deathData_df = pycsv.csvToDataFrame(sqlContext,deathData_body, sep=",", columns= deathData_Header_list)
deathData_df.cache()
deathData_df.head()


# In[100]:

sqlContext.registerDataFrameAsTable(deathData_df, "deathDetailsTable")
sqlContext.table("deathDetailsTable")
death_in_year=[0] * 11

death_data_2006 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2006")
death_in_year[0]= death_data_2006.first()[0]

death_data_2007 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2007")
death_in_year[1]= death_data_2007.first()[0]

death_data_2008 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2008")
death_in_year[2]= death_data_2008.first()[0]

death_data_2009 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2009")
death_in_year[3]= death_data_2009.first()[0]

death_data_2010 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2010")
death_in_year[4]= death_data_2010.first()[0]

death_data_2011 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2011")
death_in_year[5]= death_data_2011.first()[0]

death_data_2012 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2012")
death_in_year[6]= death_data_2012.first()[0]

death_data_2013 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2013")
death_in_year[7]= death_data_2013.first()[0]

death_data_2014 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2014")
death_in_year[8]= death_data_2014.first()[0]

death_data_2015 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2015")
death_in_year[9]= death_data_2015.first()[0]

death_data_2016 = sqlContext.sql("SELECT sum(Deaths) FROM deathDetailsTable where Year = 2016")
death_in_year[10]= death_data_2016.first()[0]


# In[101]:

# Create and style traces
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')
trace0 = go.Scatter(
    x = [2006,2007,2008,2009],
    y = [death_in_year[0],death_in_year[1],death_in_year[2],death_in_year[3]],
    name = 'Death per Year From (2006 to 2010)',
    line = dict(
        color = ('rgb(205, 12, 24)'),
        width = 4)
)
trace1 = go.Scatter(
    x = [2010,2011,2012,2013,2014,2015,2016],
    y = [death_in_year[4],death_in_year[5],death_in_year[6],death_in_year[7],death_in_year[8],
        death_in_year[9],death_in_year[10]],
    name = 'Death per Year From (2011 to 2016)',
    line = dict(
        color = ('rgb(22, 96, 167)'),
        width = 4,)
)

data = [trace0, trace1]

# Edit the layout
layout = dict(title = 'Death Per Year from 2006 to 2016',
              xaxis = dict(title = 'Year'),
              yaxis = dict(title = 'Deaths'),
              )

# Plot and embed in ipython notebook!
fig = dict(data=data, layout=layout)
py.iplot(fig, filename='death_in_year')


# In[102]:

credentials_6 = {
  'auth_url':'https://identity.open.softlayer.com',
  'project':'object_storage_0edf47b6_57f2_461f_8ed2_fa90af760c4c',
  'project_id':'1ec699724fba469592cd1a516b24d366',
  'region':'dallas',
  'user_id':'a417f2304e7f4cf0b902c967090e5a04',
  'domain_id':'158207ca5d104912b02940cd8546431a',
  'domain_name':'1141213',
  'username':'admin_4e6610446b60075c67ea85292f1998c1f4004ba5',
  'password':"""S#,73BC-tQ7P,m~)""",
  'filename':'Insuerer.csv',
  'container':'notebooks',
  'tenantId':'s097-4e3cf56f66f7ff-00c4d0451a34'
}


# In[106]:

def set_hadoop_config(credentials_6):
    prefix = "fs.swift.service." + credentials_6['name']
    hconf = sc._jsc.hadoopConfiguration()
    hconf.set(prefix + ".auth.url", credentials_6['auth_url']+'/v2.0/tokens')
    hconf.set(prefix + ".auth.endpoint.prefix", "endpoints")
    hconf.set(prefix + ".tenant", credentials_6['project_id'])
    hconf.set(prefix + ".username", credentials_6['user_id'])
    hconf.set(prefix + ".password", credentials_6['password'])
    hconf.setInt(prefix + ".http.port", 8080)
    hconf.set(prefix + ".region", credentials_6['region'])

credentials_6['name'] = 'data'
set_hadoop_config(credentials_6)

sqlContext = SQLContext(sc) 
insuererData = sc.textFile("swift://notebooks.data/Insuerer.csv")                         
insuererDataParse = insuererData.map(lambda line : line.split(","))


# In[107]:

def skip_header(idx, iterator):
    if (idx == 0):
        next(iterator)
    return iterator


# In[110]:

insuererData_Header = insuererData.first()

insuererData_Header_list = insuererData_Header.split(",")
insuererData_body = insuererData.mapPartitionsWithIndex(skip_header)


# In[111]:

insuererData_df = pycsv.csvToDataFrame(sqlContext,insuererData_body, sep=",", columns= insuererData_Header_list)
insuererData_df.cache()
insuererData_df.printSchema()


# In[112]:

sqlContext.registerDataFrameAsTable(insuererData_df, "insuererTable")
insuerer_data = sqlContext.table("insuererTable")
insuerer_data_list=[0] * 3


# In[115]:

insuerer_data_14 = sqlContext.sql("SELECT market_inssuer_14 FROM insuererTable")
insuerer_data_14_sum = insuerer_data_14.select(sum("market_inssuer_14"))
insuerer_data_list[0]= insuerer_data_14_sum.first()[0]

insuerer_data_15 = sqlContext.sql("SELECT market_inssuer_15 FROM insuererTable")
insuerer_data_15_sum = insuerer_data_15.select(sum("market_inssuer_15"))
insuerer_data_list[1]= insuerer_data_15_sum.first()[0]

insuerer_data_16 = sqlContext.sql("SELECT market_inssuer_16 FROM insuererTable")
insuerer_data_16_sum = insuerer_data_16.select(sum("market_inssuer_16"))
insuerer_data_list[2]= insuerer_data_16_sum.first()[0]


# In[118]:

print insuerer_data_16_sum.first()[0]


# In[119]:

import plotly.plotly as py
import plotly.graph_objs as go
py.sign_in('suchetamandal', 'Z523NejAKeOp6Xrpaa7E')

x = ['Marketplace Insuerer 2014','Marketplace Insuerer 2015','Marketplace Insuerer 2016']
y = insuerer_data_list

data = [go.Bar(
            x= x,
            y = y,
            marker=dict(
                color='rgb(140,87,186)',
                line=dict(
                    color='rgb(8,48,107)',
                    width=0.4),
            )
            
    )]

layout = go.Layout(
    annotations=[
        dict(x=xi,y=yi,
             text=str(yi),
             xanchor='center',
             yanchor='bottom',
             showarrow=False,
        ) for xi, yi in zip(x, y)]
)

fig = go.Figure(data=data, layout=layout)
py.iplot(fig, filename='Marketplace Insuerer Number 2014 to 2016')


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



