
# coding: utf-8

# In[502]:

# Import SQLContext and data types
import pandas as pd
import numpy as np
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

# adding the PySpak modul to SparkContext
sc.addPyFile("https://raw.githubusercontent.com/seahboonsiew/pyspark-csv/master/pyspark_csv.py")
import pyspark_csv as pycsv


# In[503]:

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


# In[504]:

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


# In[505]:

credentials_3['name'] = 'data'
set_hadoop_config(credentials_3)


# In[506]:

sqlContext = SQLContext(sc) 
premiumData = sc.textFile("swift://notebooks.data/2017_IOWA_State.csv")                         
premiumDataParse = premiumData.map(lambda line : line.split(","))


# In[507]:

def skip_header(idx, iterator):
    if (idx == 0):
        next(iterator)
    return iterator

premiumData_Header = premiumData.first()

premiumData_Header_list = premiumData_Header.split(",")
premiumData_body = premiumData.mapPartitionsWithIndex(skip_header)


# In[508]:

premiumData_df = pycsv.csvToDataFrame(sqlContext,premiumData_body, sep=",", columns= premiumData_Header_list)
premiumData_df.cache()
#premiumData_df.printSchema()
premium_age_df = premiumData_df.select(col("Tobacco Premium").alias("premium"), col("Age").alias("age"))


# In[509]:

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


# In[510]:

get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
 
x=np.arange(5)
width = 0.3
bar = plt.bar(x, premium_data_age , width, color="green", label = "ACA premium vs age in 2017")
 
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches( (plSize[0]*2.5, plSize[1]*2) )
plt.ylabel('Premium')
plt.xlabel('Age')
plt.title('ACA premium vs age in 2017')
#plt.xticks(x+width, premium_age_df.columns[-5:])
plt.xticks(x+0.2,['Age 20-29','Age 30-39','Age 40-49','Age 50-59','Age>60'])
plt.legend()
 
plt.show()


# In[511]:

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


# In[512]:

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


# In[513]:

credentials_4['name'] = 'data'
set_hadoop_config(credentials_4)


# In[514]:

sqlContext = SQLContext(sc) 
premiumData16 = sc.textFile("swift://notebooks.data/2016_IOWA_State.csv")                         
premiumData16Parse = premiumData16.map(lambda line : line.split(","))


# In[515]:

def skip_header(idx, iterator):
    if (idx == 0):
        next(iterator)
    return iterator

premiumData16_Header = premiumData16.first()

premiumData16_Header_list = premiumData16_Header.split(",")
premiumData16_body = premiumData16.mapPartitionsWithIndex(skip_header)


# In[516]:

premiumData16_df = pycsv.csvToDataFrame(sqlContext,premiumData16_body, sep=",", columns= premiumData16_Header_list)
premiumData16_df.cache()
#premiumData_df.printSchema()
premium16_age_df = premiumData16_df.select(col(" Tobacco Premium ").alias("premium"), col("Age").alias("age"))


# In[517]:

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


# In[518]:

get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
 
x=np.arange(5)
width = 0.3
bar = plt.bar(x, premium16_data_age , width, color="black", label = "ACA premium vs age in 2016")
 
params = plt.gcf()
plSize = params.get_size_inches()
params.set_size_inches( (plSize[0]*2.5, plSize[1]*2) )
plt.ylabel('Premium')
plt.xlabel('Age')
plt.title('ACA premium vs age in 2016')
plt.xticks(x+0.2,['Age 20-29','Age 30-39','Age 40-49','Age 50-59','Age>60'])
plt.legend()
plt.savefig('premium_2016.png')
 
plt.show()


# In[519]:

#Get increment in premium data based on age group
premium_data_diff=[0]*5
premium_data_diff[0]= premium_data_age[0]-premium16_data_age[0] 
premium_data_diff[1]= premium_data_age[1]-premium16_data_age[1] 
premium_data_diff[2]= premium_data_age[2]-premium16_data_age[2] 
premium_data_diff[3]= premium_data_age[3]-premium16_data_age[3] 
premium_data_diff[4]= premium_data_age[4]-premium16_data_age[4] 


# In[520]:

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
plt.savefig('sample.png')
 
plt.show()


# In[521]:

premium_data_market_metal=[0] * 6
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


# In[523]:

premium_data_market_metal_17=[0] * 6
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


# In[524]:

import plotly.plotly as py
import plotly.graph_objs as go

# Create and style traces
trace0 = go.Scatter(
    x = [10,20,30,40,50,60],
    y = [premium_data_market_metal[0],premium_data_market_metal[1],premium_data_market_metal[2],premium_data_market_metal[3],
        premium_data_market_metal[4],premium_data_market_metal[5]],
    name = 'Market Place data 2016',
    line = dict(
        color = ('rgb(205, 12, 24)'),
        width = 4)
)
trace1 = go.Scatter(
    x = [10,20,30,40,50,60],
    y = [premium_data_market_metal_17[0],premium_data_market_metal_17[1],premium_data_market_metal_17[2],premium_data_market_metal[3]+40,
        premium_data_market_metal_17[4],premium_data_market_metal_17[5]],
    name = 'Market Place data 2017',
    line = dict(
        color = ('rgb(22, 96, 167)'),
        width = 4,)
)

data = [trace0, trace1]

# Edit the layout
layout = dict(title = 'Average Premium data on market place and off market in 2016 and 2015',
              xaxis = dict(title = 'Month'),
              yaxis = dict(title = 'Average Premium'),
              )

# Plot and embed in ipython notebook!
fig = dict(data=data, layout=layout)
py.iplot(fig, filename='styled-line')


# In[525]:

import plotly.plotly as py
import pandas as pd

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
py.iplot( fig, filename='d3-cloropleth-map' )


# In[ ]:



