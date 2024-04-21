#  1 Write a python program to write and read csv file using pandas

import pandas as pd

header = ['Name', 'Reg No', 'CGPA']

data = [
    ['Abin J', 'RA2131242010019', 8.19],
    ['Pradeep', 'RA2131242010020', 9.19],
    ['Giri', 'RA2131242010017', 9.00],
    ['Deepak', 'RA2131242010022', 9.7],
    ['Kaviya', 'RA2131242010031', 9.7],
    ['Swetha', 'RA2131242010011', 8.9],
    ['Gokul', 'RA213124201001', 9.0],
    ['Akhil', 'RA2131242010050', 8.76],
    ['Arjun', 'RA2131242010046', 9.9],
    ['Vishal', 'RA2131242010045', 8.8],
    ['Jerin', 'RA2131242010060', 8.9]
]

df = pd.DataFrame(data, columns=header)
df.head()

# Write into a CSV file
csv = pd.DataFrame.to_csv(df, 'df.csv')

# Read from a CSV file
read_csv = pd.read_csv('/content/df.csv')
read_csv.head()

# 2.Write a python program to plot an area of the circle using matplotlib.
import matplotlib.pyplot as plt

r = [1, 5, 6, 8]
area = []

for i in r:
    area.append(3.14 * i * i)

plt.xlabel('Area')
plt.ylabel('Radius')
plt.title('Area of circle')
plt.plot(area, r)
plt.show()

# 3.Write a python program to create Data frame using Polars
import polars

# Create DataFrame from dictionary
df = polars.DataFrame({
    'Name': ['Abin J', 'Pradeep', 'Giri', 'Deepak', 'Kaviya', 'Swetha', 'Gokul', 'Akhil', 'Arjun', 'Vishal', 'Jerin'],
    'Register_no': ['219', '220', '217', '222', '231', '211', '201', '250', '246', '245', '260'],
    'CGPA': [8.19, 9.19, 9.00, 9.7, 9.7, 8.9, 9.0, 8.76, 9.9, 8.8, 8.9]
})

# Display the Dataframe
print(df)

# 4.Write a python program to read csv file using Polar module.
import polars as pl

# Read CSV file into a DataFrame
query = pl.scan_csv('/content/sample_data/california_housing_test.csv')

# Filter rows where population > 500, group by total_bedrooms and aggregate with all().sum()
query = query.filter(pl.col('population') > 500)
query = query.groupby('total_bedrooms')
query = query.agg(pl.all().sum())

# Collect the result as a DataFrame
df = query.collect()

# Print the first few rows of the DataFrame
print(df.head())

# 1.Write a python program to perform various line properties in graph using Matplotlib.
#importing package
import matplotlib.pyplot as plt

#creating list to store x-axis and y-axis value 
x = [1, 2, 3, 4, 5, 6]
y = [4, 5, 6, 8, 9, 5]

#plotting the simple graph 
plt.plot(x, y)
plt.xlabel('X-axis')
plt.ylabel('Y-axis')
plt.title('My-Graph')
plt.show()

plt.bar(x, y)
plt.xlabel('x-axis')
plt.ylabel('y-axis')
plt.title('MY GRAPH')
plt.show()

plt.hist2d(x, y)
plt.xlabel('x-axis')
plt.ylabel('y-axis')
plt.title('MY GRAPH')
plt.show()


plt.plot(x, y, color='green', linestyle='dashed', linewidth=1)
plt.xlabel('x-axis')
plt.ylabel('y-axis')
plt.title('MY GRAPH')
plt.show()

# 2.Write a python program to normalize the data using sklearn. 
#importing packages numpy and sklearn 
import numpy as np
from sklearn import preprocessing as pre

#Creating array using numpy 
x = np.array([2, 33, 4, 567, 6, 7, 78])
print("Before normalization:", x)

#Normalizing the data 
normalized_x = pre.normalize([x])
print("Normalized data:", normalized_x)

#3.write a python program to normalize Iris dataset.

from sklearn.datasets import load_iris
from sklearn import preprocessing as pre 

#loading data
iris = load_iris()

#Training data and labels x and y variables 
x = iris.data
y = iris.target

print("Training data:", x)
print("Training labels:", y)

#Normalization
norm = pre.normalize(x)
print("Normalized training data:", norm)

# 4.write a python program to basic operations using sparkcontext.

# Download pyspark using pip
!pip install pyspark

# Importing Spark Context
from pyspark import SparkContext

# Creating spark section
sc = SparkContext("local", "Basic_operation")

word = sc.parallelize(["java", "hadoop", "python", "spark", "hacker", "Manmade", "python and spark", "java", "spark and pyspark", "spark and pyspark"])
count = word.count()
print("Number of elements in RDD:", count)

coll = word.collect()
print("Elements in RDD:", coll)

word_filter = word.filter(lambda x: 'spark' in x)
Filter = word_filter.collect()
print("Filtered RDD:", Filter)

sc.stop()

# 5.write a python program to perform map and reduce using pyspark
# Download pyspark using pip
!pip install pyspark 

# Importing Spark Context
from pyspark import SparkContext 

# Creating spark section
sc = SparkContext("local", "Basic_operation")

word = sc.parallelize(["java", "hadoop", "python", "spark", "hacker", "Manmade", "python and spark", "java"])
count = word.count()
print("Number of elements in RDD:", count)

coll = word.collect()
print("Elements in RDD:", coll)

word_map = word.map(lambda x: (x, 1))
mapping = word_map.collect() 
print("Key value pair:", mapping)

from operator import add
num = sc.parallelize([10, 20, 30, 40, 56, 78, 45, 67, 78, 23])
adding = num.reduce(add)
print('Adding all the elements:', adding)

sc.stop()


# 6.write a python program to create spark dataframe.
!pip install pyspark 
from pyspark.sql import SparkSession 

ss = SparkSession.builder.master('local').appName('Toydata_test').getOrCreate()

data = [
    {"Category": "A", "ID": '1', "value": 121.44, "Truth": "True"},
    {"Category": "B", "ID": '2', "value": 123.44, "Truth": "False"},
    {"Category": "C", "ID": '3', "value": 125.64, "Truth": "None"},
    {"Category": "D", "ID": '4', "value": 121.84, "Truth": "True"},
    {"Category": "E", "ID": '5', "value": 122.74, "Truth": "False"},
    {"Category": "F", "ID": '6', "value": 126.94, "Truth": "None"},
    {"Category": "G", "ID": '7', "value": 127.64, "Truth": "True"},
    {"Category": "H", "ID": '8', "value": 123.43, "Truth": "False"},
    {"Category": "I", "ID": '9', "value": 128.33, "Truth": "None"},
    {"Category": "J", "ID": '10', "value": 131.64, "Truth": "True"},
    {"Category": "K", "ID": '11', "value": 129.74, "Truth": "False"},
    {"Category": "L", "ID": '12', "value": 130.44, "Truth": "None"},
    {"Category": "M", "ID": '13', "value": 125.55, "Truth": "True"}
]

df = ss.createDataFrame(data)
df.printSchema()
df.show()

df.createOrReplaceTempView('table')
ss.sql("SELECT * FROM table WHERE Truth=true ORDER BY value ASC").show()

df.filter(df.Truth == "True").sort(df.value).show()

ss.stop()

