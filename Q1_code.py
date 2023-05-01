#%% Basic File Setup
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import wget
import matplotlib.pyplot as plt

spark = SparkSession.builder \
    .master("local[2]") \
    .appName("COM6012 Spark Intro") \
    .config("spark.local.dir","/fastdata/ACP21DJM") \
    .getOrCreate()
    
sc = spark.sparkContext
sc.setLogLevel("WARN")  # This can only affect the log level after it is executed.

#%% Download file
NASA_Jul95 = 'ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz'
data_loc = '../Data'
wget.download(NASA_Jul95, out=data_loc)

#%% Load file & split into columns
#logFile=spark.read.text("../ScalableML/Data/NASA_access_log_Jul95.gz").cache()
logFile=spark.read.text("../Data/NASA_access_log_Jul95.gz").cache()

data = logFile.withColumn('host', F.regexp_extract('value', '^(.*) - -.*', 1)) \
                .withColumn('timestamp', F.regexp_extract('value', '.* - - \[(.*)\].*',1)) \
                .withColumn('request', F.regexp_extract('value', '.*\"(.*)\".*',1)) \
                .withColumn('HTTP reply code', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) -2).cast("int")) \
                .withColumn('bytes in the reply', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) - 1).cast("int")).drop("value").cache()


#%% Q1_A filter logfile into hosts
hosts = {
    "Germany": data.filter(data.host.rlike('.de$')),
    "Canada": data.filter(data.host.rlike('.ca$')),
    "Singapore": data.filter(data.host.rlike('.sg$'))
}

counts = {}
for host in hosts:
    counts[host] = hosts[host].count()

plt.bar(hosts.keys(),counts.values())
plt.savefig('requests.png')
for host in hosts:
    print(f'There are {counts[host]} requests from {host}')

#%% Q1_B using dict

hostsCount = {}
for host in hosts:
    uniqueHosts = hosts[host].select('host').distinct().count() # Number of unique hosts
    hostsCount[host] = hosts[host].select('host').groupBy('host').count().sort('count', ascending=False) # Sort by most visited host
    
    
    print(f'{host} has {uniqueHosts} unique hosts. \nThe top 9 are:')
    hostsCount[host].show(9,False)



#%% Q1_C
for host in hostsCount:
    rows = hostsCount[host].collect()
    rates = {}
    for i in range(9):
        rates[rows[i].__getitem__('host')] = (rows[i].__getitem__('count') / hosts[host].count()) # Percentage of requests
    
    plt.clf()
    plt.title(host)
    plt.xticks(rotation=90)
    plt.bar(rates.keys(), rates.values())
    plt.savefig(f'{host} rates.png', bbox_inches='tight')
    
    
#%% Q1_D
