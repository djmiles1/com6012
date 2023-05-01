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

#%% Load file
logFile=spark.read.text("../ScalableML/Data/NASA_access_log_Jul95.gz").cache()
#logFile=spark.read.text("../Data/NASA_access_log_Jul95.gz").cache()

#%% Q1_A using dict
hosts = {
    "germany": logFile.filter(logFile.value.contains(".de")).count(),
    "canada": logFile.filter(logFile.value.contains(".ca")).count(),
    "singapore": logFile.filter(logFile.value.contains(".sg")).count()
}

plt.bar(hosts.keys(),hosts.values())
plt.savefig('requests.png')
for host in hosts:
    print('There are {hosts[host]} requests from {host}')

#%% Q1_B using dict
hostsData = {}
for host in hosts:
    # split into 5 columns using regex and split from lab 2
    hostsData[host] = hosts[host].withColumn('host', F.regexp_extract('value', '^(.*) - -.*', 1)) \
                                .withColumn('timestamp', F.regexp_extract('value', '.* - - \[(.*)\].*',1)) \
                                .withColumn('request', F.regexp_extract('value', '.*\"(.*)\".*',1)) \
                                .withColumn('HTTP reply code', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) -2).cast("int")) \
                                .withColumn('bytes in the reply', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) - 1).cast("int")).drop("value").cache()

hostsCount = {}
for host in hostsData:
    hostsCount[host] = hostsData[host].select('host').distinct().count() # Number of unique hosts
    
    

#%% Q1_A using list
hosts = [
    logFile.filter(logFile.value.contains(".de")).count(),  #Germany
    logFile.filter(logFile.value.contains(".ca")).count(),  #Canada
    logFile.filter(logFile.value.contains(".sg")).count()   #Singapore
    ]

x = ['germany', 'canada', 'singapore']

plt.bar(x,hosts)
plt.savefig('requests.png')

#%% Q1_B using list
data = []
for host in hosts:
    data.append(host.withColumn('host', F.regexp_extract('value', '^(.*) - -.*', 1)) \
                    .withColumn('timestamp', F.regexp_extract('value', '.* - - \[(.*)\].*',1)) \
                    .withColumn('request', F.regexp_extract('value', '.*\"(.*)\".*',1)) \
                    .withColumn('HTTP reply code', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) -2).cast("int")) \
                    .withColumn('bytes in the reply', F.split('value', ' ').getItem(F.size(F.split('value', ' ')) - 1).cast("int")).drop("value").cache()
                )
    



# number of unique hosts
n_hosts = data.select('host').distinct().count()

# most visited host
host_count = data.select('host').groupBy('host').count().sort('count', ascending=False)
host_max = host_count.select("host").first()['host']
print("==================== Question 3 ====================")
print(f"The most frequently visited host is {host_max}")
print("====================================================")