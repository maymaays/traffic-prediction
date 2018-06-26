import os
import sys

# path for spark source folder
os.environ['SPARK_HOME']="/usr/local/Cellar/apache-spark/2.3.1"

# append pyspark to Python Path
sys.path.append("/usr/local/Cellar/apache-spark/2.3.1/libexec/python/pyspark")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)
