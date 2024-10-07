from pyspark.sql import SparkSession
from pyspark.sql import *
from configparser import *
from pyspark import SparkConf
import sys
from delta import configure_spark_with_delta_pip
from delta.tables import *
from delta import *
from pyspark.sql.functions import *


def sess_spark(typ, prop, jdbc_lib):
    spark_config = SparkConf()
    config = ConfigParser()
    config.read(prop)
    for name, value in config.items("CONFIGS"):
        spark_config.set(name, value)

    try:
        if typ == 'Delta':
            builder = SparkSession.builder.appName("Spark pro").config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog").config("spark.jars",jdbc_lib).enableHiveSupport()
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
            return spark
        else:
            bulider1 = SparkSession.builder.config(conf=spark_config).config("spark_jars",jdbc_lib).enableHiveSupport()
            spark = bulider1.getOrCreate()
            return spark

    except Exception as spark_error:
        print(spark_error)
        sys.exit(1)


def read_data(sess, type, source, struct, infsch=False, delim=',',head = False):
    if type == 'csv' and struct == '':
        read_df = sess.read.csv(source, inferSchema=infsch, sep=delim, header=head)
        return read_df
    elif type == 'csv' and struct != '':
        read_df = sess.read.csv(source, schema=struct, inferSchema=infsch, sep=delim, header=head)
        return read_df
    elif type == 'json':
        read_df = sess.read.option("multiline", "true").schema(struct).json(source)
        return read_df


def write_data(type, df, loc, mode, delim=',', head=False):
    if type == 'csv':
        df.write.mode(mode).csv(loc, header=head, sep=delim)
    elif type == 'json':
        df.write.mode(mode).option("multiline", "true").json(loc)


def check_hive_tbl(sess, db, tbl):
    from pyspark.sql.functions import col
    if (sess.sql(f"show tables in {db}").filter(col("tablename") == tbl).count() > 0):
        return True
    else:
        return False


def readhivetbl(sess, tbl):
    hive_df = sess.read.table(tbl)
    return hive_df


def writehivetbl(df, tbl, part, partcol, mode):
    if part == False:
        df.write.mode(mode).saveAsTable(tbl)
    else:
        df.write.mode(mode).partitionBy(partcol).saveAsTable(tbl)


def getRDBMSpart(sess, prop, db, tbl, partcol, low, upp, numpart):
    config = ConfigParser()
    config.read(prop)
    driver = config.get('DBCRED', 'driver')
    host = config.get('DBCRED', 'host')
    port = config.get('DBCRED', 'port')
    user = config.get('DBCRED', 'user')
    password = config.get('DBCRED', 'pass')
    url = host + ":" + port + "/" + db

    db_df = sess.read.format("jdbc").option("url", url).option("dbtable", tbl).option("user", user).option("password",password).option("driver", driver).option("lowerbound", low).option("upperbound", upp).option("numpartitions", numpart).option("partitioncolumn", partcol).load()
    return db_df


def getRDBMS(sess, prop, db, tbl):
    config = ConfigParser()
    config.read(prop)
    driver = config.get('DBCRED', 'driver')
    host = config.get('DBCRED', 'host')
    port = config.get('DBCRED', 'port')
    user = config.get('DBCRED', 'user')
    password = config.get('DBCRED', 'pass')
    url = host + ":" + port + "/" + db

    db_df = sess.read.format("jdbc").option("url", url).option("dbtable", tbl).option("user", user).option("password",password).option("driver", driver).load()
    return db_df


def writeRDBMS(df, prop, db, tbl, mode):
    config = ConfigParser()
    config.read(prop)
    driver = config.get('DBCRED', 'driver')
    host = config.get('DBCRED', 'host')
    port = config.get('DBCRED', 'port')
    user = config.get('DBCRED', 'user')
    password = config.get('DBCRED', 'pass')
    url = host + ":" + port + "/" + db
    url1 = url + "?user=" + user + "&password=" + password
    df.write.jdbc(url = url1, table = tbl, mode = mode)

def optimize(sess, df, numpart, part, cache, numshufflepart=200):
    if part:
        print(f"number of partitions in {df} is {df.rdd.getNumPartitions()}")
        opti_df = df.repartition(numpart)
        print(f"repartitioned to {opti_df.rdd.getNumPartitions()}")
    else:
        opti_df = df.coalesce(numpart)
        print(f"coalesced to {opti_df.rdd.getNumPartitions()}")
    if cache:
        df.cache()
        print("cached")
    if numshufflepart != 200:
        sess.conf.set("spark.sql.shuffle.partitions", numshufflepart)
        print("shuffled partition to {}".format(numshufflepart))
    return df

def munge(df, dedup, naall, naany, allsub, anysub, sub):
    if dedup:
        print(f"non-duplicated count is {df.count()}")
        df = df.dropDuplicates()
        print(f"de-duplicated count is {df.count()}")
    if naall:
        df = df.na.drop("all")
    if naany:
        df = df.na.drop("any")
    if allsub:
        df = df.na.drop(how="all", subset=sub)
    if anysub:
        df = df.na.drop(how="any", subset=sub)
    print("count of df is {}".format(df.count()))
    return df


