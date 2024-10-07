from pyspark.sql.functions import *
from pyspark.sql.window import Window
#from Common_func import readhivetbl
from delta.tables import *
from delta import *


def writedatascd2(tblflag, sess, tbl, df_new_emp):
    if tblflag == False:
        df_new_emp = df_new_emp.withColumn("ver", lit(1))
        df_new_emp.createOrReplaceTempView("emp")
        print("employee table not exist and the new data is..")
        sess.sql("select * from emp").show(10, False)
        sess.sql("create external table spark_retail_dim.hive_employees (`empnum` int,`lname` string,`fname` string,`extension` string,`email` string,`offcode` string,`report` int,`job` string,`upddt` date,`leave` string,`ver` int) row format delimited fields terminated by ',' location '/user/hduser/spark_pro/empdata/'")
        sess.sql("insert into spark_retail_dim.hive_employees select * from emp")
        print("table doesn't exists, hence created and loaded the data")
    elif tblflag == True:
        print("Hive employees table exists")
        exis_tbl = readhivetbl(sess, tbl).groupBy("empnum").agg(max("ver")).alias("max_ver")
        if exis_tbl.count() > 0:
            joined_tbl = df_new_emp.alias("new").join(exis_tbl.alias("exist"), on="empnum", how="inner")
            joined_tbl_exist = joined_tbl.select("new.*", "exist.max_ver").withColumn("ver", expr(
                "row_number().over(partition by empnum order by upddt) + coalesce(max_ver,0)")).drop("max_ver")
            joined_tbl_exist.show()
            joined_tbl_exist.createOrReplaceTempView("empexist")
            print("employee exist and the old+new data is..")
            sess.sql("select * from empexist").show()
            sess.sql("insert into spark_retail_dim.hive_employees select * from empexist")


def writedataDeltaMerge(tblflag, sess, tbl, df_prod_new_udf):
    if tblflag == False:
        print("table doesn't exists")
        df_prod_new_udf.registerTempTable("completetable")
        sess.sql("drop table if exists spark_retail_dim.hive_products")
        sess.sql("create external table spark_retail_dim.hive_products (productcode string,productname string,productline string,productscale string,productvendor string,productdescription string,quantityinstock int,buyprice decimal(10,2),msrp decimal(10,2),upddt date,profit decimal(10,2),profper decimal(10,2),promoind smallint,demandind smallint) row format delimited fields terminated by ',' location '/user/hduser/sparkpro/hive_prod'")
        sess.sql("insert into spark_retail_dim.hive_products select * from completetable")
        return df_prod_new_udf
    elif tblflag == True:
        print("table exists")
        exis_tbl = readhivetbl(sess, tbl)
        exis_tbl.write.format("delta").mode('overwrite').save('/user/hduser/df_hive_exist')
        exist_df = DeltaTable.forPath(sess, '/user/hduser/df_hie_exist')
        exist_df.alias("exist").merge(source=df_prod_new_udf.alias("new"),
                                      condition=expr("new.productCode=exist.productCode")).whenMatchedUpdate(
            set={"productName": col("new.productName"), "productLine": col("new.productLine"), "productScale": col("new.productScale"),
                 "productVendor": col("new.productVendor"), "productDescription": col("new.productDescription"),
                 "quantityInStock": col("new.quantityInStock"), "buyPrice": col("new.buyPrice"), "MSRP": col("new.MSRP"),
                 "upddt": col("new.upddt"), "profit": col("new.profit"), "profper": col("new.profper"),
                 "promoind": col("new.promoind"), "demandind": col("new.demandind")}).whenNotmatchedInsert(
            values={"productCode": col("new.productCode"), "productName": col("new.productName"), "productLine": col("new.productLine"),
                    "productScale": col("new.productScale"), "productVendor": col("new.productVendor"),
                    "productDescription": col("new.productDescription"), "quantityInStock": col("new.quantityInStock"),
                    "buyPrice": col("new.buyPrice"), "MSRP": col("new.MSRP"), "upddt": col("new.upddt"),
                    "profit": col("new.profit"), "profper": col("new.profper"), "promoind": col("new.promoind"),
                    "demandind": col("new.demandind")}).execute()
        complete_df = exist_df.toDF()
        complete_df.createTempTable("completetable")
        sess.sql("drop table if exists spark_retail_dim.hive_products")
        sess.sql(""" create table if not exists spark_retail_dim.hive_products (productcode string,productname string,
        productline string, productscale string, productvendor string, productdescription string,quantityinstock int,buyprice decimal(10,2),msrp decimal(10,2),upddt date,profit decimal(10,2),profper decimal(10,2),promoind smallint,demandind smallint )""")
        sess.sql(""" insert overwrite table spark_retail_dim.hive_products select * from completeTable""")
        return complete_df


ordprodquery = """(select o.customernumber,o.ordernumber,o.orderdate,o.shippeddate,o.status,o.comments,
      od.quantityordered,od.priceeach,od.orderlinenumber,p.productCode,p.productName,
      p.productLine,p.productScale,p.productVendor,p.productDescription,p.quantityInStock,p.buyPrice,p.MSRP  
      from orders o inner join orderdetails od 
      on o.ordernumber=od.ordernumber  
      inner join products p 
      on od.productCode=p.productCode 
      and year(o.orderdate)=2022 
      and month(o.orderdate)=10 ) ordprod"""


def wrangle(df_product_merged,df_orderdetails):
    df = df_product_merged.alias("p").join(df_orderdetails.alias("o"), on="productCode", how="inner")
    if __name__ == '__main__':
        df = df.withColumn(row_number().over(Window.orderBy(desc("orderdate"))).alias("rno")).select("p.MSRP",
                                                                                                     "p.buyPrice",
                                                                                                     "o.comments",
                                                                                                     "o.customernumber",
                                                                                                     "o.orderdate",
                                                                                                     "o.orderlinenumber",
                                                                                                     "o.ordernumber",
                                                                                                     "o.priceeach",
                                                                                                     "p.productCode",
                                                                                                     "p.productDescription",
                                                                                                     "p.productLine",
                                                                                                     "p.productName",
                                                                                                     "p.productScale",
                                                                                                     "p.productVendor",
                                                                                                     "p.quantityInStock",
                                                                                                     "o.quantityordered",
                                                                                                     "o.shippeddate",
                                                                                                     "o.status")
    return df


def profpromo(cp, sp, qty):
    profit = sp - cp
    profper = (profit / cp) * 100
    if profit > 0:
        profind = 1
    else:
        profind = 0
    if qty > 0 and qty < 1500 and profper > 50.0:
        demandind = 1
    else:
        demandind = 0
    if qty > 1500 and profper > 20.0:
        promoind = 1
    else:
        promoind = 0
    return profit, profind, demandind, promoind