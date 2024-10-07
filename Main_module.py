# Databricks notebook source
# MAGIC %run /Users/harivigneshnatarajan@gmail.com/Spark_Project/Common_func

# COMMAND ----------

# MAGIC %run /Users/harivigneshnatarajan@gmail.com/Spark_Project/ETL_Process

# COMMAND ----------

from delta import *
from pyspark.sql.functions import *

def main(arg1_conn_file,arg2_jar_file,arg3_sparksess):
    print("### Create spark session ###")
    spark=sess_spark('Delta',arg3_sparksess,arg2_jar_file)
    spark.sparkContext.setLogLevel("ERROR")

    spark.sql("drop database if exists spark_retail_curated cascade")
    spark.sql("create database spark_retail_curated")
    spark.sql("drop database if exists spark_retail_discovery cascade")
    spark.sql("create database spark_retail_discovery")
    spark.sql("drop database if exists spark_retail_dim cascade")
    spark.sql("create database spark_retail_dim")

    print("##### Employees Data (Slowly Changing Dimension 2) #####")
    print("##### Select only the employees info updated or inserted today in the source DB #####")
    emp_query="(select * from employees where upddt > current_date - interval 1 day) tblquery"
    df_new_emp=getRDBMS(spark,arg1_conn_file,'empoffice',emp_query)
    print("Employee RDBMS Data")
    df_new_emp.show(5)
    print("Data Munging")
    df_new_emp=munge(df_new_emp,True,True,False,False,False,"")
    print("Check for Hive Table")
    tblflag=check_hive_tbl(spark,"spark_retail_dim","hive_employees")
    print(tblflag)
    print("SCD2 data load to hive table spark_retail_dim.hive_employees")
    writedatascd2(tblflag,spark,"spark_retail_dim.hive_employees",df_new_emp)

    print("###### Select entire Office RDBMS Data #####")
    df_offices=getRDBMS(spark,arg1_conn_file,'empoffice',"offices")
    print("Data Munging")
    df_offices=munge(df_offices,True,True,False,False,False,"")
    print("Overwrite Hive table spark_retail_dim.offices_raw")
    writehivetbl(df_offices,"spark_retail_dim.offices_raw",False,"",'overwrite')

    print("##### Select joined Customer & Payments Data (Apply Partition and Pushdown Optimization) #####")
    custpayment_query="""(select c.customerNumber as customernumber,upper(c.customerName) as custname,c.country,c.salesRepEmployeeNumber,c.creditLimit,p.checkNumber,p.paymentDate,p.amount,current_date as datadt from customers c inner join payments p on c.customernumber=p.customernumber and year(p.paymentDate)=2022 and month(paymentDate)>=07) query"""

    df_custpayments=getRDBMSpart(spark,arg1_conn_file,'custpayments',custpayment_query,'customerNumber',1,100,4)
    print("##### optimize_performance of the DF #####")
    df_custpayments=optimize(spark,df_custpayments,4,True,True,10)
    print("Overwrite Hive table retail_curated.custpayments")
    writehivetbl(df_custpayments,"spark_retail_curated.custpayments",True,'datadt','overwrite')
    spark.sql("select * from spark_retail_curated.custpayments").show()
    df_custpayments.createOrReplaceTempView("custpayments")

    print("##### Orders & orderdetails Data #####")
    orderdetails_query = """(select o.customernumber, o.ordernumber, o.orderdate, o.shippeddate, o.status, o.comments,od.quantityordered, od.priceeach, od.orderlinenumber, od.productCode, current_date as datadt from orders o inner join orderdetails od on o.ordernumber = od.ordernumber where year(o.orderdate) = 2022 and month(o.orderdate) >= 07) orders"""

    df_orderdetails=getRDBMSpart(spark,arg1_conn_file,'ordersproducts',orderdetails_query,"customerNumber",1,100,4)
    df_orderdetails=optimize(spark,df_orderdetails,5,True,True,10)
    df_orderdetails.show(2)
    writehivetbl(df_orderdetails,"spark_retail_curated.orderdetails",True,'datadt','overwrite')

    print("##### Products Data (Apply Delta lake merge to update if products exists else insert) #####")
    print("##### Product Profit value, Profit percent, promotion indicator and demand indicator KPI Metrics #####")
    prodquery="(select * from products where upddt > current_date - interval 1 day) query"
    df_prod_new=getRDBMS(spark,arg1_conn_file,'ordersproducts',prodquery)
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StructType,StructField,DecimalType,ShortType
    struct=StructType([StructField('profit',DecimalType(10,2)),StructField('profper',DecimalType(10,2)),StructField('promoind',ShortType(),True),StructField('demandind',ShortType(),True)])
    udfprofpromo=udf(profpromo,struct)
    df_prod_new_udf=df_prod_new.select("productCode","productName","productLine","productScale","productVendor","productDescription","quantityInStock","buyPrice","MSRP","upddt",udfprofpromo(col("buyPrice"),col("MSRP"),col("quantityInStock")).alias("profpromo"))
    df_prod_new_udf.printSchema()
    df_prod_new_udf=df_prod_new_udf.select("productCode","productName","productLine","productScale","productVendor","productDescription","quantityInStock","buyPrice","MSRP","upddt","profpromo.profit","profpromo.profper","profpromo.promoind","profpromo.demandind")
    print("UDF applied Products with Profit ratio, profit percent, promo indicator and demand indicator")
    df_prod_new_udf.show(5)
    tblflag=check_hive_tbl(spark,"spark_retail_dim","hive_products")
    print(tblflag)
    print("(Apply Delta lake merge to update if products exists else insert into retail_dim.hive_products)")
    df_product_merged = writedataDeltaMerge(tblflag,spark,"spark_retail_dim.hive_products",df_prod_new_udf)
    df_product_merged = spark.read.format("delta").table("spark_retail_dim.hive_products")

    print("##### Data Wrangling i.e., joining of merged latest products with the orders #####")
    df_orders_products=wrangle(df_product_merged,df_orderdetails)
    df_orders_products.show(5,False)
    print("Overwrite Hive table retail_curated.ordersproducts")
    writehivetbl(df_orders_products,'spark_retail_curated.ordersproducts',True,'datadt','overwrite')

    print("##### Custnavigation nested json Data parsing with custom schema #####")
    cus_struct=StructType([StructField("id",StringType(),False),StructField("comments",StringType(),True),StructField("pagevisit",ArrayType(StringType()),True)])

    print("Read Cust navigation json data")
    df_cust_nav=read_data(spark,"json","file:///home/hduser/PysparkRetailDatalakeOnpremProject_2_2024/cust_navigation.json",cus_struct)
    df_cust_nav.createOrReplaceTempView("cust_navigation")
    df_cust_nav.show(2)

    print("##### Customer Navigation Curation load to hive with positional explode ######")
    print("Create hive external table using hql, insert into the table and write the curated data into json")
    spark.sql("create external table if not exists spark_retail_curated.cust_navigation (customernumber string,navigation_idx int,navigation_pg string) row format delimited fields terminated by ',' location '/user/hduser/sparkpro/custnavigation/'")
    spark.sql("""insert into table spark_retail_curated.cust_navigation select id,pgnavigationidx,pgnavigation from cust_navigation lateral view posexplode(pagevisit) rename1 pgnavigationidx,pgnavigation""")
    spark.sql("""select id,pgnavigationidx,pgnavigation from cust_navigation lateral view posexplode(pagevisit) rename1 pgnavigationidx,pgnavigation""").show(4,False)

    report1=spark.sql("select c1.navigation_pg,count(distinct(c1.customernumber)) as custcnt,'last pagevisited' as pagevisit from spark_retail_curated.cust_navigation c1 inner join (select a.customernumber,max(a.navigation_idx) as max_navigation from spark_retail_curated.cust_navigation a group by a.customernumber) as c2 on (c1.customernumber = c2.customernumber and c1.navigation_idx = c2.max_navigation) group by c1.navigation_pg union all select navigation_pg,count(distinct customernumber) as custcnt,'first pagevisited' as pagevisit from spark_retail_curated.cust_navigation where navigation_idx = 0 group by navigation_pg")
    curdt=spark.sql("select date_format(current_date(),'yyyyMMdd')").first()[0]
    write_data("json",report1.coalesce(1),"hdfs://localhost:54310/user/hduser/retail_discovery/first_last_page"+curdt,"overwrite")
    report1.show(5,False)


    print("##### Dim Order table External table load using load command #####")
    spark.sql("create external table spark_retail_discovery.dim_order_rate (rid int,orddesc string,comp_cust string,siverity int,intent string) row format delimited fields terminated by ',' location '/user/hduser/sparkpro/dimorders/'")

    spark.sql("load data local inpath 'file:///home/hduser/PysparkRetailDatalakeOnpremProject_2_2024/orders_rate.csv' overwrite into table spark_retail_discovery.dim_order_rate")

    print("##### Employee Rewards Discovery load #####")
    df_offices.createOrReplaceTempView("offices_raw_view")
    spark.sql("select o.*,e.*,c.* from offices_raw_view o inner join spark_retail_dim.hive_employees e inner join custpayments c on o.officecode = e.offCode and e.empnum = c.salesrepemployeenumber").createOrReplaceTempView("office_emp_custpayments")

    df_emp_rewards = spark.sql("select * from (select email,sum_amt,rank() over(partition by state order by sum_amt desc) as rnk,current_date as datadt from (select email,state,sum(amount) as sum_amt from office_emp_custpayments where datadt>=date_add(current_date,0) group by email,state) temp) temp2 where rnk = 1")
    df_emp_rewards.show(4,False)
    print("Overwrite Hive table spark_retail_discovery.employeerewards to send the rewards to high performing employees")
    writehivetbl(df_emp_rewards,'spark_retail_discovery.employeerewards',True,'datadt','overwrite')


    print("##### Customer Frustration Discovery joining cust_navigation and spark_retail_dim.dim_order_rate tables #####")
    frustrated_df=spark.sql("select customernumber,total_siverity,case when total_siverity between -10 and -3 then 'Hightly Frustrated' when total_siverity between-2 and -1 then 'Low_Frustrated' when total_siverity = 0 then 'Nuetral' when total_siverity between 1 and 2 then 'Happy' when total_siverity between 3 and 10 then 'Overwhelming' else 'Unknown' end as cust_frustration_level from (select customernumber,sum(siverity) as total_siverity from (select c.id as customernumber,c.comments,r.orddesc,r.siverity from cust_navigation c left outer join spark_retail_discovery.dim_order_rate r where c.comments like concat('%',r.orddesc,'%')) temp1 group by customernumber) temp2")

    frustrated_df.show(5,False)

    print("##### Convert DF to frustrationview tempview #####")
    frustrated_df.createOrReplaceTempView("frustrationview")

    print("Overwrite Hive table spark_retail_discovery.cust_frustration_level to store the customer frustration levels")
    spark.sql("create external table if not exists spark_retail_discovery.cust_frustration_level (customernumber string,total_siverity int,customer_frustration_level string) row format delimited fields terminated by ',' location '/user/hduser/sparkpro/custmartfrustration/'")

    print("Writing Customer frustration data into Hive")
    spark.sql("insert overwrite table spark_retail_discovery.cust_frustration_level select * from frustrationview")

    print("Writing Customer frustration data into RDBMS")
    writeRDBMS(frustrated_df,arg1_conn_file,"spark_project","customer_frustration_level","overwrite")
    print("Writing Customer frustration data into S3 Bucket Location")
    sc = spark.sparkContext
    print("##### Application completed successfully #####")


if __name__ == '__main__':
    print("##### Starting the main application #####")
    import sys
    #if len(sys.argv)==4:
    if len(sys.argv)==3:
        print("##### Initializing the Dependent modules #####")
        #from Common_func import read_data,sess_spark,getRDBMSpart,getRDBMS,check_hive_tbl,munge,writehivetbl,write_data,writeRDBMS,optimize
        #from ETL_Process import writedatascd2,writedataDeltaMerge,wrangle,profpromo
        from pyspark.sql.types import *
        #print("Calling the main method with {0}, {1}, {2},{3}".format(sys.argv[0],sys.argv[1],sys.argv[2],sys.argv[3]))
        main(sys.argv[0],sys.argv[1],sys.argv[2])

# COMMAND ----------

main('dbfs:/FileStore/config/connection.prop','dbfs:/FileStore/config/spark_jars','dbfs:/FileStore/config/app_properties')
