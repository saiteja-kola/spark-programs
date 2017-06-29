/**
  * Created by skola on 6/28/2017.
  */

package com.sparkprograms
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import com.databricks.spark.xml



object BestBuy {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Best_Buy")

    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)

    val df_review= hiveContext.read.format("com.databricks.spark.xml").option("rowTag", "review").load("file:///home/cloudera/Downloads/product_data/reviews/*.xml")

    hiveContext.sql(s"drop table if exists reviewsdata purge")

    df_review.write.saveAsTable("reviewsdata")

    hiveContext.sql(s"drop table if exists reviews_data purge")

    hiveContext.sql(s"create table reviews_data as select * from reviewsdata")

    val df_category= hiveContext.read.format("com.databricks.spark.xml").option("rowTag", "category").load("file:///home/cloudera/Downloads/product_data/categories/*.xml")

    hiveContext.sql(s"drop table if exists categorydata purge")

    df_category.write.saveAsTable("categorydata")

    hiveContext.sql(s"drop table if exists category_data purge")

    hiveContext.sql(s"create table category_data as select * from categorydata")

    val df_product= hiveContext.read.format("com.databricks.spark.xml").option("rowTag", "product").load("file:///home/cloudera/Downloads/product_data/products/products_0001_2570_to_430420.xml")

    hiveContext.sql(s"drop table if exists productdata purge")

    df_product.write.saveAsTable("productdata")

    hiveContext.sql(s"drop table if exists product_data purge")

    hiveContext.sql(s"create table product_data as select * from productdata")

    hiveContext.sql(s"alter table product_data change `cast` casting struct<member:array<struct<name:string,role:string>>>")

    hiveContext.sql(s"alter table product_data change departmentid dept_id bigint")

    hiveContext.setConf("hive.exec.dynamic.partition", "true")

    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

    hiveContext.sql(s"drop table if exists reviews_data_partition purge")

    hiveContext.sql(s"create table reviews_data_partition (aboutme struct<customerType:string>, comment string, id bigint,  reviewer struct<name:string>, sku bigint, submissiontime string, title string, createdtime timestamp) partitioned By (rating double) row format delimited fields terminated by ','")

    hiveContext.sql(s"insert overwrite table reviews_data_partition partition (rating) select aboutme,comment,id,reviewer,sku,submissiontime,title,current_timestamp,rating from reviews_data")

    hiveContext.setConf("set hive.enforce.bucketing","true")

    hiveContext.sql(s"drop table if exists category_data_partition purge")

    hiveContext.sql(s"create table category_data_partition (id string, name string, createdtime timestamp) clustered by (name) into 8 buckets row format delimited fields terminated by ','")

    hiveContext.sql(s"insert into table category_data_partition  select id,name,current_timestamp from category_data")

    val (extTable, finalTable) = ("product_data","product_data_partition")

    var schDF = hiveContext.table(extTable)

   // hiveContext.sql("set hive.exec.parallel=true")
   // hiveContext.sql("set hive.exec.dynamic.partition=true")
   // hiveContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
   // hiveContext.sql("set hive.optimize.sort.dynamic.partition=true")


    hiveContext.sql(s"drop table if exists $finalTable purge")

    var cols = schDF.toString().replace(": ", " ").replace("[", "("). replace("]", ", createdtime timestamp)").replace(" genre string,","").
      replace(" department string,","").replace(" new boolean,","")


    var column = schDF.columns.toList.mkString(",").replace("genre,","").replace("department,","").replace("new,","")

    val partitionCol = "(genre string,new boolean,department string)"

    val partitionColWithOutdType = "(genre,new,department)"


    hiveContext.sql(s"create table $finalTable $cols partitioned by $partitionCol row format delimited fields terminated by ','")
    hiveContext.sql(s"insert overwrite table $finalTable partition $partitionColWithOutdType select $column,current_timestamp,genre,new,department from $extTable")


// spark-submit --packages com.databricks:spark-xml_2.10:0.4.0 --master yarn --class com.sparkprograms.BestBuy spark-programs_2.10-1.0.jar


  }
}