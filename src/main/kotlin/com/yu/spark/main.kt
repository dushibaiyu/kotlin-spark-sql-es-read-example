package com.yu.spark

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession;
import java.time.*; // 引入java.time 模块下的所有符号

fun main(args:Array<String>)
{
    val master  = if(args.isNotEmpty()) args[0] else "local" ;
    val conf = SparkConf()
            .setMaster(master)
            .setAppName("Kotlin Spark ES Test")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "10.1.11.42");
    conf.set("es.port", "9200");
    conf.set("pushdown", "true");

    val nowTime = System.currentTimeMillis() / 1000L; //当前Unix time时间戳
    val start = LocalDate.now().atTime(0,0).toEpochSecond(ZoneOffset.UTC); // 当天开始的UNix时间戳

    val sql  = SparkSession.Builder().config(conf).getOrCreate().sqlContext();
    var df  = sql.read().format("org.elasticsearch.spark.sql").load("ptlog_paios_power/85bfac7251c93e16b7b946ea5eded05d"); // 从es获取数据，spark sql接口
    var playlist   = df.filter(df.col("access").equalTo("wifi").and(df.col("create_time").lt(nowTime)).and(df.col("create_time").gt(start))); // 过滤条件

    playlist.show(5); // 显示获取的前5条数据， 这儿才执行spark job的，上面过滤一类的都是懒加载的

    val frist = playlist.first(); // 这儿同样是一个任务，获取第一条

    val tv = frist.getStruct(frist.fieldIndex("open_data")); // 获取 open_data 的数据，格式是json，可以还当作row去获取

    println("tv = $tv");

    val packageName = tv.getString(tv.fieldIndex("packageName")); // 获取子json里面的数据

    println("tv = $packageName");
}
