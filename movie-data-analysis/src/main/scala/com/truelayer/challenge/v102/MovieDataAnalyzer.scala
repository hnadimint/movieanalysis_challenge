package com.truelayer.challenge.v102

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  * IMDB movie data analysis
  */
object MovieDataAnalyzer {

  def writeToDB(df: DataFrame): Unit = {
    val connProps = new Properties()
    connProps.put("user", "postgres")

    df.coalesce(1)
      .write
      .mode("append")
      //.option("truncate", "true")
      .jdbc("jdbc:postgresql://localhost:5432/postgres",
        "imdb_movie_analysis", connProps)
  }

  def getMovieDetail(moviesDF: DataFrame, wikiDF: DataFrame): DataFrame = {
    // prepare join key column using title of the movie in imdb
    val moviesJoinDF = moviesDF.withColumn("join_key",
      lower(regexp_replace(concat(lit("wikipedia:"), col("title")),
      "\\s+", "")))

    // prepare join key column using title of the movie in the wiki
    val wikiJoinDF = wikiDF.withColumn("join_key",
      lower(regexp_replace(col("title"), "\\s+", "")))
        .withColumnRenamed("title", "w_title")

    // match both the datasets based on the join_key created from title
    wikiJoinDF.join(broadcast(moviesJoinDF), Seq("join_key"), "inner")
  }

  def computeMovieEfficiencyRatio(df: DataFrame): DataFrame = {
    val cleanDF = df.withColumn("budget", col("budget").cast("double"))
      //there can't be a movie with zero budget
      //there shouldn't be a movie with imdb_id null in the data
      .where("imdb_id is not null and budget > 0")

    //populate ratio as -1 when revenue is 0
    val ratioDF = cleanDF.withColumn("ratio",
      when(col("revenue") > 0,
        round(col("budget")/col("revenue"),4))
        .otherwise(-1)
    )
    ratioDF
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName(getClass.getName)
      .master("local[*]")
      .getOrCreate()

    // overriding the log level to ERROR
    //spark.sparkContext.setLogLevel("ERROR")

    // load the movies metadata
    val moviesDF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("escape", "\"")
      .option("multiLine", true)
      .csv(args(0))
       
    // load the wikipedia abstract dataset
    val wikipediaDF = spark.read.format("xml")
      .option("rowTag", "doc")
      .load(args(1))

    // 1. For each film, calculate the ratio of budget to revenue
    // compute the ratio for every valid movie record
    val ratioDF = computeMovieEfficiencyRatio(moviesDF)
    //ratioDF.show()
    //println(ratioDF.count())

    // 2. Match each movie in the IMDB dataset
    // with its corresponding Wikipedia page
    val matchDF = getMovieDetail(ratioDF, wikipediaDF)
        .drop("join_key")
    //matchDF.printSchema()
    //matchDF.show
    //println(matchDF.count())

    val outDF = matchDF.selectExpr("title", "budget",
      "year(release_date) as year", "revenue", "vote_average as rating",
      "ratio", "production_companies", "url as wikipedia_link",
      "abstract as wikipedia_abstract")
      .orderBy(desc("ratio"))
      .limit(1000)

    // 3. Load the top 1000 movies (with highest ratio)
    // into a Postgres database
    writeToDB(outDF)
  }

}
