# movieanalysis_challenge

Tool/Framework used:
I have used Spark with Scala framework to implement this challenge as the wikipedia abstract data is huge and considering the scalability support as well.

How to run this:
1) Any linux/windows system with Spark and PostgreSQL installed
2) Clone the project and run sbt build to create the jar file
3) Use spark-submit to run the application using the above jar in the cluster mode with the following application arguments
   a. IMDB movies csv data set
   b. wikipedia abstract xml data set
4) Post the completion of the application, query the data in PostgreSQL using psql interface.

How to query data:
Launch psql shell to access the default database postgres, run the query to get the results
select * from imdb_movie_analysis order by create_dt desc limit 1000

There are lot of To-Do's or things to improve on:
1) Regarding the data quality of the Imdb movies dataset,
The values populated for the budget and the revenue columns are not consistent. For example, for the movie "Chasing Liberty" the wikipedia page (https://en.wikipedia.org/wiki/Chasing_Liberty) shows the budget as 23 million dollars and revenue as 12 million dollars but in the csv dataset, budget is populated as 23000000 where as revenue is populated as just 12, because of which the ratio doesn't seem to be accurate.
Same scenario repeats for most of the movies in the dataset.







