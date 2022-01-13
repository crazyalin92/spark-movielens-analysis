
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{collect_list, struct, to_json}
import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}

object MovieLens {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val movieFile = args(0);
    val ratingFile = args(1);


    val spark = SparkSession.builder
      .appName("spark-movielens-analysis")
      .master("local[*]")
      .getOrCreate();

    //1. Прочитать файл c фильмами
    val movieSchema = StructType(Array(
      StructField("movieId", IntegerType, true),
      StructField("movieTitle", StringType, true),
      StructField("releaseDate", DateType, true),
      StructField("videoReleaseDate", DateType, true),
      StructField("imdbUrl", StringType, true),
      StructField("unknown ", IntegerType, true),
      StructField("action ", IntegerType, true),
      StructField("adventure ", IntegerType, true),
      StructField("animation ", IntegerType, true),
      StructField("childrens", IntegerType, true),
      StructField("comedy", IntegerType, true),
      StructField("crime", IntegerType, true),
      StructField("documentary", IntegerType, true),
      StructField("drama", IntegerType, true),
      StructField("fantasy", IntegerType, true),
      StructField("filmNoir", IntegerType, true),
      StructField("horror", IntegerType, true),
      StructField("musical", IntegerType, true),
      StructField("mystery", IntegerType, true),
      StructField("romance", IntegerType, true),
      StructField("sciFi", IntegerType, true),
      StructField("thriller", IntegerType, true),
      StructField("war", IntegerType, true),
      StructField("western", IntegerType, true)
    ))

    val movies = spark.read
      .option("header", "false")
      .option("delimiter", "|")
      .option("treatEmptyValuesAsNulls", "true")
      .option("dateFormat", "dd-MMM-yyyy")
      .schema(movieSchema)
      .csv(movieFile)

    movies.show(100);
    movies.printSchema();

    //2. прочитать файл с рейтингом
    val ratingSchema = StructType(Array(
      StructField("userId", IntegerType, true),
      StructField("movieId", IntegerType, true),
      StructField("rating", IntegerType, true),
      StructField("timestamp", IntegerType, true)
    ))

    val ratings = spark.read
      .option("header", "false")
      .option("delimiter", "\t")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(ratingSchema)
      .csv(ratingFile)

    ratings.show(100);
    ratings.printSchema();

    //Сделать словарь фильмов
    val moviesHash = movies
      .select("movieId", "movieTitle")
      .rdd.map(r => (r.getInt(0), r.getString(1)))
      .collect().toMap

    val allRatings = ratings.groupBy("rating")
      .count()
      .orderBy("rating")

    allRatings.show(false)

    val allRatingsAgg = allRatings.agg(collect_list("count").as("hist_all"))

    allRatingsAgg.show()

    val movieRating = ratings.where("movieId = 32")
      .groupBy("rating")
      .count()
      .orderBy("rating")

    movieRating.show(false)

    val movieName = moviesHash(32)

    val movieRatingAgg = movieRating.agg(collect_list("count").as(movieName))
    movieRatingAgg.show()

    movieRatingAgg.join(allRatingsAgg)
      .write.mode(SaveMode.Overwrite)
      .json("./output/result.json")

    spark.stop();
  }
}