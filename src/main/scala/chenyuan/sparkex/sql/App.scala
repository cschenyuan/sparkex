package chenyuan.sparkex.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.functions._

/**
 * @author chenyuan
 */
object App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("test")
      .master("local")
      .getOrCreate()

    import spark.implicits._

    val mysqlOps = Map(
      "url" -> "jdbc:mysql://localhost/sparkex",
      "user" -> "root",
      "password" -> "11116666"
    )

    val userDf = spark.read
      .format("jdbc")
      .options(mysqlOps)
      .option("dbtable", "users")
      .load()

    val jiaoyiDf = spark.read
      .format("jdbc")
      .options(mysqlOps)
      .option("dbtable", "jiaoyi")
      .load()

    val users = userDf.select("name", "age", "userId")
      .filter($"age" < 30)
      .filter($"gender".isin("M"))

    val result = jiaoyiDf.select("price", "volume", "userId")
      .join(users, Seq("userId"), "inner")
      .groupBy(("name"), ("age"))
      .agg(sum(col("price") * col("volume")).alias("revenue"))

    userDf.createTempView("users")

    Project

  }

}
