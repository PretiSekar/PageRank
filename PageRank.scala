// Databricks notebook source
//Loading the data into the variable
val webcrawl= sc.textFile("/FileStore/tables/webcrawl.txt")
webcrawl.take(10)

// COMMAND ----------

//Get the link and its neighbouring links in the form of Link and Array of Links
val links = webcrawl.map { s =>
  val result = s.split("\\s+") 
   (result(0),result(2).replaceAll("\\(|\\{|\\}|\\)","").split(","))}
links.take(10)

// COMMAND ----------

//Assigning the page ranks to be 1.0 initially
var ranks = links.mapValues(v => 1.0)
ranks.take(10)

// COMMAND ----------

//For temporary usage
val temp = webcrawl.map { s =>
  val result = s.split("\\s+") 
   (result(0),result(2))}
//Run upto 100 iterations
for (i <- 1 until 100) {
  val contributions = links.join(ranks).flatMap {
    case (pageId, (temp, rank)) =>
      temp.map(dest => (dest, rank / temp.size))
  }
  // Calculating the page rank based on the original formula
  // If random surfer factor  was included using v => 0.15 + 0.85*v .But since the formula does't require it just leavin it as is after reduce
  ranks = contributions.reduceByKey((x, y) => x + y).mapValues(v => v)
}

// COMMAND ----------

//The final answer
ranks.sortBy(-_._2).take(10)

// COMMAND ----------


