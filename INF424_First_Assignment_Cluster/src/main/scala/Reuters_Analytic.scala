import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem
object Reuters_Analytic extends App {

  //Starting our SparkSession (LOCAL MODE ONLY - UNCOMMENT TO RUN LOCALLY
  //  val spark = SparkSession.builder
  //    .master("local[*]") //local[*] means "use as many threads as the number of processors available to the Java virtual machine"
  //    .appName("ex1")
  //    .getOrCreate()

  //Starting our SparkSession (CLUSTER MODE ONLY)
  val spark = SparkSession.builder
    .appName("Reuters_Analytic")
    .master("yarn")
    .config("spark.hadoop.fs.defaultFS", "hdfs://clu01.softnet.tuc.gr:8020")
    .config("spark.hadoop.yarn.resourcemanager.hostname", "http://clu01.softnet.tuc.gr:8189")
    .config("spark.hadoop.yarn.application.classpath", "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
    .getOrCreate()

  val hdfsURI = "hdfs://clu01.softnet.tuc.gr:8020"
  FileSystem.setDefaultUri(spark.sparkContext.hadoopConfiguration, hdfsURI)
  val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  //PATHS FOR LOCAL MODE (UNCOMMENT TO RUN LOCALLY)

  //  //Paths to our hdfs files
  //  val pathCategoriesPerDocument = "hdfs://localhost:9000/reuters_data/rcv1-v2.topics.qrels"
  //  val pathTermsInDocuments = "hdfs://localhost:9000/reuters_data/lyrl"
  //  val pathTermsToStems =  "hdfs://localhost:9000/reuters_data/stem.termid.idf.map.txt"
  //  //Our output path
  //  val outputPath = "hdfs://localhost:9000/reuters_data/output_files"

  //PATHS FOR CLUSTER MODE
  val pathCategoriesPerDocument = "/user/chrisa/Reuters/rcv1-v2.topics.qrels"
  val pathTermsInDocuments = "/user/chrisa/Reuters/lyrl2004_vectors*"
  val pathTermsToStems =  "/user/chrisa/Reuters/stem.termid.idf.map.txt"
  //Our output path
  val outputPath = "/user/fp5/Reuters/output_files"

  val docsPerCategoryCardinality = spark.sparkContext.textFile(pathCategoriesPerDocument)
    .map(line => {
      val fields = line.split("\\s+") //Split whenever one or more whitespaces are found
      (fields(1),fields(0))//(docID,categoryID)
    })

  val docsCountsPerCategory = docsPerCategoryCardinality
    .map(x=>(x._2,1)) //Create a tuple with the categoryID and 1
    .reduceByKey(_ + _) //So that we can count the number of documents per category as we did in Class

  //docsCountsPerCategory.foreach(println)

  val termsInDoc =  spark.sparkContext.textFile(pathTermsInDocuments)
    .flatMap(line => {
      val line1 = line.split("\\s+", 2) //Split the line into document ID and term IDs
      val docID = line1(0) //This is our doc ID
      val termIDs = line1(1).split("\\s+").map(_.split(":")(0)) //Extract term IDs, and ignore the weights that our not needed for our analysis
      termIDs.map(termID => (docID,termID)) //We return a tuple with the docID and the termID
    })

  val docsCountsInTerms = termsInDoc
    .map(x=>(x._2,1)) //Create a tuple with the termID and 1
    .reduceByKey(_ + _) //So that we can count the number of documents per term as we did in Class

  // docsCountsInTerms.foreach(println)

  val intersectionCardinality = termsInDoc
    .join(docsPerCategoryCardinality) //Join the terms with the categories, with our key being the docID
    .map { case (docID,(termID,categoryID)) => ((termID, categoryID),1) } //Create tuples with the termID and categoryID and 1
    .reduceByKey(_ + _) //So that we can count the number of documents per unique combination of term-category as we did in Class

  //  intersectionCardinality.foreach(println)

  val termToStem = spark.sparkContext.textFile(pathTermsToStems) //Reading the stem file
    .map(line => {
      val fields = line.split("\\s+") //Split depending on whitespaces
      (fields(1), fields(0)) //termID,stem
    })

  //termToStem.foreach(println)

  val jaccardIndex = intersectionCardinality.map { case ((term, categoryID), intersectionCount) =>
    (term, (categoryID, intersectionCount)) //Transform our intersection so that we can join it with the other RDDs, using term as the key
  }.join(docsCountsInTerms).map { case (term, ((category, intersectionCount), termDocCount)) =>
    (category, (term, intersectionCount, termDocCount)) //Transform our RDD so that we can join it with the other RDDs, using category as the key
  }.join(docsCountsPerCategory).map { case (category, ((term, intersectionCount, termDocCount), categoryDocCount)) =>
    val unionCardinality = termDocCount + categoryDocCount - intersectionCount //The union cardinality formula
    val jaccardIdx = Option(unionCardinality).filter(_ != 0).map(uc => intersectionCount.toDouble / uc).getOrElse(0.0) //The Jaccard Index formula
    (category, term, jaccardIdx) //Return the category, term and Jaccard Index
  }

  val jaccardIndexFinal = jaccardIndex /*Here we join the jaccard indices for each term-category pair,
                                        with the respective stem so that we can later output the results with the format <category name>; <stemid>; <jaccard index>*/
    .map { case (categoryID,termID, jaccardIndex) => (termID, (categoryID, jaccardIndex)) }
    .join(termToStem)
    .map { case (termID, ((categoryID, jaccardIndex), stem)) => (categoryID, stem, jaccardIndex) }

  //  jaccardIndexFinal.foreach(println)

  jaccardIndexFinal.map { case (category, stem, jaccardIdx) =>
    s"$category;$stem;$jaccardIdx"
  }.saveAsTextFile(outputPath) //Save the results in the output path

  spark.stop()
}