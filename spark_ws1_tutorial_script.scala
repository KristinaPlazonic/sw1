sc   // SparkContext, available in spark-shell
sc.version
sc.parallelize( List(1,2,3,4) )  // create a toy RDD
val tdd = sc.textFile("datasets/twitter_corpora.txt") //result is an RDD[Strings]

val rdd = sc.parallelize( List(1, 2, 3, 4, 10, 9, 8, 7) )   // save the toy dataset in a value "rdd"
val lines = sc.textFile("file://datasets/shakespeare.txt")  //another, unclean text file

lines.take(10).mkString("\n")  // just like Scala "take"; mkString is another handy function

import scala.util.matching._   //regex
// to clean the dataset for counting words: 
// 1. drop all non-letters but keep spaces
// 2. trim() - remove spaces from ends of strings
// 3. change to lowercase
// 4. drop empty lines 
val cleanlines = (lines map { _.replaceAll("[^A-Za-z ]", "").trim().toLowerCase() } filter { _ != "" } )
cleanlines take 10 foreach println  //first 10 lines of the cleaned up text file

// Word count in Spark
val wc = (cleanlines
  .flatMap( _.split(" ") )
  .map( w => (w,1) )
  .reduceByKey( _+_ )
)
wc.take(10).mkString("\n")   // take a look at how the result looks like

wc.cache()  // to cache wc in memory; executed only when an action is taken
wc.take(10).mkString("\n")  // should be much faster than the previous execution


// example sorting a key-value RDD - note ascending or descending order; custom orderings
println("               sortByKey default")
wc.sortByKey() take 5 foreach println
println("               swapping key and value")
wc.map( pair => pair.swap ).sortByKey(false) take 5 foreach println
println("               takeOrdered, custom Ordering")
wc.takeOrdered(5)( Ordering.by( - _._2) ) foreach println
println("               top, default (i.e. key)")
wc.top(5) foreach println
println("               top by ordering on the second place of the key-value pair")
wc.top(5)( Ordering.by( _._2)) foreach println

// examples of join and cogroup
// especially if some keys are repeated
// join and cogroup have different return types
// 'collect' is a function that returns results from all machines to the driver - it's an action, forces evaluation
//
val rdd1 = sc.parallelize( List( 1->2, 1->3, 2->4, 2->5  ))
val rdd2 = sc.parallelize( List( 1->22, 1->23, 2->5, 3->10  ))
val rdd3 = sc.parallelize( List( 4->50  ))
rdd1.cogroup(rdd2).collect().mkString("\n")
rdd1.join(rdd2).collect().mkString("\n")
rdd1.leftOuterJoin(rdd3).collect().mkString("\n")
rdd3.rightOuterJoin(rdd1).collect().mkString("\n")
rdd1.cogroup(rdd3).collect().mkString("\n")


/// example of parsing a csv file
val bankText = sc.textFile(s"datasets/bank/bank-full.csv")

// case class is a way to very quickly have a class with a constructor in the definition of the class
// here an object of class Bank has members age, job, marital, education and balance
// each row will be an object of class Bank (should have been called BankAccount, maybe)
case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
val bank = (bankText
            .map( _. replaceAll("\"", ""))   // drop quotation marks from the string
            .filter(! _.startsWith("age"))   // filter out the header
            .map( _.split(";"))              // split on fields
            .map( s => Bank(s(0).toInt, s(1), s(2), s(3), s(5).toInt) ) //construct a Bank from each record
            ).toDF()                         // convert to DataFrame
bank take 2 foreach println                  // print 2 records
bank.registerTempTable("bank")               // register bank DataFrame with sqlContext so that we can use it in SQL statements

bank.show(10)
bank.explain
bank.printSchema

sqlContext.sql("from bank select * where age<25 order by balance desc").show(10)
