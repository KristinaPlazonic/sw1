// simple word count example

val rdd = sc.textFile("datasets/shakespeare.txt")
val wc = rdd.flatMap( _.split(" ")).map(w=>(w,1)).reduceByKey(_+_)

wc.top(10) foreach println // see top 10 pairs of words and counts
