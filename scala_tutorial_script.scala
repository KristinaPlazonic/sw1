//// definining some collection types
val mylist = List(1,2,3)
val mylist2 = Seq(1,2, 3)   // Seq is a trait  (like interface in Java)
val myset = Set(1,1,2,3)    // sets have no repetition
val mymap = Map( 'a'->1, 'b'->2, 'c'->3 )    // associative array - this is a dictionary in Python
val mypair = (1,"a", List(2,3))  // this is a Tuple; elements need not be same type

/// Accessing elements of a collection - 0 based and always with round parenthesis
mylist(2)
mylist.tail
mymap('a')
mypair._2   // accessing pairs are 1-based
List(1,2,3,4,5,6,7) slice (2,4)
1 to 10  // a range

// Difference between val and var
// mylist = List(2,3,4)    //- this is illegal expression because val cannot be reassigned

// you can omit the parenthesis if the function has **zero** parameters
mylist.distinct

// you can omit the dot if the function has only **one** parameter
// you can omit the parenthesis if the function has only **one** parameter
// this also applies if the parameter is itself a function
mylist.take(2)
mylist take 2   // this is same as above

// example of defining a function in Scala: 
def square(x:Int) = x*x   // defines function "square" on Int (compiler infers the return type is Int -)
square(3)
def squareint(x:Int):Int = x*x  // you can declare the return type, but you don't have to
def squaredoub(x:Int):Double = x*x  // you can declare the return type to cast the result in a different type
squareint(3)
squaredoub(3)
def squaredouble(x:Double) = x*x
squaredouble(3)

// applying transformations to collections
mylist.map(square) // apply square to each element of mylist
mylist map square  // this is same as before, because map takes one (function) parameter
mylist foreach println  // apply println to each element of mylist; return nothing

// applying anonymous functions
mylist map square
mylist map { x => x*x } // same as above, example of anonymous function
mylist map { _*2 }   // double every number; can't express square with underscore 
mymap map { case (c, n) => n*n }  // apply a function to associative array

// chaining transformations
mylist.map(square).filter { _%2 != 0 } // square each element, then take only odd ones
mylist map square filter { _%2 != 0 } // same as above, remembering the Scala syntax

// word count in Scala 
// (Note: there is no reduceByKey function in Scala)
(      // need to wrap a multiline expression in () or {} if it is not clear it's not the end of expression
List("i am sam sam i am", 
    "i like green eggs and ham") 
  .flatMap { _.split(" ") } // produce a bag of words
  .map { w => (w,1) }  // map each word into a pair  (word, 1)
  .groupBy { _._1 }  // group by word, which is the first element of the pair
  .mapValues { _.size } // find the size of each group
)

mylist reduce { _+_ }  // sum all elements

// set-theoretic operations
List(1,2).union(List(2,3))
List(1,2) ++ List(2,3)  // same as above
List(1,2,3,4).intersect(List(2,3,4,5))
List(1,2,3,4).diff(List(2,3,4,5))     // set difference; this is replaced with "subtract" in Spark
List(1,1,1,2).distinct
List(1,2,3).zip(List("a", "b", "c"))


