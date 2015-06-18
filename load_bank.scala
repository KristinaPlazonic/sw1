// load a csv file with the following schema: 
// "age";"job";"marital";"education";"default";"balance";"housing";"loan";"contact";"day";"month";"duration";"campaign";"pdays";"previous";"poutcome";"y"
// sample row: 
// 58;"management";"married";"tertiary";"no";2143;"yes";"no";"unknown";5;"may";261;1;-1;0;"unknown";"no"
val bankText = sc.textFile(s"datasets/bank/bank-full.csv")

// case class is a way to very quickly have a class with a constructor in the definition of the class
// here an object of class Bank has members age, job, marital, education and balance
// each row will be an object of class Bank (should have been called BankAccount, maybe)

case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)

// here is an example of how to parse a csv file in Scala
val bank = (bankText
            .map( _. replaceAll("\"", ""))   // drop quotation marks from the string
            .filter(! _.startsWith("age"))   // filter out the header
            .map( _.split(";"))              // split on fields
            .map( s => Bank(s(0).toInt, s(1), s(2), s(3), s(5).toInt) ) //construct a Bank from each record
            ).toDF()                         // convert to DataFrame
bank take 2 foreach println                  // print 2 records
bank.registerTempTable("bank")               // register bank DataFrame with sqlContext so that we can use it in SQL statements
