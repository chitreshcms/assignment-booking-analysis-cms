# Commercial Booking Analysis

### Background
Additional Dependencies required are : Spark, Scala, SBT (maven can be used also, but since we are using Scala, SBT feels more native and powerful)

This Project can be run in 2 ways :- 
###### 1 . IDE - Clone this project, Open on IntelliJ and after installing dependencies Run this project (for default arguments) or Modify Run Arguments for custom arguments like booking/airport path, start/end dates.

###### 2 . Using the fat JAR Deployed and using the Spark-Submit standard command pointing to the "Main" class 



Automatically uses yarn or local master based on the booking path provided 



PS :Unit test cases, Some optimisation like using partitioning and bonus objective for handling the realtime data has been skipped due to less time bandwith.
