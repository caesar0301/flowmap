Kalin Project
=============

This [scala](http://www.scala-lang.org/) project aims to ETL (extract,
transform and load) spatiotemporal data of human mobility for further
analysis.

Beyond the preprocessing jobs, Kalin also introduces a
`stlab` package to facilitate spatotemporal data analysis with Spark RDD.

Build
-----

    $ sbt package
    
or with dependencies

    $ sbt
    
Run testing
-----------

    $ sbt test
    
Available Spark Jobs
--------------------

In package `cn.edu.sjtu.omnilab.kalin.hz`:

* `CountLogsJob`: count the number of logs of feed input;
* `PrepareDataJob`: separate input logs into isolated sets by day;
* `RadiusGyrationJob`: calculate the radius of gyration from human movement data;
* `TidyMovementJob`: filter out redundant movement history to keep data brief;