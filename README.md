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

    $ sbt assembly
    
Run testing
-----------

This project relies on [specs2](http://etorreborre.github.io/specs2/) to perform
unit test in Scala. Run `test` command to activate shipped testing cases.

    $ sbt test

Available Library
-----------------

In this project, package `cn.edu.sjtu.omnilab.stlab` undertakes more general
works on processing spatiotemporal data points ("RDD" in parentheses means that
method is feed data in Spark RDD container).

* `STUtils`: useful small toolkit for transformation of spatiotemporal data.
* `GeoPoint`: basic representation of geographic points in LON/LAT or Cartesian
coordinate.
* `GeoMidPoint` (RDD): calculate the geographic midpoint given a series of points.
Two algorithms (average lon/lat and Cartesian) are implemented with nearly
the same result in most scenarios.
* `RadiusGyration` (RDD): calculate the radius of gyration (RG) from individual
movement history.
* `TidyMovement` (RDD): remove redundant information in users' movement history.
    
Available Spark Jobs
--------------------

Package `cn.edu.sjtu.omnilab.flowmap.hz` contains jobs for HZ mobile data:

* `CountLogsJob`: count the number of logs of feed input;
* `PrepareDataJob`: separate input logs into isolated sets by day;
* `RadiusGyrationJob`: calculate the radius of gyration from human movement data;
* `GeoRangeJob`: give basic dimensions of data, including geo-range, unique
logs/users/cells numbers;
* `FilterDataGeo`: filter data to leave out logs outside given geo-range,
specifically out the range of HZ administrative area;
* `TidyMovementJob`: filter out redundant movement history to keep data brief;
* `SampleUsersJob`: sample users of high data quality out of the total population;

Package `cn.edu.sjtu.omnilab.flowmap.d4d` contains jobs for D4D data:

* `TidyMovementJob`: filter out redundant movement history to keep data brief;