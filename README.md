# BQArrowAvroBenchmarking

This project evaluates the performance difference while using Apache Arrow & Apache Avro data formats with Bigquery storage Java Client.
This processes over 113 million records from ```bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2017``` and deserializes the records till the row level.


## How to run this project?

1> Set the environment variable ```GOOGLE_APPLICATION_CREDENTIALS``` as described at https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#client-libraries-install-java

2> Clone this project (```git clone https://github.com/prash-mi/BQArrowAvroBenchmarking```)

3> Run ```mnv clean install``` from the root of the project
