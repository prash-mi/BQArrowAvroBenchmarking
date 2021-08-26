package com.google.cloud.bigquery.benchmark;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class Constants {
    public static final String SRC_TABLE = //"projects/bigquery-public-data/datasets/usa_names/tables/usa_1910_current";
            "projects/bigquery-public-data/datasets/new_york_taxi_trips/tables/tlc_yellow_trips_2017";

    public static  final List<String> FIELDS = ImmutableList.of("vendor_id","pickup_datetime","rate_code","dropoff_datetime","payment_type","pickup_location_id","dropoff_location_id");//,"dropoff_datetime","passenger_count","trip_distance","trip_distance","pickup_latitude","dropoff_longitude","payment_type","fare_amount","extra","tip_amount","imp_surcharge","pickup_location_id","dropoff_location_id");
     /*
     //All the 21 fields
     public static  final List<String> FIELDS = ImmutableList.of("vendor_id","pickup_datetime","dropoff_datetime","passenger_count","trip_distance",
            "pickup_longitude","pickup_latitude","rate_code","store_and_fwd_flag","dropoff_longitude",
            "dropoff_latitude","payment_type","fare_amount","extra","mta_tax","tip_amount"
            ,"tolls_amount","imp_surcharge","total_amount","pickup_location_id","dropoff_location_id");
            */
    public static  final String PROJECT_ID = "java-docs-samples-testing";
    public static final int WARMUP_ITERATIONS = 2;
    public static final int MEASUREMENT_ITERATIONS = 5;
}
