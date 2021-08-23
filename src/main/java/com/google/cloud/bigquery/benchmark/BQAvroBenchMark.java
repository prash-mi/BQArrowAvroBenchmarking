package com.google.cloud.bigquery.benchmark;


import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2)
@Measurement(iterations = 10)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)


public class BQAvroBenchMark {
    private static BigQueryReadClient client;
    private static final String SRC_TABLE = //"projects/bigquery-public-data/datasets/usa_names/tables/usa_1910_current";
                                                         "projects/bigquery-public-data/datasets/new_york_taxi_trips/tables/tlc_yellow_trips_2017";
    private static SimpleRowReader reader;
    private  List<AvroRows> cachedRowRes = null;
    private Schema avroSchema;

    private static  final List<String> fields = ImmutableList.of("vendor_id","pickup_datetime","rate_code","dropoff_datetime","payment_type","pickup_location_id","dropoff_location_id");//,"dropoff_datetime","passenger_count","trip_distance","trip_distance","pickup_latitude","dropoff_longitude","payment_type","fare_amount","extra","tip_amount","imp_surcharge","pickup_location_id","dropoff_location_id");

    private static class SimpleRowReader {

        private final DatumReader<GenericRecord> datumReader;

        // Decoder object will be reused to avoid re-allocation and too much garbage collection.
        private BinaryDecoder decoder = null;

        // GenericRecord object will be reused.
        private GenericRecord row = null;

        public SimpleRowReader(Schema schema) {
            Preconditions.checkNotNull(schema);
            datumReader = new GenericDatumReader<>(schema);

        }

        /**
         * Sample method for processing AVRO rows which only validates decoding.
         *
         * @param avroRows object returned from the ReadRowsResponse.
         */
        public void processRows(AvroRows avroRows, Blackhole blackhole) throws IOException {////deserialize the values and consume the hash of the values
        //    AvroRows avroRows = res.getAvroRows();
          //  System.out.println("avroRows.getRowCount(): "+avroRows.getRowCount());
            decoder =
                    DecoderFactory.get()
                            .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);
            blackhole.consume(avroRows);
           while (!decoder.isEnd()) {
                // Reusing object row
                row = datumReader.read(row, decoder);
                long hash = 0;
               // hash = row.get("vendor_id").toString().hashCode();
                for(String field: fields){
                    hash += row.get(field)!=null? row.get(field).toString().hashCode():0;
                }
                blackhole.consume(hash);
            }

        }

    }


    @Setup
    public void setUp() {
        try {
            if(this.cachedRowRes == null) {
                this.cachedRowRes = getRowsFromBQStorage();//cache the BQ rows to avoid any variation in benchmarking due to network factors
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    private List<AvroRows> getRowsFromBQStorage() throws IOException {


        this.client = BigQueryReadClient.create();
        List<AvroRows> cachedRowRes = new ArrayList<>();
        String projectId = "java-docs-samples-testing";
        Integer snapshotMillis = null;
        String[] args = {null};
        if (args.length > 1) {
            snapshotMillis = Integer.parseInt(args[1]);
        }

        String parent = String.format("projects/%s", projectId);

        TableReadOptions.Builder tb =
                TableReadOptions.newBuilder();
               tb.addAllSelectedFields(fields);

                      //  .addAllSelectedFields(fields)
        TableReadOptions options = tb.build();

        // Start specifying the read session we want created.
        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(SRC_TABLE)
                        // This API can also deliver data serialized in Apache Avro format.
                        // This example leverages Apache Avro.
                        .setDataFormat(DataFormat.AVRO)
                        .setReadOptions(options);

        // Optionally specify the snapshot time.  When unspecified, snapshot time is "now".
        if (snapshotMillis != null) {
            Timestamp t =
                    Timestamp.newBuilder()
                            .setSeconds(snapshotMillis / 1000)
                            .setNanos((int) ((snapshotMillis % 1000) * 1000000))
                            .build();
            TableModifiers modifiers = TableModifiers.newBuilder().setSnapshotTime(t).build();
            sessionBuilder.setTableModifiers(modifiers);
        }

        // Begin building the session creation request.
        CreateReadSessionRequest.Builder builder =
                CreateReadSessionRequest.newBuilder()
                        .setParent(parent)
                        .setReadSession(sessionBuilder)
                        .setMaxStreamCount(1);


        // Request the session creation.
        ReadSession session = client.createReadSession(builder.build());


        avroSchema = new Schema.Parser().parse(session.getAvroSchema().getSchema());
        // Assert that there are streams available in the session.  An empty table may not have
        // data available.  If no sessions are available for an anonymous (cached) table, consider
        // writing results of a query to a named table rather than consuming cached results directly.
        Preconditions.checkState(session.getStreamsCount() > 0);

        // Use the first stream to perform reading.
        String streamName = session.getStreams(0).getName();

        ReadRowsRequest readRowsRequest =
                ReadRowsRequest.newBuilder().setReadStream(streamName).build();

        // Process each block of rows as they arrive and decode using our simple row reader.
        ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
         reader =
                new SimpleRowReader(avroSchema);
        for (ReadRowsResponse response : stream) {
            Preconditions.checkState(response.hasAvroRows());
            cachedRowRes.add(response.getAvroRows());
        }
        return cachedRowRes;
    }

    @Benchmark
    public void deserializationBenchmark( Blackhole blackhole) throws Exception {

       // System.out.println("cachedRowRes.size(): "+cachedRowRes.size());
        for (AvroRows res: cachedRowRes){
            reader.processRows(res, blackhole);
        }

    }


    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BQAvroBenchMark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    /*    BQAvroBenchMark bqBenmark = new BQAvroBenchMark();
        bqBenmark.setUp();
        bqBenmark.deserializationBenchmark(null);//for debugging, Will throw NPE*/

    }
}