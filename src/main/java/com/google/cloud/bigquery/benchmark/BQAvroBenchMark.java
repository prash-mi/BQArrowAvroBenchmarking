package com.google.cloud.bigquery.benchmark;


import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;
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

@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)


public class BQAvroBenchMark {
    private static BigQueryReadClient client;
    private static final String SRC_TABLE_USA_NAMES = "projects/bigquery-public-data/datasets/usa_names/tables/usa_1910_current";
    private  List<ReadRowsResponse> cachedRowRes = null;
    private Schema avroSchema;

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
        public void processRows(ReadRowsResponse res, Blackhole blackhole) throws IOException {////deserialize the values and consume the hash of the values
            AvroRows avroRows = res.getAvroRows();
            decoder =
                    DecoderFactory.get()
                            .binaryDecoder(avroRows.getSerializedBinaryRows().toByteArray(), decoder);

            while (!decoder.isEnd()) {
                // Reusing object row
                row = datumReader.read(row, decoder);
                blackhole.consume(row.get("name").toString().hashCode()+row.get("state").toString().hashCode()+Long.parseLong(row.get("number").toString()));
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



    private List<ReadRowsResponse> getRowsFromBQStorage() throws IOException {
        this.client = BigQueryReadClient.create();
        List<ReadRowsResponse> cachedRowRes = new ArrayList<>();
        String projectId = "mweb-demos";
        Integer snapshotMillis = null;
        String[] args = {null};
        if (args.length > 1) {
            snapshotMillis = Integer.parseInt(args[1]);
        }

        String parent = String.format("projects/%s", projectId);
        TableReadOptions options =
                TableReadOptions.newBuilder()
                        .addSelectedFields("name")
                        .addSelectedFields("number")
                        .addSelectedFields("state")
                        .setRowRestriction("state = \"WA\"")

                        .build();

        // Start specifying the read session we want created.
        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(SRC_TABLE_USA_NAMES)
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
        for (ReadRowsResponse response : stream) {
            Preconditions.checkState(response.hasAvroRows());
            cachedRowRes.add(response);
        }
        return cachedRowRes;
    }

    @Benchmark
    public void deserializationBenchmark( Blackhole blackhole) throws Exception {
        SimpleRowReader reader =
                new SimpleRowReader(avroSchema);
        for (ReadRowsResponse res: cachedRowRes){
            reader.processRows(res, blackhole);
        }

    }


    public static void main(String[] args) throws Exception {

   /*     BQAvroBenchMark bqBenmark = new BQAvroBenchMark();
        bqBenmark.setUp();
        bqBenmark.deserializationBenchmark(null);//for debugging, Will throw NPE*/

    }
}