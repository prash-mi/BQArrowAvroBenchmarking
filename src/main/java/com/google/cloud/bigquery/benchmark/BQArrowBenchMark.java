package com.google.cloud.bigquery.benchmark;


import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.*;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;

@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class BQArrowBenchMark {
    private static BigQueryReadClient client;
    private static final String SRC_TABLE_USA_NAMES = "projects/bigquery-public-data/datasets/usa_names/tables/usa_1910_current";
    private List<ArrowRecordBatch> cachedRes = new ArrayList<>();
    private ArrowSchema arrowSchema;

    /*
     * SimpleRowReader handles deserialization of the Apache Arrow-encoded row batches transmitted
     * from the storage API using a generic datum decoder.
     */
    private static class SimpleRowReader implements AutoCloseable {

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // Decoder object will be reused to avoid re-allocation and too much garbage collection.
        private final VectorSchemaRoot root;
        private final VectorLoader loader;


        public SimpleRowReader(ArrowSchema arrowSchema) throws IOException {
            Schema schema =
                    MessageSerializer.deserializeSchema(
                            new ReadChannel(
                                    new ByteArrayReadableSeekableByteChannel(
                                            arrowSchema.getSerializedSchema().toByteArray())));
            Preconditions.checkNotNull(schema);
            List<FieldVector> vectors = new ArrayList<>();
            for (Field field : schema.getFields()) {
                vectors.add(field.createVector(allocator));
            }
            root = new VectorSchemaRoot(vectors);
            loader = new VectorLoader(root);
        }

        /**
         * Sample method for processing Arrow data which only validates decoding.
         *
         * @param batch object returned from the ReadRowsResponse.
         */
        public void processRows(ArrowRecordBatch batch, Blackhole blackhole) throws IOException {
            org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch =
                    MessageSerializer.deserializeRecordBatch(
                            new ReadChannel(
                                    new ByteArrayReadableSeekableByteChannel(
                                            batch.getSerializedRecordBatch().toByteArray())),
                            allocator);

            loader.load(deserializedBatch);
            // Release buffers from batch (they are still held in the vectors in root).
            deserializedBatch.close();

            //Splitting the TSV content by new line and the tabs to get the values as rows
            String[] rows = root.contentToTSVString().split("\n");
            for (String tabbedRow:rows){
                composeAndConsumeJson(tabbedRow.split("\t"), blackhole);
            }

           // blackhole.consume(root.contentToTSVString());
            // Release buffers from vectors in root.
            root.clear();

        }
    void composeAndConsumeJson(String[] row, Blackhole blackhole){
        Map<String, String> rowAtr = new HashMap<>();
        rowAtr.put("name", row[0]);
        rowAtr.put("number", row[1]);
        rowAtr.put("state", row[2]);
        Gson gson = new GsonBuilder().create();
        blackhole.consume(gson.toJson(rowAtr));//consume to JSON serialisation
    }

        @Override
        public void close() {
            root.close();
            allocator.close();
        }
    }


    @Setup
    public void setUp() {
        try {
            this.client = BigQueryReadClient.create();
            this.cachedRes = getArrowRowsFromBQStorage();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void deserializationBenchmark( Blackhole blackhole) throws Exception {
        SimpleRowReader reader = new SimpleRowReader(arrowSchema);
        for (ArrowRecordBatch batch: cachedRes){//deserialize the records and convert back to JSON
            reader.processRows(batch, blackhole);
        }
    }


    public  List<ArrowRecordBatch> getArrowRowsFromBQStorage(){
        List<ArrowRecordBatch> cachedRes = new ArrayList<>();
        // Sets your Google Cloud Platform project ID.
        String projectId = "mweb-demos";
        Integer snapshotMillis = null;
        String[] args = {null};
        if (args.length > 1) {
            snapshotMillis = Integer.parseInt(args[1]);
        }


        String parent = String.format("projects/%s", projectId);


        // We specify the columns to be projected by adding them to the selected fields,
        // and set a simple filter to restrict which rows are transmitted.
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
                        // This example leverages Apache Arrow.
                        .setDataFormat(DataFormat.ARROW)
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

        ReadSession session = client.createReadSession(builder.build());
        arrowSchema = session.getArrowSchema();
        // Setup a simple reader and start a read session.

            // Assert that there are streams available in the session.  An empty table may not have
            // data available.  If no sessions are available for an anonymous (cached) table, consider
            // writing results of a query to a named table rather than consuming cached results
            // directly.
            Preconditions.checkState(session.getStreamsCount() > 0);

            // Use the first stream to perform reading.
            String streamName = session.getStreams(0).getName();

            ReadRowsRequest readRowsRequest =
                    ReadRowsRequest.newBuilder().setReadStream(streamName).build();

            // Process each block of rows as they arrive and decode using our simple row reader.
            ServerStream<ReadRowsResponse> stream = client.readRowsCallable().call(readRowsRequest);
            for (ReadRowsResponse response : stream) {
                Preconditions.checkState(response.hasArrowRecordBatch());
                cachedRes.add(response.getArrowRecordBatch());
              // reader.processRows(response.getArrowRecordBatch());
            }
    return cachedRes;
    }

    public static void main(String[] args) throws Exception {
      /*  BQArrowBenchMark bqArrowBenchMark = new BQArrowBenchMark();
        bqArrowBenchMark.setUp();
        bqArrowBenchMark.deserializationBenchmark(null);//for debugging*/
    }
}
