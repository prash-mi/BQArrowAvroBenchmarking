package com.google.cloud.bigquery.benchmark;


import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.ArrowRecordBatch;
import com.google.cloud.bigquery.storage.v1.ArrowSchema;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest;
import com.google.cloud.bigquery.storage.v1.ReadRowsResponse;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableModifiers;
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions;
import com.google.common.base.Preconditions;
import com.google.protobuf.Timestamp;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

    @State(Scope.Benchmark)
    public static class SourceTables {

        @Param({
                SRC_TABLE_USA_NAMES,

        })
        public String table;
    }
    /*
     * SimpleRowReader handles deserialization of the Apache Arrow-encoded row batches transmitted
     * from the storage API using a generic datum decoder.
     */
    private static class SimpleRowReader implements AutoCloseable {

        BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

        // Decoder object will be reused to avoid re-allocation and too much garbage collection.
        private final VectorSchemaRoot root;
        private final VectorLoader loader;
        private Blackhole blackhole;

        public SimpleRowReader(ArrowSchema arrowSchema, Blackhole blackhole) throws IOException {
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
            this.blackhole = blackhole;
        }

        /**
         * Sample method for processing Arrow data which only validates decoding.
         *
         * @param batch object returned from the ReadRowsResponse.
         */
        public void processRows(ArrowRecordBatch batch) throws IOException {
            org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch =
                    MessageSerializer.deserializeRecordBatch(
                            new ReadChannel(
                                    new ByteArrayReadableSeekableByteChannel(
                                            batch.getSerializedRecordBatch().toByteArray())),
                            allocator);

            loader.load(deserializedBatch);
            // Release buffers from batch (they are still held in the vectors in root).
            deserializedBatch.close();
           // System.out.println(root.contentToTSVString());
            blackhole.consume(root.contentToTSVString());
            // Release buffers from vectors in root.
            root.clear();
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Benchmark
    public void query(BQAvroBenchMark.SourceTables sourceTables, Blackhole blackhole) throws Exception {
        runQuery(sourceTables.table, blackhole);
    }

    public static void runQuery(String srcTable, Blackhole blackhole) throws Exception{
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
                            .setTable(srcTable)
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
            // Setup a simple reader and start a read session.
            try (SimpleRowReader reader = new SimpleRowReader(session.getArrowSchema(), blackhole)) {

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
                    reader.processRows(response.getArrowRecordBatch());
                }
            }
        }
    }
