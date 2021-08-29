package com.google.cloud.bigquery.benchmark;

import com.google.api.gax.rpc.ServerStream;
import com.google.cloud.bigquery.storage.v1.*;
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
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@Fork(value = 1)
@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = Constants.WARMUP_ITERATIONS)
@Measurement(iterations = Constants.MEASUREMENT_ITERATIONS)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
//This includes network i/o times in the benchmark
public class BQArrowBenchMarkWithNW {
    private static BigQueryReadClient client;
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
        public void processRows(ArrowRecordBatch batch, Blackhole blackhole) throws IOException {//deserialize the values and consume the hash of the values
            org.apache.arrow.vector.ipc.message.ArrowRecordBatch deserializedBatch =
                    MessageSerializer.deserializeRecordBatch(
                            new ReadChannel(
                                    new ByteArrayReadableSeekableByteChannel(
                                            batch.getSerializedRecordBatch().toByteArray())),
                            allocator);

            loader.load(deserializedBatch);
            // Release buffers from batch (they are still held in the vectors in root).
            deserializedBatch.close();
            long hash = 0;
            for (int i = 0; i < root.getRowCount(); i++) {
                //"vendor_id","pickup_datetime","rate_code","dropoff_datetime","payment_type","pickup_location_id","dropoff_location_id"
                hash += new String(((VarCharVector)root.getVector("vendor_id")).get(i)).hashCode();
                hash += (String.valueOf(((TimeStampMicroVector)root.getVector("pickup_datetime")).get(i))).hashCode();
                hash += new String(((VarCharVector)root.getVector("rate_code")).get(i)).hashCode();
                hash += (String.valueOf(((TimeStampMicroVector)root.getVector("dropoff_datetime")).get(i))).hashCode();
                hash += new String(((VarCharVector)root.getVector("payment_type")).get(i)).hashCode();
                hash += new String(((VarCharVector)root.getVector("pickup_location_id")).get(i)).hashCode();
                hash += new String(((VarCharVector)root.getVector("dropoff_location_id")).get(i)).hashCode();

            }
            root.clear();
            blackhole.consume(hash);

        }

        @Override
        public void close() {
            root.close();
            allocator.close();
        }
    }


    @Setup
    public void setUp() throws IOException {
        this.client = BigQueryReadClient.create();
    }


    @Benchmark
    public  void getAndDeserializeRowsFromBQStorage(Blackhole blackhole) throws IOException {

        // Sets your Google Cloud Platform project ID.
        Integer snapshotMillis = null;
        String[] args = {null};
        if (args.length > 1) {
            snapshotMillis = Integer.parseInt(args[1]);
        }


        String parent = String.format("projects/%s", Constants.PROJECT_ID);


        // We specify the columns to be projected by adding them to the selected fields,
        // and set a simple filter to restrict which rows are transmitted.
        TableReadOptions options =
                TableReadOptions.newBuilder()
                        .addAllSelectedFields(Constants.FIELDS)
                        .build();

        // Start specifying the read session we want created.
        ReadSession.Builder sessionBuilder =
                ReadSession.newBuilder()
                        .setTable(Constants.SRC_TABLE)
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
        SimpleRowReader reader = new SimpleRowReader(arrowSchema);
        for (ReadRowsResponse response : stream) {
            Preconditions.checkState(response.hasArrowRecordBatch());
            reader.processRows(response.getArrowRecordBatch(), blackhole);
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(BQArrowBenchMark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
