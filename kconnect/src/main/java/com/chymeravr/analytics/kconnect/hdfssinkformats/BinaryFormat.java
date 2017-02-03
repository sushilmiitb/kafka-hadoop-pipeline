package com.chymeravr.analytics.kconnect.hdfssinkformats;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.*;
import io.confluent.connect.hdfs.hive.HiveMetaStore;
import io.confluent.connect.hdfs.hive.HiveUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.IOException;

/**
 * Created by rubbal on 2/2/17.
 */

/**
 * Class which takes the key, value from a topic and writes to HDFS in a space separated line.
 * The number of records written is dependent on configuration of
 *
 * 1. flush.size: Controls how many records to write before flushing the file. A file will never have more than these
 *              many records. (Though it might have lesser. See below)
 * 2. rotate.interval.ms: This is the interval at which the connector will commit files. This is required in case we
 *              too few events coming into the pipeline and the flush.size is never reached soon (Delaying the entire
 *              downstream processing). So we can configure a time interval after which all the events in stream will
 *              be flushed (i.e. written to HDFS in our case. A file won't be available untill kafka connect commits,
 *              not even partially)
 */
public class BinaryFormat implements Format {
    private final String separator = " ";

    @Override
    public RecordWriterProvider getRecordWriterProvider() {
        return new RecordWriterProvider() {

            @Override
            public String getExtension() {
                return ".tser";
            }

            @Override
            public RecordWriter<SinkRecord> getRecordWriter(final Configuration conf,
                                                            final String fileName,
                                                            SinkRecord record,
                                                            AvroData avroData) throws IOException {
                return new RecordWriter<SinkRecord>() {
                    private final Path path = new Path(fileName);
                    private final FSDataOutputStream hadoop = path.getFileSystem(conf).create(path);

                    @Override
                    public void write(SinkRecord sinkRecord) throws IOException {
                        try {
                            byte[] binaryData = (sinkRecord.key() + separator + sinkRecord.value()).getBytes();
                            hadoop.write(binaryData);
                            hadoop.write("\n".getBytes());
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void close() throws IOException {
                        hadoop.close();
                    }
                };
            }
        };
    }

    @Override
    public SchemaFileReader getSchemaFileReader(AvroData avroData) {
        return null;
    }

    @Override
    public HiveUtil getHiveUtil(HdfsSinkConnectorConfig hdfsSinkConnectorConfig, AvroData avroData, HiveMetaStore hiveMetaStore) {
        return null;
    }
}
