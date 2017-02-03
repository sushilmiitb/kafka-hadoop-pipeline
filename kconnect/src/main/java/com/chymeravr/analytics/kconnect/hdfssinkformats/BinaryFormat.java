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
