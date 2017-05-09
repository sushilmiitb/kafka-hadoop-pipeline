package com.chymeravr.analytics.eventjoin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.chymeravr.schemas.serving.ImpressionInfo;
import com.chymeravr.schemas.serving.ImpressionLog;
import com.chymeravr.schemas.serving.ResponseLog;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by rubbal on 30/1/17.
 */
public class Job {
    @Parameter(names = {"--config", "-c"}, description = "config file", required = true)
    private String configFilePath;

    public static void main(String... args) throws Exception {
        Job app = new Job();
        new JCommander(app, args);
        app.run();
    }

    private void run() throws IOException, ConfigurationException {
        PropertiesConfiguration config = new PropertiesConfiguration(configFilePath);
        config.setThrowExceptionOnMissing(true);

        Configuration kstreamConfig = config.subset("kstream");

        Properties kafkaStreamProps = ConfigurationConverter.getProperties(kstreamConfig);

        kafkaStreamProps.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        kafkaStreamProps.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        String responseTopicName = config.getString("responseLogTopic");
        String impressionTopicName = config.getString("impressionTopic");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> responseLogs = builder.stream(responseTopicName);

        KStream<String, String> impressionStream = responseLogs.flatMap(
                (key, value) -> {
                    List<KeyValue<String, String>> impressions = new ArrayList<>();
                    try {
                        ResponseLog responseLog = Utils.deserializeBase64Thrift(ResponseLog.class, value);
                        Map<String, ImpressionInfo> impressionInfoMap = responseLog.getImpressionInfoMap();
                        for (Map.Entry<String, ImpressionInfo> impressionInfoEntry : impressionInfoMap.entrySet()) {
                            ImpressionInfo impressionInfo = impressionInfoEntry.getValue();
                            ImpressionLog impressionLog = new ImpressionLog(
                                    responseLog.getTimestamp(),
                                    responseLog.getRequestId(),
                                    responseLog.getSdkVersion(),
                                    responseLog.getExperimentIds(),
                                    responseLog.getRequestInfo(),
                                    responseLog.getResponseCode(),
                                    impressionInfoEntry.getKey(),
                                    impressionInfo
                            );

                            impressions.add(new KeyValue<>(
                                    impressionInfo.getServingId(),
                                    Utils.serializeBase64Thrift(impressionLog)
                            ));
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return impressions;
                });


        impressionStream.to(impressionTopicName);

        // Build the topology and start processing
        KafkaStreams streams = new KafkaStreams(builder, kafkaStreamProps);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
