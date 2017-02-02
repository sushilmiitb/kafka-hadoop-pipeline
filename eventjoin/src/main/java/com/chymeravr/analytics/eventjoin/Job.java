package com.chymeravr.analytics.eventjoin;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.chymeravr.schemas.eventreceiver.EventLog;
import com.chymeravr.schemas.kafka.JoinedEvent;
import com.chymeravr.schemas.serving.ServingLog;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.IOException;
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

        String eventTopicName = config.getString("eventTopic");
        String serverTopicName = config.getString("serveTopic");
        String joinedTopicName = config.getString("joinedTopic");
        int joinwindow = config.getInt("joinWindow");

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> events = builder.stream(eventTopicName);
        KStream<String, String> serve = builder.stream(serverTopicName);

        KStream<String, String> joinedEvent = events.join(
                serve,
                (eventLogSer, serveLogSer) -> {
                    try {
                        ServingLog servingLog = Utils.deserializeBase64Thrift(ServingLog.class, serveLogSer);
                        EventLog eventLog = Utils.deserializeBase64Thrift(EventLog.class, eventLogSer);
                        return Utils.serializeBase64Thrift(new JoinedEvent(servingLog, eventLog));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return null;
                },
                JoinWindows.of(joinwindow));

        joinedEvent.to(joinedTopicName);

        // Build the topology and start processing
        KafkaStreams streams = new KafkaStreams(builder, kafkaStreamProps);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
