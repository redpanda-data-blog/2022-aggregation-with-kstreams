package com.application;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class EnhancedOrdersApplication {
    public static void main(final String[] args) throws Exception {
        final Properties streamsConfiguration = new Properties();

        streamsConfiguration.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "enhanced-orders-application-v1.0.13");
        streamsConfiguration.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfiguration.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, String> userProfiles = builder.table("userProfiles");
        final KStream<String, String> orders = builder.stream("orders");

        KStream<String, String> joined = orders.join(userProfiles,
                (userProfile, order) -> userProfile + order
        );
        joined.to("enhancedOrders");

        final KafkaStreams streams = new KafkaStreams(builder.build(), streamsConfiguration);
        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("enhanced-orders-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
