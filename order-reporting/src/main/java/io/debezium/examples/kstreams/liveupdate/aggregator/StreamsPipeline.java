/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.examples.kstreams.liveupdate.aggregator;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Serialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.examples.kstreams.liveupdate.aggregator.model.Event;
import io.debezium.examples.kstreams.liveupdate.aggregator.model.Order;
import io.debezium.examples.kstreams.liveupdate.aggregator.serdes.ChangeEventAwareJsonSerde;

public class StreamsPipeline {

    private static final Logger LOG = LoggerFactory.getLogger(StreamsPipeline.class);

    public static KTable<String, Long> ordersPerEvent(StreamsBuilder builder) {
        Serde<Long> longKeySerde = new ChangeEventAwareJsonSerde<>(Long.class);
        longKeySerde.configure(Collections.emptyMap(), true);

        Serde<Order> orderSerde = new ChangeEventAwareJsonSerde<>(Order.class);
        orderSerde.configure(Collections.emptyMap(), false);

        Serde<Event> categorySerde = new ChangeEventAwareJsonSerde<>(Event.class);
        categorySerde.configure(Collections.emptyMap(), false);

        KTable<Long, Event> category = builder.table("EventrEvent", Consumed.with(longKeySerde, categorySerde));

        return builder.stream(
                "EventrOrder",
                Consumed.with(longKeySerde, orderSerde)
                )
                .selectKey((k, v) -> v.eventId)
                .join(
                        category,
                        (value1, value2) -> {
                            value1.eventName = value2.name;
                            return value1;
                        },
                        Joined.with(Serdes.Long(), orderSerde, null)
                )
                .selectKey((k, v) -> v.eventName)
                .groupByKey(Serialized.with(Serdes.String(), orderSerde))
                .aggregate(
                        () -> 0L, /* initializer */
                        (aggKey, newValue, aggValue) -> {
                            aggValue++;
                            return aggValue;
                        },
                        Materialized.with(Serdes.String(), Serdes.Long())
                );
    }

    public static void waitForTopicsToBeCreated(String bootstrapServers) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            AtomicBoolean topicsCreated = new AtomicBoolean(false);

            while (topicsCreated.get() == false) {
                LOG.info("Waiting for topics to be created");

                ListTopicsResult topics = adminClient.listTopics();
                topics.names().whenComplete((t, e) -> {
                    if (e != null) {
                        throw new RuntimeException(e);
                    }
                    else if (t.contains("EventrOrder") && t.contains("EventrEvent")) {
                        LOG.info("Found topics 'EventrOrder' and 'EventrEvent'");
                        topicsCreated.set(true);
                    }
                });

                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
