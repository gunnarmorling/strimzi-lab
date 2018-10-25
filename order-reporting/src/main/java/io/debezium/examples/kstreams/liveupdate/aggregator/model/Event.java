/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.examples.kstreams.liveupdate.aggregator.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Event {

    public long id;

    public String name;

    @JsonProperty("event_date")
    public String date;

    public String price;

    public Event() {
    }
}
