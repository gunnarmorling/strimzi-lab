/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.examples.kstreams.liveupdate.aggregator.model;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Order {

    public long id;

    public String customer;

    @JsonProperty("order_date")
    public Date date;

    public String payment;

    @JsonProperty("event_id")
    public long eventId;

    @JsonProperty("event_name")
    public String eventName;
}
