package com.buffet.kafka.protocol.api;

import com.buffet.kafka.core.Broker;
import com.buffet.kafka.protocol.Request;
import com.buffet.kafka.protocol.Response;

@FunctionalInterface
public interface ApiProcessor {
    Response process(Request request, Broker broker);
}
