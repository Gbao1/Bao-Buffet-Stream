package com.buffet.kafka.protocol;

import com.buffet.kafka.core.Broker;
import com.buffet.kafka.protocol.api.ApiFactory;

public final class ProtocolGateway {
    private final Broker broker;

    public ProtocolGateway(Broker broker) {
        this.broker = broker;
    }

    public Response handle(byte[] rawRequest) {
        Request request = Request.deserialize(rawRequest);
        return ApiFactory.getProcessor(request.getHeader().getApiKey()).process(request, broker);
    }
}
