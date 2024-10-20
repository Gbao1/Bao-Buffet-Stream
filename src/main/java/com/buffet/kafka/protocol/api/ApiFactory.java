package com.buffet.kafka.protocol.api;

import com.buffet.kafka.core.Broker;
import com.buffet.kafka.protocol.ApiKey;
import com.buffet.kafka.protocol.Request;
import com.buffet.kafka.protocol.Response;
import com.buffet.kafka.protocol.ResponseApiBody;
import com.buffet.kafka.protocol.ResponseBody;
import com.buffet.kafka.protocol.ResponseHeader;

public final class ApiFactory {
    private static final short UNSUPPORTED_VERSION = 35;

    private ApiFactory() {
    }

    public static ApiProcessor getProcessor(short requestApiKey) {
        ApiKey key = ApiKey.fromId(requestApiKey);
        return switch (key) {
            case API_VERSIONS -> ApiFactory::processApiVersionsRequest;
            case DESCRIBE_TOPIC_PARTITIONS -> ApiFactory::processDescribeTopicPartitionsRequest;
        };
    }

    private static Response processApiVersionsRequest(Request request, Broker broker) {
        ResponseHeader header = new ResponseHeader(request.getHeader().getCorrelationId());
        ResponseBody body = new ResponseBody();
        short apiVersion = request.getHeader().getApiVersion();

        body.addApi(new ResponseApiBody((short) ApiKey.API_VERSIONS.id(), (short) 0, (short) 4));

        if (apiVersion < 0 || apiVersion > 4) {
            body.setErrorCode(UNSUPPORTED_VERSION);
        } else {
            body.addApi(new ResponseApiBody((short) ApiKey.DESCRIBE_TOPIC_PARTITIONS.id(), (short) 0, (short) 0));
        }

        return new Response(header, body);
    }

    private static Response processDescribeTopicPartitionsRequest(Request request, Broker broker) {
        ResponseHeader header = new ResponseHeader(request.getHeader().getCorrelationId());
        ResponseBody body = new ResponseBody();

        // We keep this intentionally minimal: just advertise supported APIs like Nafka's early implementation phase.
        body.addApi(new ResponseApiBody((short) ApiKey.API_VERSIONS.id(), (short) 0, (short) 4));
        body.addApi(new ResponseApiBody((short) ApiKey.DESCRIBE_TOPIC_PARTITIONS.id(), (short) 0, (short) 0));

        return new Response(header, body);
    }
}
