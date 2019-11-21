package com.qcloud.cos_migrate_tool.utils;

import com.qcloud.cos.endpoint.EndpointBuilder;
import com.qcloud.cos.internal.BucketNameUtils;
import com.qcloud.cos.internal.UrlComponentUtils;

public class PathStyleEndpointBuilder implements EndpointBuilder {
    private String endpointSuffix;
    public PathStyleEndpointBuilder(String endpointSuffix) {
        super();
        if (endpointSuffix == null) {
            throw new IllegalArgumentException("endpointSuffix must not be null");
        }
        while(endpointSuffix.startsWith(".")) {
            endpointSuffix = endpointSuffix.substring(1);
        }
        UrlComponentUtils.validateEndPointSuffix(endpointSuffix);
        this.endpointSuffix = endpointSuffix.trim();
    }

    public String buildGeneralApiEndpoint(String bucketName) {
        BucketNameUtils.validateBucketName(bucketName);
        return String.format("%s", this.endpointSuffix);
    }

    public String buildGetServiceApiEndpoint() {
        return this.endpointSuffix;
    }
}
