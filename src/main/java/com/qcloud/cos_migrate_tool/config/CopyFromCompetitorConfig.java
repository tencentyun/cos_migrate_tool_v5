package com.qcloud.cos_migrate_tool.config;


public class CopyFromCompetitorConfig extends CommonConfig {
    private String srcPrefix = "";
    private String srcBucket;
    private String srcEndpoint;
    private String srcAccessKeyId;
    private String srcAccessKeySecret;
    
    private String srcProxyHost = "";
    private int srcProxyPort = -1;
    
    public String getSrcProxyHost() {
    	return srcProxyHost;
    }
    
    public void setSrcProxyHost(String proxyHost) {
    	this.srcProxyHost = proxyHost;
    }
    
    public int getSrcProxyPort() {
    	return srcProxyPort;
    }
    
    public void setSrcProxyPort(int proxyPort) {
    	this.srcProxyPort = proxyPort;
    }

    public String getSrcPrefix() {
        return srcPrefix;
    }

    public void setSrcPrefix(String prefix) {
        this.srcPrefix = prefix;
    }

    
    public String getSrcBucket() {
    	return srcBucket;
    }
    
    public void setSrcBucket(String bucket) throws IllegalArgumentException {
    	if (bucket.isEmpty()) {
    		throw new IllegalArgumentException("bucket is empty");
    	}
    	
    	this.srcBucket = bucket;
    }

    public String getSrcEndpoint() {
    	return srcEndpoint;
    }
    
    public void setEndpoint(String endpoint) throws IllegalArgumentException {
    	if (endpoint.isEmpty()) {
    		throw new IllegalArgumentException("endPoint is empty");
    	}
    	
    	this.srcEndpoint = endpoint;
    }
    
    
    public String getSrcAccessKeyId() {
    	return srcAccessKeyId;
    }
    
    public void setSrcAccessKeyId(String accessKeyId) throws IllegalArgumentException {
    	if (accessKeyId.isEmpty()) {
    		throw new IllegalArgumentException("accessKeyId is empty");
    	}
    	
    	this.srcAccessKeyId = accessKeyId;
    }
    
    public String getSrcAccessKeySecret() {
    	return srcAccessKeySecret;
    }
    
    public void setSrcAccessKeySecret(String accessKeySecret) throws IllegalArgumentException {
    	if (accessKeySecret.isEmpty()) {
    		throw new IllegalArgumentException("accessKeySecret is empty");
    	}
    	
    	this.srcAccessKeySecret = accessKeySecret;
    }
        
    
}
