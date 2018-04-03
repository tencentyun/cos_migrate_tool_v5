package com.qcloud.cos_migrate_tool.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import com.qcloud.cos.http.IdleConnectionMonitorThread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Downloader {
    public static final Downloader instance = new Downloader();
    private static final Logger log = LoggerFactory.getLogger(Downloader.class);

    protected HttpClient httpClient;

    protected PoolingHttpClientConnectionManager connectionManager;
    protected IdleConnectionMonitorThread idleConnectionMonitor;

    protected RequestConfig requestConfig;

    private Downloader() {
        super();
        this.connectionManager = new PoolingHttpClientConnectionManager();
        this.connectionManager.setMaxTotal(2048);
        this.connectionManager.setDefaultMaxPerRoute(2048);
        this.connectionManager.setValidateAfterInactivity(1);
        HttpClientBuilder httpClientBuilder =
                HttpClients.custom().setConnectionManager(connectionManager);
        this.httpClient = httpClientBuilder.build();
        this.requestConfig = RequestConfig.custom().setConnectionRequestTimeout(30 * 1000)
                .setConnectTimeout(30 * 1000).setSocketTimeout(30 * 1000).build();
        this.idleConnectionMonitor = new IdleConnectionMonitorThread(this.connectionManager);
        this.idleConnectionMonitor.setDaemon(true);
        this.idleConnectionMonitor.start();
    }

    public long headFile(String url) {

        int retry = 0;
        int maxRetryCount = 5;
        while (retry < maxRetryCount) {
            HttpHead httpHead = null;
            try {
                URL encodeUrl = new URL(url);
                URI uri = new URI(encodeUrl.getProtocol(), encodeUrl.getHost(), encodeUrl.getPath(),
                        encodeUrl.getQuery(), null);
                httpHead = new HttpHead(uri);
            } catch (URISyntaxException e) {
                String errMsg = "Invalid url:" + url;
                log.error(errMsg);
            } catch (MalformedURLException e) {
                log.error("headFile url fail,url:{},msg:{}", url, e.getMessage());
                return -1;
            }

            httpHead.setConfig(requestConfig);
            httpHead.setHeader("Accept", "*/*");
            httpHead.setHeader("Connection", "Keep-Alive");
            httpHead.setHeader("User-Agent", "cos-migrate-tool");

            try {
                HttpResponse httpResponse = httpClient.execute(httpHead);
                int http_statuscode = httpResponse.getStatusLine().getStatusCode();
                if (http_statuscode < 200 || http_statuscode > 500) {
                    String errMsg = String.format(
                            "head failed, url: %s, httpResponse: %s, response_statuscode: %d", url,
                            httpResponse.toString(), http_statuscode);
                    throw new Exception(errMsg);
                }

                if (!httpResponse.containsHeader("content-length")) {
                    log.info("not find content-length");
                    return -1;
                }

                Header header = httpResponse.getFirstHeader("content-length");

                try {
                    long contentLength = Long.valueOf(header.getValue());
                    if (contentLength < 0) {
                        throw new IllegalArgumentException("The minimum of content-length is 0");
                    }
                    return contentLength;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("invalid content-length");
                }

            } catch (Exception e) {
                log.error(e.toString());
                httpHead.abort();
                return -1;
            }

        }
        return -1;
    }

    private void showDownloadProgress(String url, long byteTotal, long byteDownloadSofar) {
        double pct = 100.0;
        if (byteTotal != 0) {
            pct = byteDownloadSofar * 1.0 / byteTotal * 100;
        }
        String status = "DownloadInProgress";
        if (byteTotal == byteDownloadSofar) {
            status = "DownloadOk";
        }
        String printMsg = String.format(
                "[%s] [url: %s] [byteDownload/ byteTotal/ percentage: %d/ %d/ %.2f%%]", status, url,
                byteDownloadSofar, byteTotal, pct);
        System.out.println(printMsg);
        log.info(printMsg);
    }

    public boolean downFile(String url, File localFile) {
        int retry = 0;
        int maxRetryCount = 5;
        while (retry < maxRetryCount) {
            HttpGet httpGet = null;
            try {
                URL encodeUrl = new URL(url);
                URI uri = new URI(encodeUrl.getProtocol(), encodeUrl.getHost(), encodeUrl.getPath(),
                        encodeUrl.getQuery(), null);
                httpGet = new HttpGet(uri);
            } catch (URISyntaxException e) {
                String errMsg = "Invalid url:" + url;
                log.error(errMsg);
                return false;
            } catch (MalformedURLException e) {
                log.error("downFile url fail,url:{},msg:{}", url, e.getMessage());
                return false;
            }

            httpGet.setConfig(requestConfig);
            httpGet.setHeader("Accept", "*/*");
            httpGet.setHeader("Connection", "Keep-Alive");
            httpGet.setHeader("User-Agent", "cos-migrate-tool-v1.0");
            try {
                HttpResponse httpResponse = httpClient.execute(httpGet);
                int http_statuscode = httpResponse.getStatusLine().getStatusCode();
                if (http_statuscode < 200 || http_statuscode > 299) {
                    String errMsg = String.format(
                            "getFileinputstream failed, url: %s, httpResponse: %s, response_statuscode: %d",
                            url, httpResponse.toString(), http_statuscode);
                    log.error(errMsg);
                    throw new Exception(errMsg);
                }
                HttpEntity entity = httpResponse.getEntity();
                long contentLength = entity.getContentLength();
                long byteDownloadSoFar = 0;
                long byteDownloadLastPrint = 0;
                long lastPrintTimeStamp = 0;

                BufferedInputStream bis = new BufferedInputStream(entity.getContent());
                OutputStream out = null;
                BufferedOutputStream bos = null;
                try {
                    out = new FileOutputStream(localFile);
                    bos = new BufferedOutputStream(out);
                    int inByte;
                    while ((inByte = bis.read()) != -1) {
                        bos.write(inByte);
                        ++byteDownloadSoFar;
                        if (byteDownloadSoFar - byteDownloadLastPrint >= 1024) {
                            long currentTimeStamp = System.currentTimeMillis();
                            if (currentTimeStamp - lastPrintTimeStamp >= 2000) {
                                showDownloadProgress(url, contentLength, byteDownloadSoFar);
                                byteDownloadLastPrint = byteDownloadSoFar;
                                lastPrintTimeStamp = currentTimeStamp;
                            }
                        }
                    }
                    showDownloadProgress(url, contentLength, byteDownloadSoFar);
                    httpGet.releaseConnection();
                    return true;
                } finally {
                    try {
                        bis.close();
                        bos.close();
                    } catch (IOException e) {
                    }
                }
            } catch (Exception e) {
                log.error(e.toString());
                httpGet.abort();
                localFile.delete();
            }
            ++retry;
        }
        return false;
    }

    public void shutdown() {
        this.idleConnectionMonitor.shutdown();
    }
}
