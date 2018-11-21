package com.qcloud.cos_migrate_tool.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.MalformedURLException;
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
import com.qcloud.cos.utils.UrlEncoderUtils;

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

    public HeadAttr headFile(String url, boolean qiniuDownLoadFlag) {

        int retry = 0;
        int maxRetryCount = 5;
        HeadAttr headAttr = new HeadAttr();

        while (retry < maxRetryCount) {
            HttpHead httpHead = null;
            try {
                StringBuffer urlBuffer = new StringBuffer();
                URL encodeUrl = new URL(url);

                urlBuffer.append(encodeUrl.getProtocol()).append("://").append(encodeUrl.getHost());

                String encodeUrlStr = "";
                if (encodeUrl.getPath().startsWith("/")) {
                    encodeUrlStr = encodeUrl.getPath().substring(1);
                } else {
                    encodeUrlStr = encodeUrl.getPath();
                }
                encodeUrlStr = UrlEncoderUtils.encodeEscapeDelimiter(encodeUrlStr);
                if (qiniuDownLoadFlag) {
                    encodeUrlStr = encodeUrlStr.replaceAll("/", "%2f");
                }
                urlBuffer.append("/").append(encodeUrlStr);

                if (encodeUrl.getQuery() != null) {
                    urlBuffer.append("?").append(encodeUrl.getQuery());
                }

                httpHead = new HttpHead(urlBuffer.toString());
            } catch (MalformedURLException e) {
                log.error("headFile url fail,url:{},msg:{}", url, e.getMessage());
                return null;
            }

            httpHead.setConfig(requestConfig);
            httpHead.setHeader("Accept", "*/*");
            httpHead.setHeader("Connection", "Keep-Alive");
            httpHead.setHeader("User-Agent", "cos-migrate-tool");

            try {
                HttpResponse httpResponse = httpClient.execute(httpHead);
                int http_statuscode = httpResponse.getStatusLine().getStatusCode();
                if (http_statuscode < 200 || http_statuscode > 299) {
                    String errMsg = String.format(
                            "head failed, response_statuscode: %d, url: %s, httpResponse: %s, ", http_statuscode, url,
                            httpResponse.toString());
                    throw new Exception(errMsg);
                }

                if (httpResponse.containsHeader("content-length")) {
                    Header header = httpResponse.getFirstHeader("content-length");
                    long contentLength = -1;
                    try {
                        contentLength = Long.valueOf(header.getValue());
                        if (contentLength < 0) {
                            log.error("invalid contentlength, url {}, contentLength {}", url,
                                    header.getValue());
                            return null;
                        }
                        headAttr.fileSize = contentLength;
                    } catch (NumberFormatException e) {
                        log.error("invalid contentlength, url {}, contentLength {}", url,
                                header.getValue());
                        return null;
                    }
                }

                if (httpResponse.containsHeader("Last-Modified")) {
                    Header header = httpResponse.getFirstHeader("Last-Modified");
                    headAttr.lastModify = header.getValue();
                }

                Header[] allHeaders = httpResponse.getAllHeaders();
                final String ossUserMetaPrefix = "x-oss-meta-";
                final String awsUserMetaPrefix = "x-amz-meta-";
                for (Header headerElement : allHeaders) {
                    String headerName = headerElement.getName();
                    String headerValue = headerElement.getValue();
                    if (headerName.startsWith(ossUserMetaPrefix)
                            && !headerName.equals(ossUserMetaPrefix)) {
                        headAttr.userMetaMap.put(headerName.substring(ossUserMetaPrefix.length()),
                                headerValue);
                    } else if (headerName.startsWith(awsUserMetaPrefix)
                            && !headerName.equals(awsUserMetaPrefix)) {
                        headAttr.userMetaMap.put(headerName.substring(awsUserMetaPrefix.length()),
                                headerValue);
                    }
                }

                return headAttr;
            } catch (Exception e) {
                log.error("head file attr fail, url: {}, retry: {}/{}, exception: {}", url, retry,
                        maxRetryCount, e.toString());
                httpHead.abort();
                ++retry;
            }
        }
        return null;
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

    public HeadAttr downFile(String url, File localFile, boolean qiniuDownLoadFlag) {
        HeadAttr headAttr = new HeadAttr();
        boolean finished = false;
        int retry = 0;
        int maxRetryCount = 5;
        while (retry < maxRetryCount) {
            HttpGet httpGet = null;
            try {
                StringBuffer urlBuffer = new StringBuffer();
                URL encodeUrl = new URL(url);

                urlBuffer.append(encodeUrl.getProtocol()).append("://").append(encodeUrl.getHost());

                String encodeUrlStr = "";
                if (encodeUrl.getPath().startsWith("/")) {
                    encodeUrlStr = encodeUrl.getPath().substring(1);
                } else {
                    encodeUrlStr = encodeUrl.getPath();
                }
                encodeUrlStr = UrlEncoderUtils.encodeEscapeDelimiter(encodeUrlStr);
                if (qiniuDownLoadFlag) {
                    encodeUrlStr = encodeUrlStr.replaceAll("/", "%2f");
                }
                urlBuffer.append("/").append(encodeUrlStr);

                if (encodeUrl.getQuery() != null) {
                    urlBuffer.append("?").append(encodeUrl.getQuery());
                }

                httpGet = new HttpGet(urlBuffer.toString());

            } catch (MalformedURLException e) {
                log.error("downFile url fail, url:{}, msg:{}", url, e.getMessage());
                return null;
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
                    finished = true;
                } finally {
                    try {
                        if (bis != null) {
                            bis.close();
                        }
                        if (bos != null) {
                            bos.close();
                        }

                        // check file size
                        if (finished) {
                            if ((contentLength >= 0) && (localFile.length() != contentLength)) {
                                log.error("rsp content length:{}, local file length:{}",
                                        contentLength, localFile.length());
                                return null;
                            }
                            Header[] allHeaders = httpResponse.getAllHeaders();
                            final String ossUserMetaPrefix = "x-oss-meta-";
                            final String awsUserMetaPrefix = "x-amz-meta-";
                            final String etag = "ETag";
                            for (Header headerElement : allHeaders) {
                                String headerName = headerElement.getName();
                                String headerValue = headerElement.getValue();
                                if (headerName.startsWith(ossUserMetaPrefix)
                                        && !headerName.equals(ossUserMetaPrefix)) {
                                    headAttr.userMetaMap.put(
                                            headerName.substring(ossUserMetaPrefix.length()),
                                            headerValue);
                                } else if (headerName.startsWith(awsUserMetaPrefix)
                                        && !headerName.equals(awsUserMetaPrefix)) {
                                    headAttr.userMetaMap.put(
                                            headerName.substring(awsUserMetaPrefix.length()),
                                            headerValue);
                                } else if (headerName.equals(etag)) {
                                    headAttr.userMetaMap.put(headerName, headerValue);
                                }
                            }
                            return headAttr;
                        }

                    } catch (IOException e) {

                    }
                }
            } catch (Exception e) {
                log.error("download file failed, url: {}, retry: {}/{}, exception: {}", url, retry,
                        maxRetryCount, e.toString());
                httpGet.abort();
                localFile.delete();
            }
            ++retry;
        }
        return null;
    }

    public void shutdown() {
        this.idleConnectionMonitor.shutdown();
    }
}
