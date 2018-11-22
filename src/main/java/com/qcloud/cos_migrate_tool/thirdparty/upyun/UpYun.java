package com.qcloud.cos_migrate_tool.thirdparty.upyun;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.*;

public class UpYun {

    /**
     * SKD版本号
     */
    private final String VERSION = "2.0";

    /**
     * 路径的分割符
     */
    private final String SEPARATOR = "/";

    private final String AUTHORIZATION = "Authorization";
    private final String DATE = "Date";
    private final String CONTENT_LENGTH = "Content-Length";

    private final String CONTENT_MD5 = "Content-Md5";
    private final String X_UPYUN_FILE_TYPE = "x-upyun-file-type";
    private final String X_UPYUN_FILE_SIZE = "x-upyun-file-size";
    private final String X_UPYUN_FILE_DATE = "x-upyun-file-date";
    private final String X_UPYUN_LIST_ITER = "x-upyun-list-iter";

    private final String METHOD_HEAD = "HEAD";
    private final String METHOD_GET = "GET";
    private final String METHOD_DELETE = "DELETE";

    /**
     * 根据网络条件自动选择接入点:v0.api.upyun.com
     */
    public static final String ED_AUTO = "v0.api.upyun.com";
    /**
     * 电信接入点:v1.api.upyun.com
     */
    public static final String ED_TELECOM = "v1.api.upyun.com";
    /**
     * 联通网通接入点:v2.api.upyun.com
     */
    public static final String ED_CNC = "v2.api.upyun.com";
    /**
     * 移动铁通接入点:v3.api.upyun.com
     */
    public static final String ED_CTT = "v3.api.upyun.com";

    // 默认不开启debug模式
    public boolean debug = false;
    // 默认的超时时间：30秒
    private int timeout = 30 * 1000;
    // 默认为自动识别接入点
    private String apiDomain = ED_AUTO;

    // 空间名
    protected String bucketName = null;
    // 操作员名
    protected String userName = null;
    // 操作员密码
    protected String password = null;

    // 文件信息的参数
    protected String fileType = null;
    protected String fileSize = null;
    protected String fileDate = null;
    protected String fileMd5 = null;

    protected String listIter = null;

    private String proxyServer = null;
    private int proxyPort = 0;

    /**
     * 初始化 UpYun 存储接口
     *
     * @param bucketName 空间名称
     * @param userName   操作员名称
     * @param password   密码，不需要MD5加密
     * @return UpYun object
     */
    public UpYun(String bucketName, String userName, String password) {
        this.bucketName = bucketName;
        this.userName = userName;
        this.password = UpYunUtils.md5(password);
    }

    public void setProxy(String proxyServer, int proxyPort) {
        this.proxyServer = proxyServer;
        this.proxyPort = proxyPort;
    }

    /**
     * 切换 API 接口的域名接入点
     * <p>
     * 可选参数：<br>
     * 1) UpYun.ED_AUTO(v0.api.upyun.com)：默认，根据网络条件自动选择接入点 <br>
     * 2) UpYun.ED_TELECOM(v1.api.upyun.com)：电信接入点<br>
     * 3) UpYun.ED_CNC(v2.api.upyun.com)：联通网通接入点<br>
     * 4) UpYun.ED_CTT(v3.api.upyun.com)：移动铁通接入点
     *
     * @param domain 域名接入点
     */
    public void setApiDomain(String domain) {
        this.apiDomain = domain;
    }

    /**
     * 查看当前的域名接入点
     *
     * @return
     */
    public String getApiDomain() {
        return apiDomain;
    }

    /**
     * 设置连接超时时间，默认为30秒
     *
     * @param second 秒数，60即为一分钟超时
     */
    public void setTimeout(int second) {
        this.timeout = second * 1000;
    }

    /**
     * 查看当前的超时时间
     *
     * @return
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * 查看当前是否是debug模式
     *
     * @return
     */
    public boolean isDebug() {
        return debug;
    }

    /**
     * 设置是否开启debug模式
     *
     * @param debug
     */
    public void setDebug(boolean debug) {
        this.debug = debug;
    }

    /**
     * 获取当前SDK的版本号
     *
     * @return SDK版本号
     */
    public String version() {
        return VERSION;
    }

    /**
     * 获取某个子目录的占用量
     *
     * @param path 目标路径
     * @return 空间占用量，失败时返回 -1
     */
    @Deprecated
    public long getFolderUsage(String path) throws IOException, UpException {

        long usage = -1;

        String result = HttpAction(METHOD_GET, formatPath(path) + "/?usage", null, null);

        if (!isEmpty(result)) {

            try {
                usage = Long.parseLong(result.trim());
            } catch (NumberFormatException e) {
            }
        }

        return usage;
    }

    /**
     * 读取文件
     *
     * @param filePath 文件路径（包含文件名）
     * @param file     临时文件
     * @return true or false
     */
    public boolean readFile(String filePath, File file) throws IOException, UpException {

        String result = HttpAction(METHOD_GET, formatPath(filePath), null, file);

        return "".equals(result);
    }

    /**
     * 获取文件信息
     *
     * @param filePath 文件路径（包含文件名）
     * @return 文件信息 或 null
     */
    public FileInfo getFileInfo(String filePath) throws IOException, UpException {
        HttpAction(METHOD_HEAD, formatPath(filePath), null, null);

        // 判断是否存在文件信息
        if (isEmpty(fileType) && isEmpty(fileSize) && isEmpty(fileDate)) {
            return null;
        }

        return new FileInfo(fileType, Integer.parseInt(fileSize), fileDate, fileMd5);
    }

    /**
     * 读取目录列表
     *
     * @param path 目录路径
     * @return List<FolderItem> 或 null
     */
    public ReadDirResult readDir(String path, String nextIter, int limit) throws IOException, UpException {

        Map<String, String> params = new HashMap<String, String>();

        if (nextIter != null) {
            params.put(PARAMS.X_LIST_ITER.getValue(), nextIter);
        }
        if (limit > 0) {
            params.put(PARAMS.X_LIST_LIMIT.getValue(), Integer.toString(limit));
        }

        String result = HttpAction(METHOD_GET, formatPath(path) + SEPARATOR, params, null);

        if (isEmpty(result))
            return new ReadDirResult(null, new LinkedList<FolderItem>());

        List<FolderItem> list = new LinkedList<FolderItem>();

        String[] datas = result.split("\n");

        for (int i = 0; i < datas.length; i++) {
            if (datas[i].indexOf("\t") > 0) {
                list.add(new FolderItem(datas[i]));
            }
        }
        return new ReadDirResult(this.listIter, list);
    }

    /**
     * 获取 GMT 格式时间戳
     *
     * @return GMT 格式时间戳
     */
    private String getGMTDate() {
        SimpleDateFormat formater = new SimpleDateFormat(
                "EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.US);
        formater.setTimeZone(TimeZone.getTimeZone("GMT"));
        return formater.format(new Date());
    }

    /**
     * 连接处理逻辑
     *
     * @param method  请求方式 {GET, POST, PUT, DELETE}
     * @param uri     请求地址
     * @param datas   该请求所需发送数据（可为 null）
     * @param params  额外参数
     * @return 请求结果（字符串）或 null
     */
    private String HttpAction(String method, String uri, Map<String, String> params, File outFile) throws IOException, UpException {

        String result = null;

        HttpURLConnection conn = null;

        // 获取链接
        URL url = new URL("http://" + apiDomain + uri);

        if (this.proxyServer != null && this.proxyPort > 0) {
            Proxy proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress(this.proxyServer, this.proxyPort));
            conn = (HttpURLConnection) url.openConnection(proxy);
        } else {
            conn = (HttpURLConnection) url.openConnection();
        }


        // 设置必要参数
        conn.setConnectTimeout(timeout);
        conn.setRequestMethod(method);
        conn.setUseCaches(false);

        if (!method.equals(METHOD_DELETE) && !method.equals(METHOD_HEAD) && !method.equals(METHOD_GET)) {
            conn.setDoOutput(true);
        }

        String date = getGMTDate();

        // 设置时间
        conn.setRequestProperty(DATE, date);
        conn.setRequestProperty("User-Agent", UpYunUtils.VERSION);

        conn.setRequestProperty(CONTENT_LENGTH, "0");

        // 设置签名

        conn.setRequestProperty(AUTHORIZATION,
                UpYunUtils.sign(method, date, uri, userName, password, null));

        // 设置额外的参数
        if (params != null && !params.isEmpty()) {
            for (Map.Entry<String, String> param : params.entrySet()) {
                conn.setRequestProperty(param.getKey(), param.getValue());
            }
        }

        // 创建链接
        conn.connect();

        if (outFile == null) {
            result = getText(conn, METHOD_HEAD.equals(method));
        } else {
            result = "";
            OutputStream os = new FileOutputStream(outFile);
            byte[] data = new byte[1024 * 16];
            int temp = 0;
            InputStream is = conn.getInputStream();
            while ((temp = is.read(data)) != -1) {
                os.write(data, 0, temp);
            }
            try {
                os.close();
            } catch(Exception e) {
            }
        }
           
        if (conn != null) {
            conn.disconnect();
            conn = null;
        }
        return result;
    }

    /**
     * 获得连接请求的返回数据
     *
     * @param conn
     * @return 字符串
     */
    private String getText(HttpURLConnection conn, boolean isHeadMethod)
            throws IOException, UpAPIException {

        StringBuilder text = new StringBuilder();
        fileType = null;

        InputStream is = null;
        InputStreamReader sr = null;
        BufferedReader br = null;

        int code = conn.getResponseCode();

        try {
            is = conn.getInputStream();

            if (!isHeadMethod) {
                sr = new InputStreamReader(is);
                br = new BufferedReader(sr);

                char[] chars = new char[4096];
                int length = 0;

                while ((length = br.read(chars)) != -1) {
                    text.append(chars, 0, length);
                }
            }

            if (200 == code && conn.getHeaderField(X_UPYUN_FILE_TYPE) != null) {
                fileType = conn.getHeaderField(X_UPYUN_FILE_TYPE);
                fileSize = conn.getHeaderField(X_UPYUN_FILE_SIZE);
                fileDate = conn.getHeaderField(X_UPYUN_FILE_DATE);
                fileMd5 = conn.getHeaderField(CONTENT_MD5);
            } else {
                fileType = fileSize = fileDate = null;
            }

            if (200 == code && conn.getHeaderField(X_UPYUN_LIST_ITER) != null) {
                listIter = conn.getHeaderField(X_UPYUN_LIST_ITER);
            } else {
                listIter = null;
            }
        } finally {
            if (br != null) {
                br.close();
                br = null;
            }
            if (sr != null) {
                sr.close();
                sr = null;
            }
            if (is != null) {
                is.close();
                is = null;
            }
        }

        if (isHeadMethod) {
            if (code >= 400)
                return null;
            return "";
        }

        if (code >= 400)
            throw new UpAPIException(code, text.toString());

        return text.toString();
    }

    /**
     * 判断字符串是否为空
     * getTextgetText
     *
     * @param str
     * @return 是否为空
     */
    private boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * 格式化路径参数，去除前后的空格并确保以"/"开头，最后添加"/空间名"
     * <p>
     * 最终构成的格式："/空间名/文件路径"
     *
     * @param path 目录路径或文件路径
     * @return 格式化后的路径
     */
    private String formatPath(String path) {

        if (!isEmpty(path)) {

            // 去除前后的空格
            path = path.trim();

            // 确保路径以"/"开头
            if (!path.startsWith(SEPARATOR)) {
                return SEPARATOR + bucketName + SEPARATOR + path;
            }
        }

        return SEPARATOR + bucketName + path;
    }

    public class FileInfo {
        public String type;
        public int size;
        public String date;
        public String md5;

        public FileInfo(String type, int size, String date, String md5) {
            this.type = type;
            this.size = size;
            this.date = date;
            this.md5 = md5;
        }
    }

    public class ReadDirResult {
        public String nextIter;
        public List<FolderItem> items;

        public ReadDirResult(String nextIter, List<FolderItem> items) {
            this.nextIter = nextIter;
            this.items = items;
        }
    }

    public class FolderItem {
        // 文件名
        public String name;

        // 文件类型 {file, folder}
        public String type;

        // 文件大小
        public long size;

        // 文件日期
        public Date date;

        public FolderItem(String data) {
            String[] a = data.split("\t");
            if (a.length == 4) {
                this.name = a[0];
                this.type = ("N".equals(a[1]) ? "File" : "Folder");
                try {
                    this.size = Long.parseLong(a[2].trim());
                } catch (NumberFormatException e) {
                    this.size = -1;
                }
                long da = 0;
                try {
                    da = Long.parseLong(a[3].trim());
                } catch (NumberFormatException e) {
                }
                this.date = new Date(da * 1000);
            }
        }

        @Override
        public String toString() {
            return "time = " + date + "  size = " + size + "  type = " + type
                    + "  name = " + name;
        }
    }

    /**
     * 其他额外参数的键值和参数值
     */
    public enum PARAMS {
        X_LIST_ITER("x-list-iter"),
        X_LIST_LIMIT("x-list-limit");

        private final String value;

        private PARAMS(String val) {
            value = val;
        }

        public String getValue() {
            return value;
        }
    }
}
