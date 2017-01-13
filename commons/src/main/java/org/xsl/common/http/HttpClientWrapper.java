package org.xsl.common.http;

import com.google.common.base.Charsets;
import org.apache.http.NameValuePair;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.*;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by xiongsenlin on 15/11/9.
 */
public class HttpClientWrapper {
    private CloseableHttpClient httpClient;
    private RequestConfig requestConfig;
    private HttpContext httpContext;
    private boolean chunked = false;
    private Map<String, String> headers;

    public HttpClientWrapper() {
        RequestConfig globalRequestConfig = RequestConfig.custom().
                setSocketTimeout(60000).
                build();

        CookieStore cookieStore = new BasicCookieStore();
        this.httpContext = new BasicHttpContext();
        this.httpContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        this.httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setDefaultRequestConfig(globalRequestConfig)
                .build();
    }

    /**
     * 允许最大连接数和单个路由最大的连接数
     * @param maxCon
     * @param maxPerRoute
     */
    public HttpClientWrapper(int maxCon, int maxPerRoute) {
        RequestConfig globalRequestConfig = RequestConfig.custom().
                setSocketTimeout(60000).
                build();

        CookieStore cookieStore = new BasicCookieStore();
        this.httpContext = new BasicHttpContext();
        this.httpContext.setAttribute(HttpClientContext.COOKIE_STORE, cookieStore);

        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
        this.httpClient = HttpClients.custom()
                .setConnectionManager(cm)
                .setMaxConnPerRoute(maxPerRoute)
                .setMaxConnTotal(maxCon)
                .setDefaultRequestConfig(globalRequestConfig)
                .build();
    }

    public void setSocketTimeout(int timeout) {
        this.requestConfig = RequestConfig.custom().
                setSocketTimeout(timeout).build();
    }

    public void setConnectionAndSocketTimeout(int cTimeout, int sTimeout) {
        this.requestConfig = RequestConfig.custom().
                setConnectionRequestTimeout(cTimeout).
                setSocketTimeout(sTimeout).
                build();
    }

    public void setChunked(boolean chunked) {
        this.chunked = chunked;
    }

    public void setHeaders(Map<String, String> headers) {
        this.headers = headers;
    }

    public HttpClientResult get(String url) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        InputStream inputStream = null;
        try {
            HttpGet get = new HttpGet(url);
            this.addHeaders(get);

            if (this.requestConfig != null) {
                get.setConfig(requestConfig);
            }

            response = this.httpClient.execute(get);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());

                if (response.getEntity() != null) {
                    inputStream = response.getEntity().getContent();
                    result.setMsg(this.getContent(inputStream));
                }
            }
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    public HttpClientResult getWithoutMsg(String url) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        try {
            HttpGet get = new HttpGet(url);
            this.addHeaders(get);

            if (this.requestConfig != null) {
                get.setConfig(requestConfig);
            }

            response = this.httpClient.execute(get);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
            }
        }
        finally {
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    public HttpClientResult post(String url, String body) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        InputStream inputStream = null;
        try {
            HttpPost post = new HttpPost(url);
            this.addHeaders(post);

            if (this.requestConfig != null) {
                post.setConfig(this.requestConfig);
            }

            StringEntity httpEntity = new StringEntity(body, Charsets.UTF_8);

            if (this.chunked) {
                httpEntity.setChunked(true);
            }

            post.setEntity(httpEntity);
            response = this.httpClient.execute(post);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
                if (response.getEntity() != null) {
                    inputStream = response.getEntity().getContent();
                    result.setMsg(this.getContent(inputStream));
                }
            }
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    /**
     * 只需获取返回状态，不需要获取具体的返回内容
     * @param url
     * @param body
     * @return
     * @throws Exception
     */
    public HttpClientResult postWithoutMsg(String url, String body) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        try {
            HttpPost post = new HttpPost(url);
            this.addHeaders(post);

            if (this.requestConfig != null) {
                post.setConfig(this.requestConfig);
            }

            StringEntity httpEntity = new StringEntity(body, Charsets.UTF_8);

            if (this.chunked) {
                httpEntity.setChunked(true);
            }

            post.setEntity(httpEntity);
            response = this.httpClient.execute(post);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
            }
        }
        finally {
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    public HttpClientResult post(String url, Map<String, String> param) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        InputStream inputStream = null;
        try {
            HttpPost post = new HttpPost(url);
            this.addHeaders(post);

            if (this.requestConfig != null) {
                post.setConfig(this.requestConfig);
            }

            List<NameValuePair> params = new ArrayList<>();
            for (Map.Entry<String, String> entry : param.entrySet()) {
                params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }

            UrlEncodedFormEntity urlEntity = new UrlEncodedFormEntity(params, "UTF-8");
            if (this.chunked) {
                urlEntity.setChunked(true);
            }

            post.setEntity(urlEntity);
            response = this.httpClient.execute(post);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
                if (response.getEntity() != null) {
                    inputStream = response.getEntity().getContent();
                    result.setMsg(this.getContent(inputStream));
                }
            }
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    public HttpClientResult postWithContext(String url, String json) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        InputStream inputStream = null;
        try {
            HttpPost post = new HttpPost(url);
            this.addHeaders(post);

            if (this.requestConfig != null) {
                post.setConfig(this.requestConfig);
            }

            StringEntity httpEntity = new StringEntity(json, Charsets.UTF_8);
            httpEntity.setContentType("application/json");

            if (this.chunked) {
                httpEntity.setChunked(true);
            }

            post.setEntity(httpEntity);
            response = this.httpClient.execute(post, this.httpContext);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
                if (response.getEntity() != null) {
                    inputStream = response.getEntity().getContent();
                    result.setMsg(this.getContent(inputStream));
                }
            }
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    public HttpClientResult postWithContext(String url, Map<String, String> param) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        InputStream inputStream = null;
        try {
            HttpPost post = new HttpPost(url);
            this.addHeaders(post);

            if (this.requestConfig != null) {
                post.setConfig(this.requestConfig);
            }

            List<NameValuePair> params = new ArrayList<>();
            for (Map.Entry<String, String> entry : param.entrySet()) {
                params.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }

            UrlEncodedFormEntity urlEntity = new UrlEncodedFormEntity(params, "UTF-8");
            if (this.chunked) {
                urlEntity.setChunked(true);
            }

            post.setEntity(urlEntity);
            response = this.httpClient.execute(post, this.httpContext);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
                if (response.getEntity() != null) {
                    inputStream = response.getEntity().getContent();
                    result.setMsg(this.getContent(inputStream));
                }
            }
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    public HttpClientResult put(String url, String body) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        InputStream inputStream = null;
        try {
            HttpPut put = new HttpPut(url);
            this.addHeaders(put);

            if (this.requestConfig != null) {
                put.setConfig(this.requestConfig);
            }

            StringEntity httpEntity = new StringEntity(body, Charsets.UTF_8);

            if (this.chunked) {
                httpEntity.setChunked(true);
            }

            put.setEntity(httpEntity);
            response = this.httpClient.execute(put);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
                if (response.getEntity() != null) {
                    inputStream = response.getEntity().getContent();
                    result.setMsg(this.getContent(inputStream));
                }
            }
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    public HttpClientResult delete(String url) throws Exception {
        HttpClientResult result = new HttpClientResult();

        CloseableHttpResponse response = null;
        InputStream inputStream = null;
        try {
            HttpDelete delete = new HttpDelete(url);
            this.addHeaders(delete);

            if (this.requestConfig != null) {
                delete.setConfig(this.requestConfig);
            }

            response = this.httpClient.execute(delete);

            if (response != null) {
                result.setCode(response.getStatusLine().getStatusCode());
                if (response.getEntity() != null) {
                    inputStream = response.getEntity().getContent();
                    result.setMsg(this.getContent(inputStream));
                }
            }
        }
        finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (response != null) {
                response.close();
            }
        }

        return result;
    }

    private String getContent(InputStream inputStream) throws Exception {
        if (inputStream == null) {
            return null;
        }

        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(inputStream, "utf-8"));
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line + "\n");
            }
            return sb.toString();
        }
        finally {
            if (reader != null) {
                reader.close();
            }
        }
    }

    public class HttpClientResult {
        private int code = -1;
        private String msg;

        public int getCode() {
            return code;
        }

        public void setCode(int code) {
            this.code = code;
        }

        public String getMsg() {
            return msg;
        }

        public void setMsg(String msg) {
            this.msg = msg;
        }
    }

    private void addHeaders(HttpRequestBase request) {
        if (this.headers != null && !this.headers.isEmpty()) {
            for (Map.Entry<String, String> entry : this.headers.entrySet()) {
                request.setHeader(entry.getKey(), entry.getValue());
            }
        }
    }

    public HttpClient getHttpClient() {
        return httpClient;
    }
}
