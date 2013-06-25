/**
 * Copyright (C) 2012-2013 Dell, Inc.
 * See annotations for authorship information
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.cloudsigma;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Handles communication with the CloudSigma REST endpoint v. 2.0 by abstracting out the specifics of authentication and
 * HTTP negotiation.
 * <p>Created by Danielle Mayne: 02/15/13 12:38 PM</p>
 * @author George Reese
 * @author Danielle Mayne
 * @version 2013.02 initial version
 * @since 2013.02
 */
public class CloudSigmaMethod {
    static private final Logger logger = CloudSigma.getLogger(CloudSigmaMethod.class);
    static private final Logger wire = CloudSigma.getWireLogger(CloudSigmaMethod.class);

    /**
     * 200	OK	Command succeeded, data returned (possibly 0 length)
     */
    static public final int OK = 200;

    /**
     * 202	ACCEPTED	Command accepted for asynchronous processing, data returned (possibly 0 length)
     */
    static public final int CREATED = 201;

    /**
     * 202	ACCEPTED	Command accepted for asynchronous processing, data returned (possibly 0 length)
     */
    static public final int ACCEPTED = 202;

    /**
     * 204	No Content	Command succeeded, no data returned (by definition)
     */
    static public final int NO_CONTENT = 204;

    /**
     * 400	Bad Request
     */
    static public final int BAD_REQUEST = 400;

    /**
     * 404	Not Found	Command, drive, server or other object not found
     */
    static public final int NOT_FOUND = 404;


    static public @Nullable String seekValue(@Nonnull String body, @Nonnull String key) {
        //dmayne 20130218: use JSON parsing rather than plain text
        body = body.trim();
        if (body.length() > 0) {
            try {
                JSONObject obj = new JSONObject(body);
                if (obj != null) {
                    JSONObject json = (JSONObject) obj;
                    return json.getString(key);

                }
            }
            catch (JSONException e) {
                logger.error("Exception getting value: "+e.getMessage());
            }
        }
        return null;
    }

    static public @Nonnull Map<String, String> toMap(@Nonnull String body) {
        HashMap<String, String> values = new HashMap<String, String>();

        body = body.trim();
        if (body.length() > 0) {
            String[] lines = body.split("\n");

            for (String line : lines) {
                line = line.trim();

                int idx = line.indexOf(" ");

                if (idx == -1) {
                    values.put(line, null);
                } else {
                    String k = line.substring(0, idx);

                    values.put(k, line.substring(idx + 1));
                }
            }
        }
        return values;
    }

    private CloudSigma provider;

    public CloudSigmaMethod(@Nonnull CloudSigma provider) {
        this.provider = provider;
    }

    public @Nullable Map<String, String> getObject(@Nonnull String resource) throws InternalException, CloudException {
        String body = getString(resource);

        if (body == null || body.trim().length() < 1) {
            return null;
        }
        return toMap(body);
    }

    public @Nullable String getString(@Nonnull String resource) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + CloudSigma.class.getName() + ".getString(" + resource + ")");
        }

        try {
            String target = getEndpoint(resource);

            if (wire.isDebugEnabled()) {
                wire.debug("");
                wire.debug(">>> [GET (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    throw new CloudSigmaConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if (ctx == null) {
                        throw new NoContextException();
                    }
                    HttpGet get = new HttpGet(target);
                    String auth;

                    try {
                        String userName = new String(ctx.getAccessPublic(), "utf-8");
                        String password = new String(ctx.getAccessPrivate(), "utf-8");

                        auth = new String(Base64.encodeBase64((userName + ":" + password).getBytes()));
                    } catch (UnsupportedEncodingException e) {
                        throw new InternalException(e);
                    }
                    //dmayne 20130218: add JSON headers
                    get.addHeader("Host", uri.getHost());
                    get.addHeader("Content-Type", "application/json; charset=utf-8");
                    get.addHeader("Accept", "application/json");
                    get.addHeader("Authorization", "Basic " + auth);

                    if (wire.isDebugEnabled()) {
                        wire.debug(get.getRequestLine().toString());
                        for (Header header : get.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        response = client.execute(get);
                        status = response.getStatusLine();
                    } catch (IOException e) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if (wire.isDebugEnabled()) {
                        wire.debug(status.toString());
                        for (Header h : headers) {
                            if (h.getValue() != null) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            } else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if (status.getStatusCode() == NOT_FOUND) {
                        return null;
                    }
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT) {
                        logger.error("Expected OK for GET request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();
                        String body;

                        if (entity == null) {
                            throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        if (status.getStatusCode() == BAD_REQUEST && body.contains("could not be found")) {
                            return null;
                        }
                        throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    } else {
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            return "";
                        }
                        String body;

                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                } finally {
                    try {
                        client.getConnectionManager().shutdown();
                    } catch (Throwable ignore) {
                    }
                }
            } finally {
                if (wire.isDebugEnabled()) {
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + CloudSigma.class.getName() + ".getString()");
            }
        }
    }

    private @Nonnull HttpClient getClient(URI uri) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if (ctx == null) {
            throw new NoContextException();
        }
        boolean ssl = uri.getScheme().startsWith("https");
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        //noinspection deprecation
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "");

        Properties p = ctx.getCustomProperties();

        if (p != null) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if (proxyHost != null) {
                int port = 0;

                if (proxyPort != null && proxyPort.length() > 0) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        return new DefaultHttpClient(params);
    }

    private @Nonnull String getEndpoint(@Nonnull String resource) throws NoContextException {
        ProviderContext ctx = provider.getContext();
        String target = "";

        if (ctx == null) {
            throw new NoContextException();
        }
        String endpoint = ctx.getEndpoint();

        if (endpoint == null || endpoint.trim().equals("")) {
            //dmayne 20130218: new default endpoint for v 2.0
            endpoint = "https://lvs.cloudsigma.com/api/2.0/";
        }
        if (resource.startsWith("/")) {
            while (endpoint.endsWith("/") && !endpoint.equals("/")) {
                endpoint = endpoint.substring(0, endpoint.length() - 1);
            }
            target = endpoint + resource;
        } else if (endpoint.endsWith("/")) {
            target = endpoint + resource;
        }
        else {
            target = endpoint + "/" + resource;
        }

        return target;
    }

    public @Nullable JSONObject list(@Nonnull String resource) throws InternalException, CloudException {
        //dmayne 20130218: use JSON parsing
        String body = getString(resource);

        if (body == null) {
            return null;
        }

        try {
            JSONObject obj = new JSONObject(body);

            if (obj != null) {
                return (JSONObject) obj;
            }
        }
        catch (JSONException e) {
            throw  new InternalException(e);
        }
        return null;
    }

    public @Nullable Map<String, String> postObject(@Nonnull String resource, @Nonnull String body) throws InternalException, CloudException {
        String response = postString(resource, body);

        if (response == null || response.trim().length() < 1) {
            return null;
        }
        return toMap(response);
    }

    public @Nullable String postString(@Nonnull String resource, @Nonnull String body) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + CloudSigma.class.getName() + ".postString(" + resource + "," + body + ")");
        }

        try {
            String target = getEndpoint(resource);

            if (wire.isDebugEnabled()) {
                wire.debug("");
                wire.debug(">>> [POST (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    throw new CloudSigmaConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if (ctx == null) {
                        throw new NoContextException();
                    }
                    HttpPost post = new HttpPost(target);
                    String auth;

                    try {
                        String userName = new String(ctx.getAccessPublic(), "utf-8");
                        String password = new String(ctx.getAccessPrivate(), "utf-8");

                        auth = new String(Base64.encodeBase64((userName + ":" + password).getBytes()));
                    } catch (UnsupportedEncodingException e) {
                        throw new InternalException(e);
                    }
                    //dmayne 20130218: add new JSON headers
                    post.addHeader("Host", uri.getHost());
                    post.addHeader("Content-Type", "application/json; charset=utf-8");
                    post.addHeader("Accept", "application/json");
                    post.addHeader("Authorization", "Basic " + auth);
                    try {
                        post.setEntity(new StringEntity(body, "utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        logger.error("Unsupported encoding UTF-8: " + e.getMessage());
                        throw new InternalException(e);
                    }

                    if (wire.isDebugEnabled()) {
                        wire.debug(post.getRequestLine().toString());
                        for (Header header : post.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug(body);
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        response = client.execute(post);
                        status = response.getStatusLine();
                    } catch (IOException e) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if (wire.isDebugEnabled()) {
                        wire.debug(status.toString());
                        for (Header h : headers) {
                            if (h.getValue() != null) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            } else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if (status.getStatusCode() == NOT_FOUND) {
                        return null;
                    }
                    //dmayne 20130227: creating server returns accepted (202) response
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT && status.getStatusCode() != CREATED && status.getStatusCode() != ACCEPTED) {
                        logger.error("Expected OK for POST request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    } else {
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            return "";
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                } finally {
                    try {
                        client.getConnectionManager().shutdown();
                    } catch (Throwable ignore) {
                    }
                }
            } finally {
                if (wire.isDebugEnabled()) {
                    wire.debug("<<< [POST (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + CloudSigma.class.getName() + ".postString()");
            }
        }
    }

    public @Nullable String putString(@Nonnull String resource, @Nonnull String body) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + CloudSigma.class.getName() + ".putString(" + resource + "," + body + ")");
        }

        try {
            String target = getEndpoint(resource);

            if (wire.isDebugEnabled()) {
                wire.debug("");
                wire.debug(">>> [PUT (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    throw new CloudSigmaConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if (ctx == null) {
                        throw new NoContextException();
                    }
                    HttpPut put = new HttpPut(target);
                    String auth;

                    try {
                        String userName = new String(ctx.getAccessPublic(), "utf-8");
                        String password = new String(ctx.getAccessPrivate(), "utf-8");

                        auth = new String(Base64.encodeBase64((userName + ":" + password).getBytes()));
                    } catch (UnsupportedEncodingException e) {
                        throw new InternalException(e);
                    }
                    //dmayne 20130218: add new JSON headers
                    put.addHeader("Host", uri.getHost());
                    put.addHeader("Content-Type", "application/json; charset=utf-8");
                    put.addHeader("Accept", "application/json");
                    put.addHeader("Authorization", "Basic " + auth);
                    try {
                        put.setEntity(new StringEntity(body, "utf-8"));
                    } catch (UnsupportedEncodingException e) {
                        logger.error("Unsupported encoding UTF-8: " + e.getMessage());
                        throw new InternalException(e);
                    }

                    if (wire.isDebugEnabled()) {
                        wire.debug(put.getRequestLine().toString());
                        for (Header header : put.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug(body);
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        response = client.execute(put);
                        status = response.getStatusLine();
                    } catch (IOException e) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if (wire.isDebugEnabled()) {
                        wire.debug(status.toString());
                        for (Header h : headers) {
                            if (h.getValue() != null) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            } else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if (status.getStatusCode() == NOT_FOUND) {
                        return null;
                    }
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT && status.getStatusCode() != CREATED && status.getStatusCode() != ACCEPTED) {
                        logger.error("Expected OK for PUT request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    } else {
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            return "";
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                } finally {
                    try {
                        client.getConnectionManager().shutdown();
                    } catch (Throwable ignore) {
                    }
                }
            } finally {
                if (wire.isDebugEnabled()) {
                    wire.debug("<<< [PUT (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + CloudSigma.class.getName() + ".putString()");
            }
        }
    }

    public @Nullable String deleteString(@Nonnull String resource, @Nonnull String body) throws InternalException, CloudException {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + CloudSigma.class.getName() + ".deleteString(" + resource + "," + body + ")");
        }

        try {
            String target = getEndpoint(resource);

            if (wire.isDebugEnabled()) {
                wire.debug("");
                wire.debug(">>> [DELETE (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                URI uri;

                try {
                    uri = new URI(target);
                } catch (URISyntaxException e) {
                    throw new CloudSigmaConfigurationException(e);
                }
                HttpClient client = getClient(uri);

                try {
                    ProviderContext ctx = provider.getContext();

                    if (ctx == null) {
                        throw new NoContextException();
                    }
                    HttpDelete delete = new HttpDelete(target);
                    String auth;

                    try {
                        String userName = new String(ctx.getAccessPublic(), "utf-8");
                        String password = new String(ctx.getAccessPrivate(), "utf-8");

                        auth = new String(Base64.encodeBase64((userName + ":" + password).getBytes()));
                    } catch (UnsupportedEncodingException e) {
                        throw new InternalException(e);
                    }
                    //dmayne 20130218: add new JSON headers
                    delete.addHeader("Host", uri.getHost());
                    delete.addHeader("Content-Type", "application/json; charset=utf-8");
                    delete.addHeader("Accept", "application/json");
                    delete.addHeader("Authorization", "Basic " + auth);

                    if (wire.isDebugEnabled()) {
                        wire.debug(delete.getRequestLine().toString());
                        for (Header header : delete.getAllHeaders()) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                        wire.debug(body);
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        response = client.execute(delete);
                        status = response.getStatusLine();
                    } catch (IOException e) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if (wire.isDebugEnabled()) {
                        wire.debug(status.toString());
                        for (Header h : headers) {
                            if (h.getValue() != null) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            } else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if (status.getStatusCode() == NOT_FOUND) {
                        return null;
                    }
                    if (status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT) {
                        logger.error("Expected OK for DELETE request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    } else {
                        HttpEntity entity = response.getEntity();

                        if (entity == null) {
                            return "";
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        } catch (IOException e) {
                            throw new CloudSigmaException(e);
                        }
                        if (wire.isDebugEnabled()) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                } finally {
                    try {
                        client.getConnectionManager().shutdown();
                    } catch (Throwable ignore) {
                    }
                }
            } finally {
                if (wire.isDebugEnabled()) {
                    wire.debug("<<< [DELETE (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + CloudSigma.class.getName() + ".deleteString()");
            }
        }
    }
}
