/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2012 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks Inc
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
 * ====================================================================
 */
package org.dasein.cloud.cloudsigma;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.StatusLine;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
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
 * Handles communication with the CloudSigma REST endpoint by abstracting out the specifics of authentication and
 * HTTP negotiation.
 * <p>Created by George Reese: 10/25/12 7:43 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.02
 */
public class CloudSigmaMethod {
    static private final Logger logger = CloudSigma.getLogger(CloudSigmaMethod.class);
    static private final Logger wire   = CloudSigma.getWireLogger(CloudSigmaMethod.class);

    static public final String M_PROFILE_INFO = "/profile/info";

    /**
     * 200	OK	Command succeeded, data returned (possibly 0 length)
     */
    static public final int OK             = 200;
    /**
     * 204	No Content	Command succeeded, no data returned (by definition)
     */
    static public final int NO_CONTENT     = 204;

    /**
     * 404	Not Found	Command, drive, server or other object not found
     */
    static public final int NOT_FOUND      = 404;


    static public @Nullable String seekValue(@Nonnull String body, @Nonnull String key) {
        body = body.trim();
        if( body.length() > 0 ) {
            String[] lines = body.split("\n");

            for( String line : lines ) {
                line = line.trim();
                String[] components = line.split("\t");

                if( components.length < 2 ) {
                    if( line.equals(key) ) {
                        return line;
                    }
                }
                else if( components[0].equals(key) ) {
                    return components[1];
                }
            }
        }
        return null;
    }

    static public @Nonnull Map<String,String> toMap(@Nonnull String body) {
        HashMap<String,String> values = new HashMap<String, String>();

        body = body.trim();
        if( body.length() > 0 ) {
            String[] lines = body.split("\n");

            for( String line : lines ) {
                line = line.trim();
                String[] components = line.split("\t");

                if( components.length < 2 ) {
                    values.put(line, null);
                }
                else {
                    values.put(components[0], components[1]);
                }
            }
        }
        return values;
    }

    private CloudSigma provider;

    public CloudSigmaMethod(@Nonnull CloudSigma provider) { this.provider = provider; }

    public @Nullable Map<String,String> getObject(@Nonnull String resource) throws InternalException, CloudException {
        String body = getString(resource);

        if( body == null || body.trim().length() < 1 ) {
            return null;
        }
        return toMap(body);
    }

    public @Nullable String getString(@Nonnull String resource) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + CloudSigma.class.getName() + ".getString(" + resource + ")");
        }

        try {
            String target = getEndpoint(resource);

            if( wire.isDebugEnabled() ) {
                wire.debug("");
                wire.debug(">>> [GET (" + (new Date()) + ")] -> " + target + " >--------------------------------------------------------------------------------------");
            }
            try {
                HttpClient client = getClient();

                try {
                    HttpGet get = new HttpGet(target);

                    if( wire.isDebugEnabled() ) {
                        wire.debug(get.getRequestLine().toString());
                        for( Header header : get.getAllHeaders() ) {
                            wire.debug(header.getName() + ": " + header.getValue());
                        }
                        wire.debug("");
                    }
                    HttpResponse response;
                    StatusLine status;

                    try {
                        response = client.execute(get);
                        status = response.getStatusLine();
                    }
                    catch( IOException e ) {
                        logger.error("Failed to execute HTTP request due to a cloud I/O error: " + e.getMessage());
                        throw new CloudException(e);
                    }
                    if( logger.isDebugEnabled() ) {
                        logger.debug("HTTP Status " + status);
                    }
                    Header[] headers = response.getAllHeaders();

                    if( wire.isDebugEnabled() ) {
                        wire.debug(status.toString());
                        for( Header h : headers ) {
                            if( h.getValue() != null ) {
                                wire.debug(h.getName() + ": " + h.getValue().trim());
                            }
                            else {
                                wire.debug(h.getName() + ":");
                            }
                        }
                        wire.debug("");
                    }
                    if( status.getStatusCode() == NOT_FOUND ) {
                        return null;
                    }
                    if( status.getStatusCode() != OK && status.getStatusCode() != NO_CONTENT ) {
                        logger.error("Expected OK for GET request, got " + status.getStatusCode());
                        HttpEntity entity = response.getEntity();
                        String body;

                        if( entity == null ) {
                            throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), status.getReasonPhrase());
                        }
                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new CloudSigmaException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        throw new CloudSigmaException(CloudErrorType.GENERAL, status.getStatusCode(), status.getReasonPhrase(), body);
                    }
                    else {
                        HttpEntity entity = response.getEntity();

                        if( entity == null ) {
                            return null;
                        }
                        String body;

                        try {
                            body = EntityUtils.toString(entity);
                        }
                        catch( IOException e ) {
                            throw new CloudSigmaException(e);
                        }
                        if( wire.isDebugEnabled() ) {
                            wire.debug(body);
                        }
                        wire.debug("");
                        return body;
                    }
                }
                finally {
                    try { client.getConnectionManager().shutdown(); }
                    catch( Throwable ignore ) { }
                }
            }
            finally {
                if( wire.isDebugEnabled() ) {
                    wire.debug("<<< [GET (" + (new Date()) + ")] -> " + target + " <--------------------------------------------------------------------------------------");
                    wire.debug("");
                }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + CloudSigma.class.getName() + ".getString()");
            }
        }
    }

    private @Nonnull HttpClient getClient() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String endpoint = ctx.getEndpoint();

        if( endpoint == null ) {
            throw new CloudSigmaConfigurationException("No cloud endpoint was defined");
        }
        boolean ssl = endpoint.startsWith("https");
        int targetPort;
        URI uri;

        try {
            uri = new URI(endpoint);
            targetPort = uri.getPort();
            if( targetPort < 1 ) {
                targetPort = (ssl ? 443 : 80);
            }
        }
        catch( URISyntaxException e ) {
            throw new CloudSigmaConfigurationException(e);
        }
        HttpHost targetHost = new HttpHost(uri.getHost(), targetPort, uri.getScheme());
        HttpParams params = new BasicHttpParams();

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        HttpProtocolParams.setUserAgent(params, "");

        Properties p = ctx.getCustomProperties();

        if( p != null ) {
            String proxyHost = p.getProperty("proxyHost");
            String proxyPort = p.getProperty("proxyPort");

            if( proxyHost != null ) {
                int port = 0;

                if( proxyPort != null && proxyPort.length() > 0 ) {
                    port = Integer.parseInt(proxyPort);
                }
                params.setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, port, ssl ? "https" : "http"));
            }
        }
        DefaultHttpClient client = new DefaultHttpClient(params);

        try {
            String userName = new String(ctx.getAccessPublic(), "utf-8");
            String password = new String(ctx.getAccessPrivate(), "utf-8");

            client.getCredentialsProvider().setCredentials(new AuthScope(targetHost.getHostName(), targetHost.getPort()), new UsernamePasswordCredentials(userName, password));
        }
        catch( UnsupportedEncodingException e ) {
            throw new InternalException(e);
        }
        return client;
    }

    private @Nonnull String getEndpoint(@Nonnull String resource) throws NoContextException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String endpoint = ctx.getEndpoint();

        if( endpoint == null || endpoint.trim().equals("") ) {
            endpoint = "https://api.zrh.cloudsigma.com";
        }
        if( resource.startsWith("/") ) {
            while( endpoint.endsWith("/") && !endpoint.equals("/") ) {
                endpoint = endpoint.substring(0, endpoint.length()-1);
            }
            return endpoint + resource;
        }
        else if( endpoint.endsWith("/") ) {
            return endpoint + resource;
        }
        return (endpoint + "/" + resource);
    }
}
