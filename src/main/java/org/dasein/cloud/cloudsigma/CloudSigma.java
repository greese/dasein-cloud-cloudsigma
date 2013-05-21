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

import org.apache.log4j.Logger;
import org.dasein.cloud.AbstractCloud;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.cloudsigma.compute.CloudSigmaComputeServices;
import org.dasein.cloud.cloudsigma.network.CloudSigmaNetworkServices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Support for the CloudSigma cloud using their 2.0 API. This implementation owes almost everything to the work done by
 * George Reese in prior support for Dasein Cloud CloudSigma.
 * <p>Created by Danielle Mayne: 02/15/13 12:30 PM</p>
 * @author George Reese
 * @author Danielle Mayne
 * @version 2013.02 initial version
 * @since 2013.02
 */
public class CloudSigma extends AbstractCloud {
    static private final Logger logger = getLogger(CloudSigma.class);

    static private @Nonnull String getLastItem(@Nonnull String name) {
        int idx = name.lastIndexOf('.');

        if (idx < 0) {
            return name;
        } else if (idx == (name.length() - 1)) {
            return "";
        }
        return name.substring(idx + 1);
    }

    static public @Nonnull Logger getLogger(@Nonnull Class<?> cls) {
        String pkg = getLastItem(cls.getPackage().getName());

        if (pkg.equals("cloudsigma")) {
            pkg = "";
        } else {
            pkg = pkg + ".";
        }
        return Logger.getLogger("dasein.cloud.cloudsigma.std." + pkg + getLastItem(cls.getName()));
    }

    static public @Nonnull Logger getWireLogger(@Nonnull Class<?> cls) {
        return Logger.getLogger("dasein.cloud.cloudsigma.wire." + getLastItem(cls.getPackage().getName()) + "." + getLastItem(cls.getName()));
    }

    public CloudSigma() {
    }

    @Override
    public @Nonnull String getCloudName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getCloudName());

        return (name == null ? "CloudSigma2" : name);
    }

    @Override
    public @Nonnull CloudSigmaComputeServices getComputeServices() {
        return new CloudSigmaComputeServices(this);
    }

    @Override
    public @Nonnull CloudSigmaDataCenterServices getDataCenterServices() {
        return new CloudSigmaDataCenterServices(this);
    }

    @Override
    public @Nonnull CloudSigmaNetworkServices getNetworkServices() {
        return new CloudSigmaNetworkServices(this);
    }

    @Override
    public @Nonnull String getProviderName() {
        ProviderContext ctx = getContext();
        String name = (ctx == null ? null : ctx.getProviderName());

        return (name == null ? "CloudSigma2" : name);
    }

    @Override
    public @Nullable String testContext() {
        if (logger.isTraceEnabled()) {
            logger.trace("ENTER - " + CloudSigma.class.getName() + ".testContext()");
        }
        try {
            ProviderContext ctx = getContext();

            if (ctx == null) {
                logger.warn("No context was provided for testing");
                return null;
            }
            try {
                CloudSigmaMethod method = new CloudSigmaMethod(this);
                //dmayne 20130218: amended to use new API call
                String body = method.getString("profile/");

                if (body == null) {
                    return null;
                }
                String uuid = CloudSigmaMethod.seekValue(body, "uuid");
                if (logger.isDebugEnabled()) {
                    logger.debug("UUID=" + uuid);
                }
                if (uuid == null) {
                    logger.warn("No valid UUID was provided in the response during context testing");
                    return null;
                }
                return uuid;
            } catch (Throwable t) {
                logger.error("Error testing CloudSigma credentials for " + ctx.getAccountNumber() + ": " + t.getMessage());
                return null;
            }
        } finally {
            if (logger.isTraceEnabled()) {
                logger.trace("EXIT - " + CloudSigma.class.getName() + ".textContext()");
            }
        }
    }
}