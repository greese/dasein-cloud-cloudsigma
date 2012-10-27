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
package org.dasein.cloud.cloudsigma.network;

import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.network.ip.StaticIPSupport;
import org.dasein.cloud.cloudsigma.network.vlan.ServerVLANSupport;
import org.dasein.cloud.network.AbstractNetworkServices;

import javax.annotation.Nonnull;

/**
 * Implements the various network services supported in CloudSigma.
 * <p>Created by George Reese: 10/26/12 12:28 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class CloudSigmaNetworkServices extends AbstractNetworkServices {
    private CloudSigma provider;

    public CloudSigmaNetworkServices(@Nonnull CloudSigma provider) { this.provider = provider; }

    @Override
    public @Nonnull StaticIPSupport getIpAddressSupport() {
        return new StaticIPSupport(provider);
    }

    @Override
    public @Nonnull ServerVLANSupport getVlanSupport() {
        return new ServerVLANSupport(provider);
    }
}
