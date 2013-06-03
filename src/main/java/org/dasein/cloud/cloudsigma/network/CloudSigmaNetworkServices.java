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

package org.dasein.cloud.cloudsigma.network;

import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.network.ip.StaticIPSupport;
import org.dasein.cloud.cloudsigma.network.vlan.ServerVLANSupport;
import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.cloudsigma.network.firewall.ServerFirewallSupport;

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

    /*@Override
    public @Nonnull ServerFirewallSupport getFirewallSupport() {
        return new ServerFirewallSupport(provider);
    } */
}
