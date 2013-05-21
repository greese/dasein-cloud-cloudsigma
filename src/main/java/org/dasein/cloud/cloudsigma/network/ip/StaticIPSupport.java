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

package org.dasein.cloud.cloudsigma.network.ip;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.CloudSigmaConfigurationException;
import org.dasein.cloud.cloudsigma.CloudSigmaMethod;
import org.dasein.cloud.cloudsigma.NoContextException;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.AddressType;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.IpForwardingRule;
import org.dasein.cloud.network.Protocol;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * Support for static IP addresses in CloudSigma.
 * <p>Created by Danielle Mayne: 02/20/13 13:56 PM</p>
 * @author George Reese
 * @author Danielle Mayne
 * @version 2013.02 initial version
 * @since 2013.02
 */
public class StaticIPSupport implements IpAddressSupport {
    static private final Logger logger = CloudSigma.getLogger(StaticIPSupport.class);

    private CloudSigma provider;

    public StaticIPSupport(@Nonnull CloudSigma provider) {
        this.provider = provider;
    }

    @Override
    public void assign(@Nonnull String addressId, @Nonnull String serverId) throws InternalException, CloudException {
        IpAddress address = getIpAddress(addressId);

        if (address == null) {
            throw new CloudException("No such IP address: " + address);
        }
        provider.getComputeServices().getVirtualMachineSupport().assignIP(serverId, address);
    }

    @Override
    public void assignToNetworkInterface(@Nonnull String addressId, @Nonnull String nicId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("NICs are not supported");
    }

    @Override
    public @Nonnull String forward(@Nonnull String addressId, int publicPort, @Nonnull Protocol protocol, int privatePort, @Nonnull String onServerId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("IP forwarding not supported");
    }

    @Override
    public IpAddress getIpAddress(@Nonnull String addressId) throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        try {
            String object = (method.getString(toAddressURL(addressId, "")));
            if (object != null) {
                return toIP(new JSONObject(object), false);
            }
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForIpAddress(@Nonnull Locale locale) {
        return "static IP";
    }

    @Override
    public @Nonnull Requirement identifyVlanForVlanIPRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isAssigned(@Nonnull AddressType type) {
        return type.equals(AddressType.PUBLIC);
    }

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
        //todo dmayne 20130221: should ipv6 be added here?
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean isAssignablePostLaunch(@Nonnull IPVersion version) throws CloudException, InternalException {
        //todo dmayne 20130221: should ipv6 be added here?
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean isForwarding() {
        return false;
    }

    @Override
    public boolean isForwarding(IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull AddressType type) {
        //dmayne 20130221: requesting ip addresses is not supported in api 2.0
        //return type.equals(AddressType.PUBLIC);
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        //dmayne 20130221: requesting ip addresses is not supported in api 2.0
        //return version.equals(IPVersion.IPV4);
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
    }

    @Override
    public @Nonnull Iterable<IpAddress> listPrivateIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IpAddress> listPublicIpPool(boolean unassignedOnly) throws InternalException, CloudException {
        //todo dmayne 20130221: should ipv6 be added here?
        return listIpPool(IPVersion.IPV4, unassignedOnly);
    }

    @Override
    public @Nonnull Iterable<IpAddress> listIpPool(@Nonnull IPVersion version, boolean unassignedOnly) throws InternalException, CloudException {
        if (version.equals(IPVersion.IPV4)) {

            ArrayList<IpAddress> addresses = new ArrayList<IpAddress>();
            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            boolean moreData = true;
            String baseTarget = "/ips/detail/";
            String target = "";

            while(moreData)  {
                //dmayne 20130218: JSON Parsing
                logger.debug("Target "+target);
                target = baseTarget+target;
                logger.debug("final target "+target);

                JSONObject pool = method.list(target);

                if (pool == null) {
                    throw new CloudException("Unable to communicate with CloudSigma endpoint");
                }
                //dmayne 20130218: use JSON parsing
                try {
                    JSONArray objects = pool.getJSONArray("objects");
                    for (int i = 0; i < objects.length(); i++) {
                        JSONObject jObj = objects.getJSONObject(i);

                        IpAddress address = toIP(jObj, unassignedOnly);

                        if (address != null) {
                            addresses.add(address);
                        }
                    }

                    //dmayne 20130314: check if there are more pages
                    if (pool.has("meta")) {
                        logger.debug("Found meta tag");
                        JSONObject meta = pool.getJSONObject("meta");

                        logger.debug("Number of objects "+addresses.size()+" out of "+meta.getString("total_count"));

                        if (meta.has("next") && !(meta.isNull("next")) && !meta.getString("next").equals("")) {
                            logger.debug("Found new page "+meta.getString("next"));
                            target = meta.getString("next");
                            logger.debug("target "+target);
                            target = target.substring(target.indexOf("?"));
                            logger.debug("new target "+target);
                            moreData = true;
                        }
                        else  {
                            moreData = false;
                        }
                    }
                }
                catch (JSONException e) {
                    throw new InternalException(e);
                }
            }
            return addresses;
        }
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listIpPoolStatus(@Nonnull IPVersion version) throws InternalException, CloudException {
        if (version.equals(IPVersion.IPV4)) {

            ArrayList<ResourceStatus> addresses = new ArrayList<ResourceStatus>();
            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            boolean moreData = true;
            String baseTarget = "/ips/detail/";
            String target = "";

            while(moreData)  {
                //dmayne 20130218: JSON Parsing
                logger.debug("Target "+target);
                target = baseTarget+target;
                logger.debug("final target "+target);

                JSONObject pool = method.list(target);

                if (pool == null) {
                    throw new CloudException("Unable to communicate with CloudSigma endpoint");
                }
                //dmayne 20130218: use JSON parsing
                try {
                    JSONArray objects = pool.getJSONArray("objects");
                    for (int i = 0; i < objects.length(); i++) {
                        JSONObject jObj = objects.getJSONObject(i);

                        ResourceStatus address = toStatus(jObj);

                        if (address != null) {
                            addresses.add(address);
                        }
                    }

                    //dmayne 20130314: check if there are more pages
                    if (pool.has("meta")) {
                        logger.debug("Found meta tag");
                        JSONObject meta = pool.getJSONObject("meta");

                        logger.debug("Number of objects "+addresses.size()+" out of "+meta.getString("total_count"));

                        if (meta.has("next") && !(meta.isNull("next")) && !meta.getString("next").equals("")) {
                            logger.debug("Found new page "+meta.getString("next"));
                            target = meta.getString("next");
                            logger.debug("target "+target);
                            target = target.substring(target.indexOf("?"));
                            logger.debug("new target "+target);
                            moreData = true;
                        }
                        else  {
                            moreData = false;
                        }
                    }
                }
                catch (JSONException e) {
                    throw  new InternalException(e);
                }
            }
            return addresses;
        }
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IpForwardingRule> listRules(@Nonnull String addressId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        //todo dmayne 20130221: should ipv6 be added here?
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public void releaseFromPool(@Nonnull String addressId) throws InternalException, CloudException {
        //dmayne 20130222: api 2.0 does not support deleting ips
        throw new OperationNotSupportedException("IP deletion handled through subscriptions");
    }

    @Override
    public void releaseFromServer(@Nonnull String addressId) throws InternalException, CloudException {
        IpAddress address = getIpAddress(addressId);

        if (address == null) {
            throw new CloudException("No such IP address: " + address);
        }
        provider.getComputeServices().getVirtualMachineSupport().releaseIP(address);
    }

    @Override
    public @Nonnull String request(@Nonnull AddressType typeOfAddress) throws InternalException, CloudException {
        //dmayne: 20130221 api 2.0 does not support requesting ips
        throw new OperationNotSupportedException("IP request handled through subscriptions");
    }

    @Override
    public @Nonnull String request(@Nonnull IPVersion version) throws InternalException, CloudException {
        //dmayne 20130221: creating ips not supported in api 2.0
        throw new OperationNotSupportedException("IP request handled through subscriptions");
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion version) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Requesting for VLANs is not supported");
    }

    @Override
    public @Nonnull String requestForVLAN(@Nonnull IPVersion version, @Nonnull String vlanId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Requesting for VLANs is not supported");
    }

    @Override
    public void stopForward(@Nonnull String ruleId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("IP forwarding is not supported");
    }

    @Override
    public boolean supportsVLANAddresses(@Nonnull IPVersion ofVersion) throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable IpAddress toIP(@Nullable JSONObject object, boolean unassignedOnly) throws CloudException, InternalException {
        if (object == null) {
            return null;
        }
        ProviderContext ctx = provider.getContext();

        if (ctx == null) {
            throw new NoContextException();
        }
        String regionId = ctx.getRegionId();

        if (regionId == null) {
            throw new CloudSigmaConfigurationException("No region was specified for this request");
        }

        IpAddress address = new IpAddress();

        address.setForVlan(false);
        address.setProviderLoadBalancerId(null);
        address.setProviderNetworkInterfaceId(null);
        address.setRegionId(regionId);
        //todo dmayne 20130221: how do we determine v4 or v6 address?
        address.setVersion(IPVersion.IPV4);
        address.setAddressType(AddressType.PUBLIC);

        try {
            String id = object.getString("uuid");

            if (id != null && !id.equals("")) {
                address.setIpAddressId(id);
                address.setAddress(id);
            }

            String host = null;
            JSONObject server;

            if (object.has("server") && !object.isNull("server")) {
                server = object.getJSONObject("server");
                if (server != null) {
                    host = server.getString("uuid");
                }
            }

            if (host != null && !host.equals("")) {
                address.setServerId(host);
            }

            if (object.has("owner")) {
                JSONObject owner = object.getJSONObject("owner");
                String user = null;
                if (owner!= null && owner.has("uuid")) {
                    user = owner.getString("uuid");
                }

                if (user != null && !user.equals("") && !user.equals(ctx.getAccountNumber())) {
                    return null;
                }
            }
            if (address.getServerId() != null && unassignedOnly) {
                return null;
            }
        }
        catch (JSONException e) {
            throw new  InternalException(e);
        }
        return address;
    }

    private @Nullable ResourceStatus toStatus(@Nullable JSONObject object) throws CloudException, InternalException {
        if (object == null) {
            return null;
        }
        ProviderContext ctx = provider.getContext();

        if (ctx == null) {
            throw new NoContextException();
        }
        String regionId = ctx.getRegionId();

        if (regionId == null) {
            throw new CloudSigmaConfigurationException("No region was specified for this request");
        }
        try {
        String id = object.getString("uuid");

        if (id != null && !id.equals("")) {
            String host = null;
            JSONObject server;

            if (object.has("server") && !object.isNull("server") ) {
                server = object.getJSONObject("server");
                if (server != null) {
                    host = server.getString("uuid");
                }
            }
            boolean available = (host != null && !host.equals(""));

            return new ResourceStatus(id, available);
        }
        } catch (JSONException e){
            throw new InternalException(e + " -> " + object);
        }
        return null;
    }

    private @Nonnull String toAddressURL(@Nonnull String addressId, @Nonnull String action) throws InternalException {
        try {
            return ("/ips/" + URLEncoder.encode(addressId, "utf-8") + "/" + action);
        } catch (UnsupportedEncodingException e) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
    }
}
