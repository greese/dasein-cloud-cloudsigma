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
package org.dasein.cloud.cloudsigma.network.ip;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
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
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;

/**
 * Support for static IP addresses in CloudSigma.
 * <p>Created by George Reese: 10/26/12 12:30 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class StaticIPSupport implements IpAddressSupport {
    static private final Logger logger = CloudSigma.getLogger(StaticIPSupport.class);

    private CloudSigma provider;

    public StaticIPSupport(@Nonnull CloudSigma provider) { this.provider = provider; }

    @Override
    public void assign(@Nonnull String addressId, @Nonnull String serverId) throws InternalException, CloudException {
        IpAddress address = getIpAddress(addressId);

        if( address == null ) {
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

        return toIP(method.getObject(toAddressURL(addressId, "info")), false);
    }

    @Override
    public @Nonnull String getProviderTermForIpAddress(@Nonnull Locale locale) {
        return "static IP";
    }

    @Override
    public boolean isAssigned(@Nonnull AddressType type) {
        return type.equals(AddressType.PUBLIC);
    }

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
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
        return type.equals(AddressType.PUBLIC);
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
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
        return listIpPool(IPVersion.IPV4, unassignedOnly);
    }

    @Override
    public @Nonnull Iterable<IpAddress> listIpPool(@Nonnull IPVersion version, boolean unassignedOnly) throws InternalException, CloudException {
        if( version.equals(IPVersion.IPV4) ) {
            CloudSigmaMethod method = new CloudSigmaMethod(provider);
            Collection<Map<String,String>> pool = method.list("/resources/ip/info");
            ArrayList<IpAddress> addresses = new ArrayList<IpAddress>();

            if( pool == null ) {
                throw new CloudException("Unable to communicate with CloudSigma endpoint");
            }
            for( Map<String,String> object : pool ) {
                IpAddress address = toIP(object, unassignedOnly);

                if( address != null ) {
                    addresses.add(address);
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
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public void releaseFromPool(@Nonnull String addressId) throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        method.getObject(toAddressURL(addressId, "destroy"));
    }

    @Override
    public void releaseFromServer(@Nonnull String addressId) throws InternalException, CloudException {
        IpAddress address = getIpAddress(addressId);

        if( address == null ) {
            throw new CloudException("No such IP address: " + address);
        }
        provider.getComputeServices().getVirtualMachineSupport().releaseIP(address);
    }

    @Override
    public @Nonnull String request(@Nonnull AddressType typeOfAddress) throws InternalException, CloudException {
        if( typeOfAddress.equals(AddressType.PRIVATE) ) {
            throw new CloudException("Static private IPs are not supported");
        }
        return request(IPVersion.IPV4);
    }

    @Override
    public @Nonnull String request(@Nonnull IPVersion version) throws InternalException, CloudException {
        if( !version.equals(IPVersion.IPV4) ) {
            throw new OperationNotSupportedException("Unsupported IP version: " + version);
        }
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        IpAddress address = toIP(method.postObject("/resources/ip/create", ""), false);

        if( address == null ) {
            throw new CloudException("No error was returned, but no IP address was allocated");
        }
        return address.getProviderIpAddressId();
    }

    @Override
    public @Nonnull String requestForVLAN(IPVersion version) throws InternalException, CloudException {
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

    private @Nullable IpAddress toIP(@Nullable Map<String,String> object, boolean unassignedOnly) throws CloudException, InternalException {
        System.out.println("IP: " + object);
        if( object == null ) {
            return null;
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudSigmaConfigurationException("No region was specified for this request");
        }

        IpAddress address = new IpAddress();

        address.setForVlan(false);
        address.setProviderLoadBalancerId(null);
        address.setProviderNetworkInterfaceId(null);
        address.setRegionId(regionId);
        address.setVersion(IPVersion.IPV4);
        address.setAddressType(AddressType.PUBLIC);

        String id = object.get("resource");

        if( id != null && !id.equals("") ) {
            address.setIpAddressId(id);
            address.setAddress(id);
        }

        String user = object.get("user");

        if( user != null && !user.equals("") && !user.equals(ctx.getAccountNumber())) {
            return null;
        }
        String type = object.get("type");

        if( type != null && !type.equals("") && !type.equals("ip") ) {
            return null;
        }
        address.setServerId(null); // TODO: find server
        if( address.getServerId() != null && unassignedOnly ) {
            return null;
        }
        return address;
    }
    private @Nonnull String toAddressURL(@Nonnull String addressId, @Nonnull String action) throws InternalException {
        try {
            return ("/resources/ip/" + URLEncoder.encode(addressId, "utf-8") + "/" + action);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
    }
}
