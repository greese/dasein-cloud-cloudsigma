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
package org.dasein.cloud.cloudsigma.network.vlan;

import org.apache.log4j.Logger;
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
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.IpAddressSupport;
import org.dasein.cloud.network.NICCreateOptions;
import org.dasein.cloud.network.NetworkInterface;
import org.dasein.cloud.network.NetworkServices;
import org.dasein.cloud.network.Networkable;
import org.dasein.cloud.network.RoutingTable;
import org.dasein.cloud.network.Subnet;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.network.VLANState;
import org.dasein.cloud.network.VLANSupport;

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
 * Support for VLANs in CloudSigma.
 * <p>Created by George Reese: 10/26/12 12:30 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class ServerVLANSupport implements VLANSupport {
    static private final Logger logger = CloudSigma.getLogger(ServerVLANSupport.class);

    private CloudSigma provider;

    public ServerVLANSupport(@Nonnull CloudSigma provider) { this.provider = provider; }

    @Override
    public void addRouteToAddress(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String address) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public void addRouteToGateway(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String gatewayId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public void addRouteToNetworkInterface(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String nicId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public void addRouteToVirtualMachine(@Nonnull String toRoutingTableId, @Nonnull IPVersion version, @Nullable String destinationCidr, @Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public boolean allowsNewNetworkInterfaceCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsNewVlanCreation() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean allowsNewSubnetCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public void assignRoutingTableToSubnet(@Nonnull String subnetId, @Nonnull String routingTableId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public void assignRoutingTableToVlan(@Nonnull String vlanId, @Nonnull String routingTableId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public void attachNetworkInterface(@Nonnull String nicId, @Nonnull String vmId, int index) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs are not supported");
    }

    @Override
    public String createInternetGateway(@Nonnull String forVlanId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Internet gateways are not supported");
    }

    @Override
    public @Nonnull String createRoutingTable(@Nonnull String forVlanId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public @Nonnull NetworkInterface createNetworkInterface(@Nonnull NICCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs are not supported");
    }

    @Override
    public @Nonnull Subnet createSubnet(@Nonnull String cidr, @Nonnull String inProviderVlanId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Subnets are not supported");
    }

    @Override
    public @Nonnull VLAN createVlan(@Nonnull String cidr, @Nonnull String name, @Nonnull String description, @Nonnull String domainName, @Nonnull String[] dnsServers, @Nonnull String[] ntpServers) throws CloudException, InternalException {
        StringBuilder body = new StringBuilder();

        body.append("name ").append(name.replaceAll("\n", " ")).append("\n");

        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        VLAN vlan = toVLAN(method.postObject("/resources/vlan/create", body.toString()));

        if( vlan == null ) {
            throw new CloudException("VLAN creation succeeded without an error, but no matching VLAN was returned");
        }
        return vlan;
    }

    @Override
    public void detachNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs are not supported");
    }

    @Override
    public int getMaxNetworkInterfaceCount() throws CloudException, InternalException {
        return 0;
    }

    @Override
    public int getMaxVlanCount() throws CloudException, InternalException {
        return -1;
    }

    @Override
    public @Nonnull String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "NIC";
    }

    @Override
    public @Nonnull String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "subnet";
    }

    @Override
    public @Nonnull String getProviderTermForVlan(@Nonnull Locale locale) {
        return "VLAN";
    }

    @Override
    public @Nullable NetworkInterface getNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nullable RoutingTable getRoutingTableForSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nonnull Requirement getRoutingTableSupport() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public RoutingTable getRoutingTableForVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public Subnet getSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nonnull Requirement getSubnetSupport() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public VLAN getVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        return toVLAN(method.getObject(toNetworkURL(vlanId, "info")));
    }

    @Override
    public boolean isNetworkInterfaceSupportEnabled() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
    }

    @Override
    public boolean isSubnetDataCenterConstrained() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isVlanDataCenterConstrained() throws CloudException, InternalException {
        return false;
    }

    @Override
    public Collection<String> listFirewallIdsForNIC(@Nonnull String nicId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Iterable<ResourceStatus> listNetworkInterfaceStatus() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfaces() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesForVM(@Nonnull String forVmId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesInSubnet(@Nonnull String subnetId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<NetworkInterface> listNetworkInterfacesInVLAN(@Nonnull String vlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<Networkable> listResources(@Nonnull String inVlanId) throws CloudException, InternalException {
        ArrayList<Networkable> resources = new ArrayList<Networkable>();
        NetworkServices network = provider.getNetworkServices();

        IpAddressSupport ipSupport = network.getIpAddressSupport();

        if( ipSupport != null ) {
            for( IPVersion version : ipSupport.listSupportedIPVersions() ) {
                for( IpAddress addr : ipSupport.listIpPool(version, false) ) {
                    if( inVlanId.equals(addr.getProviderVlanId()) ) {
                        resources.add(addr);
                    }
                }
            }
        }
        for( RoutingTable table : listRoutingTables(inVlanId) ) {
            resources.add(table);
        }
        ComputeServices compute = provider.getComputeServices();
        VirtualMachineSupport vmSupport = provider.getComputeServices().getVirtualMachineSupport();
        Iterable<VirtualMachine> vms = vmSupport.listVirtualMachines();

        for( VirtualMachine vm : vms ) {
            if( inVlanId.equals(vm.getProviderVlanId()) ) {
                resources.add(vm);
            }
        }
        return resources;
    }

    @Override
    public @Nonnull Iterable<RoutingTable> listRoutingTables(@Nonnull String inVlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<Subnet> listSubnets(@Nonnull String inVlanId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVlanStatus() throws CloudException, InternalException {
        ArrayList<ResourceStatus> networks = new ArrayList<ResourceStatus>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        Collection<Map<String,String>> list = method.list("/resources/vlan/info");

        if( list == null ) {
            throw new CloudException("No VLAN endpoint was found");
        }
        for( Map<String,String> object : list ) {
            ResourceStatus status = toStatus(object);

            if( status != null ) {
                networks.add(status);
            }
        }
        return networks;
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        ArrayList<VLAN> networks = new ArrayList<VLAN>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        Collection<Map<String,String>> list = method.list("/resources/vlan/info");

        if( list == null ) {
            throw new CloudException("No VLAN endpoint was found");
        }
        for( Map<String,String> object : list ) {
            VLAN vlan = toVLAN(object);

            if( vlan != null ) {
                networks.add(vlan);
            }
        }
        return networks;
    }

    @Override
    public void removeInternetGateway(@Nonnull String forVlanId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Internet gateways are not supported");
    }

    @Override
    public void removeNetworkInterface(@Nonnull String nicId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("NICs are not supported");
    }

    @Override
    public void removeRoute(@Nonnull String inRoutingTableId, @Nonnull String destinationCidr) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public void removeRoutingTable(@Nonnull String routingTableId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Routing tables are not supported");
    }

    @Override
    public void removeSubnet(String providerSubnetId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Subnets are not supported");
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        if( method.postString(toNetworkURL(vlanId, "destroy"), "") == null ) {
            throw new CloudException("No VLAN endpoint for destroy operation");
        }
    }

    @Override
    public boolean supportsInternetGatewayCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsRawAddressRouting() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable ResourceStatus toStatus(@Nullable Map<String,String> object) throws CloudException, InternalException {
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
        String id = object.get("resource");

        if( id == null || id.equals("") ) {
            return null;
        }
        return new ResourceStatus(id, VLANState.AVAILABLE);
    }

    private @Nullable VLAN toVLAN(@Nullable Map<String,String> object) throws CloudException, InternalException {
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

        VLAN vlan = new VLAN();

        vlan.setDnsServers(new String[0]);
        vlan.setNtpServers(new String[0]);
        vlan.setProviderDataCenterId(regionId + "-a");
        vlan.setProviderRegionId(regionId);
        vlan.setProviderOwnerId(ctx.getAccountNumber());
        vlan.setSupportedTraffic(new IPVersion[]{IPVersion.IPV4});
        vlan.setCidr("0.0.0.0/0");
        vlan.setCurrentState(VLANState.AVAILABLE);

        String id = object.get("resource");

        if( id != null && !id.equals("") ) {
            vlan.setProviderVlanId(id);
        }
        String name = object.get("name");

        if( name != null && !name.equals("") ) {
            vlan.setName(name);
        }
        String user = object.get("user");

        if( user != null && !user.equals("") && !user.equals(ctx.getAccountNumber())) {
            return null;
        }
        String type = object.get("type");

        if( type != null && !type.equals("") && !type.equals("vlan") ) {
            return null;
        }

        if( vlan.getProviderVlanId() == null ) {
            return null;
        }
        if( vlan.getName() == null ) {
            vlan.setName(vlan.getProviderVlanId());
        }
        if( vlan.getDescription() == null ) {
            vlan.setDescription(vlan.getName());
        }
        return vlan;
    }

    private @Nonnull String toNetworkURL(@Nonnull String vlanId, @Nonnull String action) throws InternalException {
        try {
            return ("/resources/vlan/" + URLEncoder.encode(vlanId, "utf-8") + "/" + action);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
    }
}
