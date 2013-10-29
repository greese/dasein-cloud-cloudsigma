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

package org.dasein.cloud.cloudsigma.network.vlan;

import org.dasein.cloud.network.AbstractVLANSupport;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.*;

/**
 * Support for VLANs in CloudSigma.
 * <p>Created by Danielle Mayne: 02/20/13 14:30 PM</p>
 * @author George Reese
 * @author Danielle Mayne
 * @version 2013.02 initial version
 * @since 2013.02
 */
public class ServerVLANSupport extends AbstractVLANSupport {
    static private final Logger logger = CloudSigma.getLogger(ServerVLANSupport.class);

    private CloudSigma provider;

    public ServerVLANSupport(@Nonnull CloudSigma provider) {
        super(provider);
        this.provider = provider;
    }

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
        //dmayne 20130221: New VLANs are created by buying a subscription.
        return false;
    }

    @Override
    public boolean allowsNewSubnetCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsMultipleTrafficTypesOverSubnet() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean allowsMultipleTrafficTypesOverVlan() throws CloudException, InternalException {
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
        if (vlanId.length() > 0){
            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            try {
                String obj = method.getString(toNetworkURL(vlanId, ""));
                if (obj != null ) {
                    return toVLAN(new JSONObject(obj));
                }
                return null;
            }
            catch (JSONException e) {
                throw new InternalException(e);
            }
        }
        else {
            throw new InternalException("Vlan id is null/empty!");
        }
    }

    @Override
    public boolean isConnectedViaInternetGateway(@Nonnull String vlanId) throws CloudException, InternalException {
        return true;
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

        if (ipSupport != null) {
            for (IPVersion version : ipSupport.listSupportedIPVersions()) {
                for (IpAddress addr : ipSupport.listIpPool(version, false)) {
                    if (inVlanId.equals(addr.getProviderVlanId())) {
                        resources.add(addr);
                    }
                }
            }
        }
        for (RoutingTable table : listRoutingTables(inVlanId)) {
            resources.add(table);
        }
        ComputeServices compute = provider.getComputeServices();
        VirtualMachineSupport vmSupport = provider.getComputeServices().getVirtualMachineSupport();
        Iterable<VirtualMachine> vms = vmSupport.listVirtualMachines();

        for (VirtualMachine vm : vms) {
            if (inVlanId.equals(vm.getProviderVlanId())) {
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

        boolean moreData = true;
        String baseTarget = "/vlans/";
        String target = "?limit=0&fields=uuid";

      //  while(moreData)  {                         - commented out as it seems paging is no longer supported
        //  but who knows when the api will change back again
            //dmayne 20130218: JSON Parsing
            target = baseTarget+target;

            try {
                JSONObject json = method.list(target);

                if (json == null) {
                    throw new CloudException("No VLAN endpoint was found");
                }
                JSONArray objects = json.getJSONArray("objects");
                for (int i = 0; i < objects.length(); i++) {
                    JSONObject jObj = objects.getJSONObject(i);

                    ResourceStatus status = toStatus(jObj);

                    if (status != null) {
                        networks.add(status);
                    }
                }

               /* //dmayne 20130314: check if there are more pages    - commented out as it seems paging is no longer supported
              //  but who knows when the api will change back again
                if (json.has("meta")) {
                    JSONObject meta = json.getJSONObject("meta");

                    if (meta.has("next") && !(meta.isNull("next")) && !meta.getString("next").equals("")) {
                        target = meta.getString("next");
                        target = target.substring(target.indexOf("?"));
                        moreData = true;
                    }
                    else  {
                        moreData = false;
                    }
                } */
            }
            catch (JSONException e) {
                throw new InternalException(e);
            }
      //  }
        return networks;
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        ArrayList<VLAN> networks = new ArrayList<VLAN>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        boolean moreData = true;
        String baseTarget = "/vlans/detail/?limit=0";
        String target = "";

     //   while(moreData)  {        - commented out as it seems paging is no longer supported
        //  but who knows when the api will change back again
            //dmayne 20130218: JSON Parsing
            target = baseTarget+target;

            try {
                JSONObject json = method.list(target);

                if (json == null) {
                    throw new CloudException("No VLAN endpoint was found");
                }
                JSONArray objects = json.getJSONArray("objects");
                for (int i = 0; i < objects.length(); i++) {
                    JSONObject jObj = objects.getJSONObject(i);

                    VLAN vlan = toVLAN(jObj);

                    if (vlan != null) {
                        networks.add(vlan);
                    }
                }

               /* //dmayne 20130314: check if there are more pages    - commented out as it seems paging is no longer supported
              //  but who knows when the api will change back again
                if (json.has("meta")) {
                    JSONObject meta = json.getJSONObject("meta");

                    if (meta.has("next") && !(meta.isNull("next")) && !meta.getString("next").equals("")) {
                        target = meta.getString("next");
                        target = target.substring(target.indexOf("?"));
                        moreData = true;
                    }
                    else  {
                        moreData = false;
                    }
                } */
            }
            catch (JSONException e) {
                throw new InternalException(e);
            }
       // }
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
        //dmayne 20130222: api 2.0 does not support deleting vlan
        throw new OperationNotSupportedException("Vlan deletion handled through subscriptions");
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
            if (id == null || id.equals("")) {
                return null;
            }
            return new ResourceStatus(id, VLANState.AVAILABLE);
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
    }

    private @Nullable VLAN toVLAN(@Nullable JSONObject object) throws CloudException, InternalException {
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

        VLAN vlan = new VLAN();

        vlan.setDnsServers(new String[0]);
        vlan.setNtpServers(new String[0]);
        vlan.setProviderDataCenterId(regionId + "-a");
        vlan.setProviderRegionId(regionId);
        vlan.setProviderOwnerId(ctx.getAccountNumber());
        vlan.setSupportedTraffic(new IPVersion[]{IPVersion.IPV4});
        vlan.setCidr("0.0.0.0/0");
        vlan.setCurrentState(VLANState.AVAILABLE);

        try {String id = object.getString("uuid");

        if (id != null && !id.equals("")) {
            vlan.setProviderVlanId(id);
        }

        if (object.has("owner")) {
            JSONObject owner = object.getJSONObject("owner");
            String user = null;
            if (owner != null && owner.has("uuid")) {
                user = owner.getString("uuid");
            }

            if (user != null && !user.equals("") && !user.equals(ctx.getAccountNumber())) {
                return null;
            }
        }

        if (object.has("meta")) {
            JSONObject meta = object.getJSONObject("meta");
            if (meta != null && meta.has("name")) {
                String name = meta.getString("name");
                vlan.setName(name);
            }
            if (meta != null && meta.has("description")) {
                String description = meta.getString("description");
                vlan.setDescription(description);
            }
        }

        if (vlan.getProviderVlanId() == null) {
            return null;
        }
        if (vlan.getName() == null) {
            vlan.setName(vlan.getProviderVlanId());
        }
        if (vlan.getDescription() == null) {
            vlan.setDescription(vlan.getName());
        }
        return vlan;
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
    }

    private @Nonnull String toNetworkURL(@Nonnull String vlanId, @Nonnull String action) throws InternalException {
        try {
            return ("/vlans/" + URLEncoder.encode(vlanId, "utf-8") + "/" + action);
        } catch (UnsupportedEncodingException e) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
    }
}
