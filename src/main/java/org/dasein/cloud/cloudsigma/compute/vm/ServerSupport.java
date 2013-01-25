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
package org.dasein.cloud.cloudsigma.compute.vm;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.CloudSigmaConfigurationException;
import org.dasein.cloud.cloudsigma.CloudSigmaMethod;
import org.dasein.cloud.cloudsigma.NoContextException;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VMScalingCapabilities;
import org.dasein.cloud.compute.VMScalingOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VmStatistics;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.IpAddress;
import org.dasein.cloud.network.RawAddress;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Provides access to virtual machines in CloudSigma.
 * <p>Created by George Reese: 10/25/12 11:04 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class ServerSupport implements VirtualMachineSupport {
    static private final Logger logger = CloudSigma.getLogger(ServerSupport.class);

    private CloudSigma provider;

    public ServerSupport(@Nonnull CloudSigma provider) { this.provider = provider; }


    public void assignIP(@Nonnull String serverId, @Nonnull IpAddress address) throws CloudException, InternalException {
        VirtualMachine vm = getVirtualMachine(serverId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + serverId);
        }
        StringBuilder body = new StringBuilder();

        body.append("nic:0:dhcp ").append(address.getProviderIpAddressId()).append("\n");
        change(vm, body.toString());
    }

    public void attach(@Nonnull Volume volume, @Nonnull String serverId, @Nonnull String deviceId) throws CloudException, InternalException {
        if( volume.getProviderVirtualMachineId() != null ) {
            throw new CloudException("Volume is already attached to " + volume.getProviderVirtualMachineId());
        }
        VirtualMachine vm = getVirtualMachine(serverId);

        if( vm == null ) {
            throw new CloudException("Virtual machine " + serverId + " does not exist");
        }

        StringBuilder body = new StringBuilder();

        body.append("block:").append(deviceId).append(" ").append(volume.getProviderVolumeId()).append("\n");
        change(vm, body.toString());
    }

    private void change(@Nonnull VirtualMachine vm, @Nonnull String body) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + ServerSupport.class.getName() + ".change(" + vm + "," + body + ")");
        }
        try {
            boolean restart = !VmState.STOPPED.equals(vm.getCurrentState());
            VirtualMachine workingVm = vm;

            if( restart ) {
                if( logger.isInfoEnabled() ) {
                    logger.info("Virtual machine " + vm.getProviderVirtualMachineId() + " needs to be stopped prior to change");
                }
                stop(vm.getProviderVirtualMachineId());
                if( logger.isInfoEnabled() ) {
                    logger.info("Waiting for " + vm.getProviderVirtualMachineId() + " to fully stop");
                }
                workingVm = waitForState(workingVm, CalendarWrapper.MINUTE * 10L, VmState.STOPPED);
                if( workingVm == null ) {
                    logger.info("Virtual machine " + vm.getProviderVirtualMachineId() + " disappared while waiting for stop");
                    throw new CloudException("Virtual machine " + vm.getProviderVirtualMachineId() + " disappeared before attachment could happen");
                }
                if( logger.isInfoEnabled() ) {
                    logger.info("Done waiting for " + vm.getProviderVirtualMachineId() + ": " + workingVm.getCurrentState());
                }
            }
            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            if( logger.isInfoEnabled() ) {
                logger.info("POSTing changes to " + vm.getProviderVirtualMachineId());
            }
            if( method.postObject(toServerURL(vm.getProviderVirtualMachineId(), "set"), body) == null ) {
                throw new CloudException("Unable to locate servers endpoint in CloudSigma");
            }
            if( logger.isInfoEnabled() ) {
                logger.info("Change to " + vm.getProviderVirtualMachineId() + " succeeded");
            }
            if( restart ) {
                if( logger.isInfoEnabled() ) {
                    logger.info("Restarting " + vm.getProviderVirtualMachineId());
                }
                final String id = vm.getProviderVirtualMachineId();

                Thread t = new Thread() {
                    public void run() {
                        try {
                            try {
                                ServerSupport.this.start(id);
                            }
                            catch( Exception e ) {
                                logger.warn("Failed to start VM post-change: " + e.getMessage());
                            }
                        }
                        finally {
                            provider.release();
                        }
                    }
                };

                provider.hold();
                t.setName("Restart CloudSigma VM " + id);
                t.setDaemon(true);
                t.start();
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + ServerSupport.class.getName() + ".change()");
            }
        }
    }

    @Override
    public VirtualMachine alterVirtualMachine(@Nonnull String vmId, @Nonnull VMScalingOptions options) throws InternalException, CloudException {
        throw new OperationNotSupportedException("VM alteration not yet supported");
    }

    @Override
    public @Nonnull VirtualMachine clone(final @Nonnull String vmId, @Nonnull String intoDcId, @Nonnull String name, @Nonnull String description, boolean powerOn, @Nullable String... firewallIds) throws InternalException, CloudException {
        VirtualMachine vm = getVirtualMachine(vmId);

        if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
            throw new CloudException("No such virtual machine to clone: " + vmId);
        }
        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

        if( !VmState.STOPPED.equals(vm.getCurrentState()) ) {
            stop(vmId);
            while( timeout > System.currentTimeMillis() ) {
                if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                    throw new CloudException("Virtual machine terminated during stop for cloning");
                }
                if( VmState.STOPPED.equals(vm.getCurrentState()) ) {
                    break;
                }
                try { Thread.sleep(30000L); }
                catch( InterruptedException ignore ) { }
                try { vm = getVirtualMachine(vmId); }
                catch( Exception ignore ) { }
            }
        }
        try {
            StringBuilder body = new StringBuilder();

            body.append("name ");
            body.append(name.replaceAll("\n", " "));
            body.append("\n");

            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            vm = toVirtualMachine(method.postObject(toServerURL(vmId, "clone"), body.toString()));
            if( vm == null ) {
                throw new CloudException("No virtual machine was provided in the response");
            }
            if( powerOn ) {
                vm = waitForState(vm, CalendarWrapper.MINUTE*15L, VmState.STOPPED, VmState.RUNNING);
                if( vm == null ) {
                    throw new CloudException("New VM disappeared");
                }
                if( !VmState.RUNNING.equals(vm.getCurrentState()) ) {
                    final String id = vm.getProviderVirtualMachineId();

                    Thread t = new Thread() {
                        public void run() {
                            try {
                                try {
                                    ServerSupport.this.start(id);
                                }
                                catch( Exception e ) {
                                    logger.warn("Failed to start VM post-create: " + e.getMessage());
                                }
                            }
                            finally {
                                provider.release();
                            }
                        }
                    };

                    provider.hold();
                    t.setName("Start CloudSigma VM " + id);
                    t.setDaemon(true);
                    t.start();
                }
            }
            return vm;
        }
        finally {
            provider.hold();
            Thread t = new Thread() {
                public void run() {
                    try {
                        try { ServerSupport.this.start(vmId); }
                        catch( Throwable ignore ) { }
                    }
                    finally {
                        provider.release();
                    }
                }
            };

            t.setName("CloudSigma Clone Restarted " + vmId);
            t.setDaemon(true);
            t.start();
        }
    }

    @Override
    public @Nullable VMScalingCapabilities describeVerticalScalingCapabilities() throws CloudException, InternalException {
        return null;
    }


    public void detach(@Nonnull Volume volume) throws CloudException, InternalException {
        String serverId = volume.getProviderVirtualMachineId();

        if( serverId == null ) {
            throw new CloudException("No server is attached to " + volume.getProviderVolumeId());
        }

        VirtualMachine vm = getVirtualMachine(serverId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + serverId);
        }

        String deviceId = getDeviceId(vm, volume.getProviderVolumeId());

        if( deviceId == null ) {
            throw new CloudException("Volume does not appear to be attached null vs " + volume.getDeviceId());
        }
        StringBuilder body = new StringBuilder();

        body.append("block:").append(deviceId).append(" ").append("\n");
        change(vm, body.toString());
    }

    @Override
    public void disableAnalytics(String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public void enableAnalytics(String vmId) throws InternalException, CloudException {
        // NO-OP
    }

    @Override
    public @Nonnull String getConsoleOutput(@Nonnull String vmId) throws InternalException, CloudException {
        return "";
    }

    @Override
    public int getCostFactor(@Nonnull VmState state) throws InternalException, CloudException {
        return 100;
    }

    public @Nullable String getDeviceId(@Nonnull VirtualMachine vm, @Nonnull String volumeId) throws CloudException, InternalException {
        for( int i=0; i<8; i++ ) {
            String id = (String)vm.getTag("block:" + i);

            if( id != null && id.equals(volumeId) ) {
                return String.valueOf(i);
            }
        }
        return null;
    }

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        return -2;
    }

    @Override
    public VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        String[] parts = productId.split(":");
        int cpuCount, ramInMb, cpuSpeed;

        if( parts.length < 2 ) {
            return null;
        }
        try {
            if( parts.length == 2 ) {
                cpuCount = 1;
            }
            else {
                cpuCount = Integer.parseInt(parts[2]);
            }
            ramInMb = Integer.parseInt(parts[0]);
            cpuSpeed = Integer.parseInt(parts[1]);
        }
        catch( NumberFormatException e ) {
            return null;
        }
        VirtualMachineProduct product = new VirtualMachineProduct();

        product.setProviderProductId(productId);
        product.setName(ramInMb + "MB - " + cpuCount + "x" + cpuSpeed + "MHz");
        product.setRamSize(new Storage<Megabyte>(ramInMb, Storage.MEGABYTE));
        product.setCpuCount(cpuCount);
        product.setDescription(product.getName());
        product.setRootVolumeSize(new Storage<Gigabyte>(0, Storage.GIGABYTE));
        return product;
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "server";
    }

    @Override
    public VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        return toVirtualMachine(method.getObject(toServerURL(vmId, "info")));
    }

    @Override
    public VmStatistics getVMStatistics(String vmId, long from, long to) throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Iterable<VmStatistics> getVMStatisticsForPeriod(@Nonnull String vmId, @Nonnegative long from, @Nonnegative long to) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return (cls.equals(ImageClass.MACHINE) ? Requirement.REQUIRED : Requirement.NONE);
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyPasswordRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public @Nonnull Requirement identifyRootVolumeRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyShellKeyRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isBasicAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isExtendedAnalyticsSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        getVirtualMachine("---no such id---");
        return true;
    }

    @Override
    public boolean isUserDataSupported() throws CloudException, InternalException {
        return false;
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + ServerSupport.class.getName() + ".launch(" + withLaunchOptions + ")");
        }
        try {
            MachineImage img = provider.getComputeServices().getImageSupport().getImage(withLaunchOptions.getMachineImageId());

            if( img == null ) {
                throw new CloudException("No such machine image: " + withLaunchOptions.getMachineImageId());
            }
            if( logger.isInfoEnabled() ) {
                logger.info("Cloning drive from machine image " + img.getProviderMachineImageId() + "...");
            }
            Map<String,String> drive = provider.getComputeServices().getImageSupport().cloneDrive(withLaunchOptions.getMachineImageId(), withLaunchOptions.getHostName(), null);

            if( logger.isDebugEnabled() ) {
                logger.debug("drive=" + drive);
            }
            String driveId = drive.get("drive");

            if( driveId == null ) {
                throw new CloudException("No drive was cloned to support the machine launch process");
            }
            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE*20L);
            String status = drive.get("status");

            if( logger.isInfoEnabled() ) {
                logger.info("Waiting for new drive " + driveId + " to become active...");
            }
            while( timeout > System.currentTimeMillis() ) {
                if( logger.isDebugEnabled() ) {
                    logger.debug("status.drive." + driveId + "=" + status);
                }
                if( status != null && status.equals("active") ) {
                    if( logger.isInfoEnabled() ) {
                        logger.info("Drive is now ready for launching");
                    }
                    break;
                }
                try { Thread.sleep(20000L); }
                catch( InterruptedException ignore ) { }
                try { drive = provider.getComputeServices().getImageSupport().getDrive(driveId); }
                catch( Throwable ignore ) { }
                if( drive == null ) {
                    throw new CloudException("Cloned drive has disappeared");
                }
                status = drive.get("status");
            }
            String productId = withLaunchOptions.getStandardProductId();
            StringBuilder body = new StringBuilder();

            body.append("name ");
            body.append(withLaunchOptions.getHostName().replaceAll("\n", " "));
            body.append("\nide:0:0 ");
            body.append(driveId);
            body.append("\nboot ide:0:0");
            body.append("\ncpu ");

            int cpuCount = 1, cpuSpeed = 1000, ramInMb = 512;
            String[] parts = productId.replaceAll("\n", " ").split(":");


            if( parts.length > 1 ) {
                cpuCount = 1;
                try {
                    ramInMb = Integer.parseInt(parts[0]);
                    cpuSpeed = Integer.parseInt(parts[1]);
                    if( parts.length == 3 ) {
                        cpuCount = Integer.parseInt(parts[2]);
                    }
                }
                catch( NumberFormatException ignore ) {
                    // ignore
                }
            }
            body.append(String.valueOf(cpuSpeed));
            body.append("\nmem ");
            body.append(String.valueOf(ramInMb));
            body.append("\nsmp ");
            body.append(String.valueOf(cpuCount));
            if( withLaunchOptions.getVlanId() != null ) {
                body.append("\nnic:1:vlan ");
                body.append(withLaunchOptions.getVlanId().replaceAll("\n", " "));
            }
            body.append("\n");

            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            if( logger.isInfoEnabled() ) {
                logger.info("Creating server....");
            }
            VirtualMachine vm = toVirtualMachine(method.postObject("/servers/create", body.toString()));

            if( logger.isDebugEnabled() ) {
                logger.debug("vm=" + vm);
            }
            if( vm == null ) {
                throw new CloudException("No virtual machine was provided in the response");
            }
            if( logger.isInfoEnabled() ) {
                logger.info("Waiting for " + vm.getProviderVirtualMachineId() + " to be STOPPED or RUNNING...");
            }
            vm = waitForState(vm, CalendarWrapper.MINUTE*15L, VmState.STOPPED, VmState.RUNNING);
            if( logger.isDebugEnabled() ) {
                logger.debug("post wait vm=" + vm);
            }
            if( vm == null ) {
                throw new CloudException("Virtual machine disappeared waiting for startup state");
            }
            if( logger.isDebugEnabled() ) {
                logger.debug("status.vm." + vm.getProviderVirtualMachineId() + "=" + vm.getCurrentState());
            }
            if( !VmState.RUNNING.equals(vm.getCurrentState()) ) {
                if( logger.isInfoEnabled() ) {
                    logger.info("Setting up a separate thread to start " + vm.getProviderVirtualMachineId() + "...");
                }
                final String id = vm.getProviderVirtualMachineId();

                Thread t = new Thread() {
                    public void run() {
                        try {
                            VirtualMachine vm = null;

                            for( int i=0; i<5; i++ ) {
                                try {
                                    if( vm == null ) {
                                        try {
                                            vm = getVirtualMachine(id);
                                        }
                                        catch( Throwable ignore ) {
                                            // ignore
                                        }
                                    }
                                    if( vm != null ) {
                                        if( logger.isInfoEnabled() ) {
                                            logger.info("Verifying the state of " + id);
                                        }
                                        vm = waitForState(vm, CalendarWrapper.MINUTE*15L, VmState.STOPPED, VmState.RUNNING);
                                        if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) || VmState.RUNNING.equals(vm.getCurrentState()) ) {
                                            if( logger.isInfoEnabled() ) {
                                                logger.info("Pre-emptive return due to non-existence or true running: " + id);
                                            }
                                            return;
                                        }
                                    }
                                    if( logger.isInfoEnabled() ) {
                                        logger.info("Start attempt " + (i+1) + " on " + id);
                                    }
                                    ServerSupport.this.start(id);
                                    if( logger.isInfoEnabled() ) {
                                        logger.info("VM " + id + " started");
                                    }
                                    return;
                                }
                                catch( Exception e ) {
                                    logger.warn("Failed to start virtual machine " + id + " post-create: " + e.getMessage());
                                }
                                try { Thread.sleep(60000L); }
                                catch( InterruptedException ignore ) { }
                            }
                            if( logger.isInfoEnabled() ) {
                                logger.info("VM " + id + " never started");
                                if( vm != null ) {
                                    logger.debug("status.vm." + id + " (not started)=" + vm.getCurrentState());
                                }
                            }
                        }
                        finally {
                            provider.release();
                        }
                    }
                };

                provider.hold();
                t.setName("Start CloudSigma VM " + id);
                t.setDaemon(true);
                t.start();
            }
            return vm;
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + ServerSupport.class.getName() + ".launch()");
            }
        }
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nonnull String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String... firewallIds) throws InternalException, CloudException {
        return launch(fromMachineImageId, product, dataCenterId, name, description, withKeypairId, inVlanId, withAnalytics, asSandbox, firewallIds, new Tag[0]);
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull String fromMachineImageId, @Nonnull VirtualMachineProduct product, @Nonnull String dataCenterId, @Nonnull String name, @Nonnull String description, @Nullable String withKeypairId, @Nullable String inVlanId, boolean withAnalytics, boolean asSandbox, @Nullable String[] firewallIds, @Nullable Tag... tags) throws InternalException, CloudException {
        VMLaunchOptions cfg = VMLaunchOptions.getInstance(product.getProviderProductId(), fromMachineImageId, name, description);

        if( withKeypairId != null ) {
            cfg.withBoostrapKey(withKeypairId);
        }
        if( inVlanId != null ) {
            cfg.inVlan(null, dataCenterId, inVlanId);
        }
        else {
            cfg.inDataCenter(dataCenterId);
        }
        if( withAnalytics ) {
            cfg.withExtendedAnalytics();
        }
        if( firewallIds != null && firewallIds.length > 0 ) {
            cfg.behindFirewalls(firewallIds);
        }
        if( tags != null && tags.length > 0 ) {
            for( Tag t : tags ) {
                cfg.withMetaData(t.getKey(), t.getValue());
            }
        }
        return launch(cfg);
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    private transient ArrayList<VirtualMachineProduct> cachedProducts;

    @Override
    public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
        ArrayList<VirtualMachineProduct> products = cachedProducts;

        if( products == null ) {
            products = new ArrayList<VirtualMachineProduct>();

            for( int ram : new int[] { 512, 1024, 2048, 4096, 8192, 12288, 16384, 20480, 24576, 28668, 32768 } ) {
                for( int cpu : new int[] { 1000, 1200, 1500, 2000, 2500, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000, 12000, 14000, 16000, 18000, 20000} ) {
                    for( int cpuCount : new int[] { 1, 2, 4, 8 } ) {
                        if( cpuCount == 1 ) {
                            products.add(getProduct(ram + ":" + cpu));
                        }
                        else {
                            products.add(getProduct(ram + ":" + cpu + ":" + cpuCount));
                        }
                    }
                }
            }
            cachedProducts = products;
        }
        return products;
    }

    static private volatile Collection<Architecture> architectures;

    @Override
    public Iterable<Architecture> listSupportedArchitectures() {
        if( architectures == null ) {
            ArrayList<Architecture> list = new ArrayList<Architecture>();

            list.add(Architecture.I64);
            list.add(Architecture.I32);
            architectures = Collections.unmodifiableCollection(list);
        }
        return architectures;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVirtualMachineStatus() throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        List<Map<String,String>> objects = method.list("/servers/info");

        if( objects == null ) {
            throw new CloudException("No servers endpoint found");
        }
        ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
        for( Map<String,String> object : objects ) {
            ResourceStatus vm = toStatus(object);

            if( vm != null ) {
                list.add(vm);
            }
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        List<Map<String,String>> objects = method.list("/servers/info");

        if( objects == null ) {
            throw new CloudException("No servers endpoint found");
        }
        ArrayList<VirtualMachine> list = new ArrayList<VirtualMachine>();
        for( Map<String,String> object : objects ) {
            VirtualMachine vm = toVirtualMachine(object);

            if( vm != null ) {
                list.add(vm);
            }
        }
        return list;
    }

    @Override
    public void pause(@Nonnull String vmId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("CloudSigma does not support pause/unpause");
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        VirtualMachine vm = getVirtualMachine(vmId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + vmId);
        }
        stop(vmId);

        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 20L);

        while( timeout > System.currentTimeMillis() ) {
            try { vm = getVirtualMachine(vmId); }
            catch( Exception ignore ) { }
            if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                throw new CloudException("Server disappeared during reboot");
            }
            if( VmState.STOPPED.equals(vm.getCurrentState()) ) {
                break;
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
        }
        start(vmId);
    }

    public void releaseIP(@Nonnull IpAddress address) throws CloudException, InternalException {
        String serverId = address.getServerId();

        if( serverId == null ) {
            throw new CloudException("No server is assigned to " + address.getProviderIpAddressId());
        }
        VirtualMachine vm = getVirtualMachine(serverId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + serverId);
        }
        StringBuilder body = new StringBuilder();

        body.append("nic:0:dhcp \n");
        change(vm, body.toString());
    }

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support suspend/resume");
    }

    @Override
    public void start(@Nonnull String vmId) throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        method.getObject(toServerURL(vmId, "start"));
    }

    @Override
    public void stop(@Nonnull String vmId) throws InternalException, CloudException {
        stop(vmId, false);
        stop(vmId, true);
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws InternalException, CloudException {
        if( logger.isTraceEnabled() ) {
            logger.trace("ENTER - " + ServerSupport.class.getName() + ".stop(" + vmId + "," + force + ")");
        }
        try {
            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            method.getObject(toServerURL(vmId, force ? "stop" : "shutdown"));

            long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE * 5L);

            while( System.currentTimeMillis() > timeout ) {
                try {
                    VirtualMachine vm = getVirtualMachine(vmId);

                    if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) || VmState.STOPPED.equals(vm.getCurrentState()) ) {
                        return;
                    }
                }
                catch( Throwable ignore ) {
                    // ignore
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
            }
        }
        finally {
            if( logger.isTraceEnabled() ) {
                logger.trace("EXIT - " + ServerSupport.class.getName() + ".stop()");
            }
        }
    }

    @Override
    public boolean supportsAnalytics() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsPauseUnpause(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return true;
    }

    @Override
    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        return false;
    }

    @Override
    public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support suspend/resume");
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        VirtualMachine vm = getVirtualMachine(vmId);

        if( vm == null ) {
            throw new CloudException("No such virtual machine: " + vmId);
        }
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        method.getObject(toServerURL(vmId, "destroy"));

        long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE*10L);

        try { vm = getVirtualMachine(vmId); }
        catch( Exception ignore ) { }
        while( timeout > System.currentTimeMillis() ) {
            if( vm == null || VmState.TERMINATED.equals(vm.getCurrentState()) ) {
                return;
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
        }
        logger.warn("System timed out waiting ro the VM termination to complete");
    }

    @Override
    public void unpause(@Nonnull String vmId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support pause/unpause");
    }

    @Override
    public void updateTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void updateTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void removeTags(@Nonnull String vmId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public void removeTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private boolean isPublic(@Nonnull String ip) {
        if( ip.startsWith("127.0.0.") ) {
            return false;
        }
        if( ip.startsWith("10.") ) {
            return false;
        }
        if( ip.startsWith("192.168.")) {
            return false;
        }
        if( ip.startsWith("172.") ) {
            String[] parts = ip.split("\\.");

            if( parts.length != 4 ) {
                return true;
            }
            try {
                int x = Integer.parseInt(parts[1]);

                if( x >= 16 && x <33 ) {
                    return false;
                }
            }
            catch( NumberFormatException ignore ) {
                // ignore
            }
        }
        return true;
    }

    private void setIP(@Nonnull VirtualMachine vm, @Nonnull TreeSet<String> ips) {
        ArrayList<String> pub = new ArrayList<String>();
        ArrayList<String> priv = new ArrayList<String>();

        for( String ip : ips ) {
            if( isPublic(ip) ) {
                pub.add(ip);
            }
            else {
                priv.add(ip);
            }
        }
        vm.setPrivateIpAddresses(priv.toArray(new String[priv.size()]));
        vm.setPublicIpAddresses(pub.toArray(new String[pub.size()]));
    }


    private @Nullable ResourceStatus toStatus(@Nullable Map<String,String> object) throws CloudException {
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
        String id = null;

        if( object.containsKey("server") ) {
            id = object.get("server");
        }
        if( id == null || id.equals("") ) {
            return null;
        }
        VmState state = VmState.PENDING;

        if( object.containsKey("status") ) {
            String status = object.get("status");


            if( status != null ) {
                if( status.equalsIgnoreCase("stopped") ) {
                    state = VmState.STOPPED;
                }
                else if( status.equalsIgnoreCase("active") ) {
                    state = VmState.RUNNING;
                }
                else if( status.equalsIgnoreCase("paused") ) {
                    state = VmState.PAUSED;
                }
                else if( status.equalsIgnoreCase("dead") || status.equalsIgnoreCase("dumped") ) {
                    state = VmState.TERMINATED;
                }
                else if( status.startsWith("imaging") ) {
                    state = VmState.PENDING;
                }
                else {
                    logger.warn("DEBUG: Unknown CloudSigma server status: " + status);
                }
            }
        }
        return new ResourceStatus(id, state);
    }

    private @Nullable VirtualMachine toVirtualMachine(@Nullable Map<String,String> object) throws CloudException {
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
        VirtualMachine vm = new VirtualMachine();

        vm.setPersistent(true);
        vm.setCurrentState(VmState.PENDING);
        vm.setImagable(false);
        vm.setPausable(false);
        vm.setRebootable(false);
        vm.setPlatform(Platform.UNKNOWN);
        vm.setProviderDataCenterId(regionId + "-a");
        vm.setProviderRegionId(regionId);
        vm.setArchitecture(Architecture.I64);
        if( object.containsKey("server") ) {
            String id = object.get("server");

            vm.setProviderVirtualMachineId(id);
        }
        String boot = object.get("boot");

        if( boot == null || boot.trim().equals("") ) {
            boot = "ide:0:0";
        }
        String imageId = object.get(boot);

        if( imageId != null && !imageId.equals("") ) {
            vm.setProviderMachineImageId(imageId);
        }
        String vlanId = object.get("nic:0:vlan");

        if( vlanId == null || vlanId.trim().equals("") ) {
            vlanId = object.get("nic:1:vlan");
            if( vlanId == null || vlanId.trim().equals("") ) {
                vlanId = null;
            }
        }
        if( vlanId != null ) {
            vm.setProviderVlanId(vlanId);
        }
        for( int i=0; i<8; i++ ) {
            String key = "block:" + i;
            String value = object.get(key);

            if( value != null ) {
                vm.setTag(key, value);
            }
        }
        TreeSet<String> allIps = new TreeSet<String>();
        String ip = object.get("nic:0:dhcp");

        if( ip != null && !ip.equals("") && !ip.equals("auto") ) {
            vm.setProviderAssignedIpAddressId(ip);
            allIps.add(ip);
        }
        ip = object.get("vnc:ip");
        if( ip != null && !ip.equals("") && !ip.equals("auto") ) {
            allIps.add(ip);
        }
        for( int i=1; i<10; i++ ) {
            ip = object.get("nic:" + i + ":dhcp");
            if( ip == null ) {
                break;
            }
            if( ip != null && !ip.equals("") && !ip.equals("auto") ) {
                allIps.add(ip);
            }
        }
        if( !allIps.isEmpty() ) {
            setIP(vm, allIps);
        }
        if( object.containsKey("user") ) {
            String user = object.get("user");

            vm.setProviderOwnerId(user);
        }
        if( object.containsKey("name") ) {
            String value = object.get("name");

            vm.setName(value);
        }
        if( object.containsKey("vnc:password") ) {
            String value = object.get("vnc:password");

            vm.setRootUser("root");
            vm.setRootPassword(value);
        }
        if( object.containsKey("status") ) {
            String status = object.get("status");


            if( status != null ) {
                if( status.equalsIgnoreCase("stopped") ) {
                    vm.setCurrentState(VmState.STOPPED);
                }
                else if( status.equalsIgnoreCase("active") ) {
                    vm.setCurrentState(VmState.RUNNING);
                }
                else if( status.equalsIgnoreCase("paused") ) {
                    vm.setCurrentState(VmState.PAUSED);
                }
                else if( status.equalsIgnoreCase("dead") || status.equalsIgnoreCase("dumped") ) {
                    vm.setCurrentState(VmState.TERMINATED);
                }
                else if( status.startsWith("imaging") ) {
                    vm.setCurrentState(VmState.PENDING);
                }
                else {
                    logger.warn("DEBUG: Unknown CloudSigma server status: " + status);
                }
            }
            else {
                vm.setCurrentState(VmState.PENDING);
            }
        }
        String cpuCount = "1", cpuSpeed = "1000", ram = "512";

        try {
            String tmp = object.get("cpu");

            if( tmp != null ) {
                cpuSpeed = String.valueOf(Integer.parseInt(tmp));
            }
        }
        catch( NumberFormatException ignore ) {
            // ignore
        }
        try {
            String tmp = object.get("smp");

            if( tmp != null ) {
                cpuCount = String.valueOf(Integer.parseInt(tmp));
            }
        }
        catch( NumberFormatException ignore ) {
            // ignore
        }
        try {
            String tmp = object.get("mem");

            if( tmp != null ) {
                ram = String.valueOf(Integer.parseInt(tmp));
            }
        }
        catch( NumberFormatException ignore ) {
            // ignore
        }
        if( cpuCount.equals("1") ) {
            vm.setProductId(ram + ":" + cpuSpeed);
        }
        else {
            vm.setProductId(ram + ":" + cpuSpeed + ":" + cpuCount);
        }
        if( vm.getProviderVirtualMachineId() == null ) {
            return null;
        }
        if( vm.getName() == null ) {
            vm.setName(vm.getProviderVirtualMachineId());
        }
        if( vm.getDescription() == null ) {
            vm.setDescription(vm.getName());
        }
        vm.setClonable(VmState.PAUSED.equals(vm.getCurrentState()));
        return vm;
    }

    private @Nonnull String toServerURL(@Nonnull String vmId, @Nonnull String action) throws InternalException {
        try {
            return ("/servers/" + URLEncoder.encode(vmId, "utf-8") + "/" + action);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
    }

    private @Nullable VirtualMachine waitForState(@Nonnull VirtualMachine vm, long timeoutPeriod, @Nonnull VmState ... states) {
        long timeout = System.currentTimeMillis() + timeoutPeriod;
        VirtualMachine newVm = vm;

        while( timeout > System.currentTimeMillis() ) {
            if( newVm == null ) {
                return null;
            }
            for( VmState state : states ) {
                if( state.equals(newVm.getCurrentState()) ) {
                    return newVm;
                }
            }
            try { Thread.sleep(15000L); }
            catch( InterruptedException ignore ) { }
            try { newVm = getVirtualMachine(vm.getProviderVirtualMachineId()); }
            catch( Exception ignore ) { }
        }
        return newVm;
    }
}
