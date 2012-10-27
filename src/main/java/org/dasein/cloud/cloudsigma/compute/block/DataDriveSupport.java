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
package org.dasein.cloud.cloudsigma.compute.block;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.CloudSigmaConfigurationException;
import org.dasein.cloud.cloudsigma.CloudSigmaMethod;
import org.dasein.cloud.cloudsigma.NoContextException;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeCreateOptions;
import org.dasein.cloud.compute.VolumeProduct;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.compute.VolumeType;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.util.uom.storage.*;

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

/**
 * Implements support for block storage devices that may be attached to virtual machines in CloudSigma.
 * <p>Created by George Reese: 10/26/12 4:08 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class DataDriveSupport implements VolumeSupport {
    static private final Logger logger = CloudSigma.getLogger(DataDriveSupport.class);

    private CloudSigma provider;

    public DataDriveSupport(@Nonnull CloudSigma provider) { this.provider = provider; }

    @Override
    public void attach(@Nonnull String volumeId, @Nonnull String toServer, @Nonnull String deviceId) throws InternalException, CloudException {
        Volume v = getVolume(volumeId);

        if( v == null ) {
            throw new CloudException("No such volume: " + volumeId);
        }
        provider.getComputeServices().getVirtualMachineSupport().attach(v, toServer, deviceId);
    }

    @Override
    @Deprecated
    public @Nonnull String create(@Nonnull String fromSnapshot, @Nonnegative int sizeInGb, @Nonnull String inZone) throws InternalException, CloudException {
        String name = "vol-" + System.currentTimeMillis();

        //noinspection ConstantConditions
        if( fromSnapshot == null ) {
            return createVolume(VolumeCreateOptions.getInstance(new Storage<Gigabyte>(sizeInGb, Storage.GIGABYTE), name, name));
        }
        else {
           return createVolume(VolumeCreateOptions.getInstanceForSnapshot(fromSnapshot, new Storage<Gigabyte>(sizeInGb, Storage.GIGABYTE), name, name));
        }
    }

    @Override
    public @Nonnull String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        if( options.getSnapshotId() != null ) {
            throw new CloudException("CloudSigma does not support snapshots");
        }
        StringBuilder body = new StringBuilder();

        body.append("name ");
        body.append(options.getName().replaceAll("\n", " "));
        body.append("\nsize ");
        body.append(String.valueOf(options.getVolumeSize().convertTo(Storage.BYTE).longValue()));
        body.append("\nclaim:type exclusive");
        if( options.getVolumeProductId() != null || "ssd".equals(options.getVolumeProductId()) ) {
            body.append("\ntags affinity:ssd");
        }
        body.append("\n");

        CloudSigmaMethod method = new CloudSigmaMethod(provider);
        Volume volume = toVolume(method.postObject("/drives/create", body.toString()));

        if( volume == null ) {
            throw new CloudException("Volume created but no volume information was provided");
        }
        return volume.getProviderVolumeId();
    }

    @Override
    public void detach(@Nonnull String volumeId) throws InternalException, CloudException {
        Volume v = getVolume(volumeId);

        if( v == null ) {
            throw new CloudException("No such volume: " + volumeId);
        }
        provider.getComputeServices().getVirtualMachineSupport().detach(v);
    }

    public @Nullable  Map<String,String> getDrive(String driveId) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        return method.getObject(toDriveURL(driveId, "info"));
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Override
    public @Nonnull String getProviderTermForVolume(@Nonnull Locale locale) {
        return "drive";
    }

    @Override
    public Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        return toVolume(getDrive(volumeId));
    }

    @Override
    public @Nonnull Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    static private Collection<String> deviceIds;

    @Override
    public @Nonnull Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        if( deviceIds == null ) {
            ArrayList<String> ids = new ArrayList<String>();

            for( int i=0; i<8; i++ ) {
                ids.add(String.valueOf(i));
            }
            deviceIds = Collections.unmodifiableList(ids);
        }
        return deviceIds;
    }

    @Override
    public @Nonnull Iterable<VolumeProduct> listVolumeProducts() throws InternalException, CloudException {
        ArrayList<VolumeProduct> products = new ArrayList<VolumeProduct>();

        products.add(VolumeProduct.getInstance("hdd", "HDD", "HDD Affinity", VolumeType.HDD));
        products.add(VolumeProduct.getInstance("ssd", "SSD", "SSD Affinity", VolumeType.SSD));
        return products;
    }

    @Override
    public @Nonnull Iterable<Volume> listVolumes() throws InternalException, CloudException {
        ArrayList<Volume> list = new ArrayList<Volume>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        List<Map<String,String>> matches = method.list("/drives/info");

        if( matches == null ) {
            throw new CloudException("Could not identify drive endpoint for CloudSigma");
        }
        for( Map<String,String> object : matches ) {
            Volume volume = toVolume(object);

            if( volume != null ) {
                list.add(volume);
            }
        }
        return list;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
    }

    @Override
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        if( method.postString(toDriveURL(volumeId, "destroy"), "") == null ) {
            throw new CloudException("Unable to identify drives endpoint for removal");
        }
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable Volume toVolume(@Nullable Map<String,String> drive) throws CloudException, InternalException {
        if( drive == null ) {
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

        Volume volume = new Volume();

        volume.setProviderDataCenterId(regionId + "-a");
        volume.setProviderRegionId(regionId);

        String id = drive.get("drive");

        if( id != null && !id.equals("") ) {
            volume.setProviderVolumeId(id);
        }

        String host = drive.get("host");

        if( host != null && !host.equals("") ) {
            VirtualMachine vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(host);

            if( vm != null ) {
                volume.setProviderVirtualMachineId(host);

                String deviceId = provider.getComputeServices().getVirtualMachineSupport().getDeviceId(vm, id);

                if( deviceId != null ) {
                    volume.setDeviceId(deviceId);
                }
            }
        }

        String name = drive.get("name");

        if( name != null && name.length() > 0 ) {
            volume.setName(name);
        }

        String size = drive.get("size");

        if( size != null && size.length() > 0 ) {
            try {
                volume.setSize(new Storage<org.dasein.util.uom.storage.Byte>(Long.parseLong(size), Storage.BYTE));
            }
            catch( NumberFormatException e ) {
                logger.warn("Invalid drive size: " + size);
            }
        }
        if( volume.getSize() == null ) {
            volume.setSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
        }

        String s = drive.get("status");

        if( s != null ) {
            if( s.equalsIgnoreCase("active") ) {
                volume.setCurrentState(VolumeState.AVAILABLE);
            }
            else if( s.equalsIgnoreCase("inactive") ) {
                volume.setCurrentState(VolumeState.DELETED);
            }
            else if( s.startsWith("copying") ) {
                volume.setCurrentState(VolumeState.PENDING);
            }
            else {
                logger.warn("DEBUG: Unknown drive state for CloudSigma: " + s);
            }
        }
        if( VolumeState.AVAILABLE.equals(volume.getCurrentState()) ) {
            s = drive.get("imaging");
            if( s != null ) {
                volume.setCurrentState(VolumeState.PENDING);
            }
        }

        String tags = drive.get("tags");

        if( tags != null && tags.equals("affinity:ssd") ) {
            volume.setType(VolumeType.SSD);
            volume.setProviderProductId("ssd");
        }
        else {
            volume.setType(VolumeType.HDD);
            volume.setProviderProductId("hdd");
        }

        if( volume.getProviderVolumeId() == null ) {
            return null;
        }
        if( volume.getName() == null ) {
            volume.setName(volume.getProviderVolumeId());
        }
        if( volume.getDescription() == null ) {
            volume.setDescription(volume.getName());
        }

        String os = drive.get("os");

        if( os != null && !os.equals("") ) {
            Platform platform = Platform.guess(os);

            if( platform.equals(Platform.UNKNOWN) ) {
                platform = Platform.guess(volume.getName());
            }
            else if( platform.equals(Platform.UNIX) ) {
                Platform p = Platform.guess(volume.getName());

                if( !p.equals(Platform.UNKNOWN) ) {
                    platform = p;
                }
            }
            volume.setGuestOperatingSystem(platform);
            volume.setRootVolume(true);
        }

        return volume;
    }

    private @Nonnull String toDriveURL(@Nonnull String vmId, @Nonnull String action) throws InternalException {
        try {
            return ("/drives/" + URLEncoder.encode(vmId, "utf-8") + "/" + action);
        }
        catch( UnsupportedEncodingException e ) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
    }
}
