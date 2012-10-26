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
package org.dasein.cloud.cloudsigma.compute.image;

import org.apache.log4j.Logger;
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.CloudSigmaConfigurationException;
import org.dasein.cloud.cloudsigma.CloudSigmaMethod;
import org.dasein.cloud.cloudsigma.NoContextException;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.util.uom.storage.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Maps CloudSigma drives to the concept of machine images. While there's a fairly huge disconnect between the
 * CloudSigma concept of drives and what Dasein Cloud thinks of as a machine image, this implementation attempts
 * to bridge that gap.
 * <p>Created by George Reese: 10/26/12 10:30 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class BootDriveSupport implements MachineImageSupport {
    static private final Logger logger = CloudSigma.getLogger(BootDriveSupport.class);

    private CloudSigma provider;

    public BootDriveSupport(@Nonnull CloudSigma provider) { this.provider = provider; }

    public @Nonnull Map<String,String> cloneDrive(@Nonnull String driveId, @Nonnull String name, Platform os) throws CloudException, InternalException {
        Map<String,String> currentDrive = getDrive(driveId);

        if( currentDrive == null ) {
            throw new CloudException("No such drive: " + driveId);
        }
        StringBuilder body = new StringBuilder();

        body.append("name ");
        body.append(name.replaceAll("\n", " "));
        String size = currentDrive.get("size");

        if( size != null ) {
            body.append("\nsize ");
            body.append(size);
        }
        body.append("\nclaim:type exclusive");
        if( os != null ) {
            // TODO: find a way to tag this
        }
        body.append("\n");

        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        Map<String,String> object = method.postObject(toDriveURL(driveId, "clone"), body.toString());

        if( object == null ) {
            throw new CloudException("Clone supposedly succeeded, but no drive information was provided");
        }
        return object;
    }

    @Override
    public void downloadImage(@Nonnull String machineImageId, @Nonnull OutputStream toOutput) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support image downloading");
    }

    public @Nullable Map<String,String> getDrive(String driveId) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        Map<String,String> object = method.getObject(toDriveURL(driveId, "info"));

        if( object == null ) {
            System.out.println("Failed " + driveId + ", looking...");
            object = method.getObject("/drives/standard/img/" + driveId + "/info");
            System.out.println("SUCCESS: " + (object != null));
        }
        return object;
    }

    @Override
    public MachineImage getMachineImage(@Nonnull String machineImageId) throws CloudException, InternalException {
        return toMachineImage(getDrive(machineImageId));
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return "drive";
    }

    @Override
    public boolean hasPublicLibrary() {
        return true;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachine(final @Nonnull String vmId, final @Nullable String name, final @Nullable String description) throws CloudException, InternalException {
        final AsynchronousTask<String> task = new AsynchronousTask<String>();
        final String fname = (name == null ? "Clone of " + vmId : name);

        provider.hold();
        Thread t = new Thread() {
            public void run() {
                try {
                    VirtualMachine vm;

                    try {
                        vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);
                        if( vm == null ) {
                            throw new CloudException("Virtual machine not found: " + vmId);
                        }
                    }
                    catch( CloudException e ) {
                        logger.error("Unable to load virtual machine: " + e.getMessage());
                        task.complete(e);
                        return;
                    }
                    catch( InternalException e ) {
                        logger.error("Unable to load virtual machine: " + e.getMessage());
                        task.complete(e);
                        return;
                    }
                    try {
                        provider.getComputeServices().getVirtualMachineSupport().stop(vmId);
                    }
                    catch( CloudException e ) {
                        logger.error("Unable to stop virtual machine: " + e.getMessage());
                        task.complete(e);
                        return;
                    }
                    catch( InternalException e ) {
                        logger.error("Unable to stop virtual machine: " + e.getMessage());
                        task.complete(e);
                        return;
                    }
                    try {
                        String driveId = vm.getProviderMachineImageId();

                        try {
                            Map<String,String> object = cloneDrive(driveId, fname, vm.getPlatform());
                            String id = object.get("drive");

                            if( id != null ) {
                                task.completeWithResult(id);
                            }
                            else {
                                throw new CloudException("Drive cloning completed, but no ID was provided for clone");
                            }
                        }
                        catch( CloudException e ) {
                            logger.error("Unable to stop virtual machine: " + e.getMessage());
                            task.complete(e);
                        }
                        catch( InternalException e ) {
                            logger.error("Unable to stop virtual machine: " + e.getMessage());
                            task.complete(e);
                        }
                    }
                    finally {
                        try {
                            provider.getComputeServices().getVirtualMachineSupport().start(vmId);
                        }
                        catch( Throwable ignore ) {
                            logger.warn("Failed to restart " + vmId + " after drive cloning");
                        }
                    }
                }
                finally {
                    provider.release();
                }
            }
        };

        t.setName("Image " + vmId);
        t.setDaemon(true);
        t.start();
        return task;
    }

    @Override
    public @Nonnull AsynchronousTask<String> imageVirtualMachineToStorage(String vmId, String name, String description, String directory) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support imaging to cloud storage");
    }

    @Override
    public @Nonnull String installImageFromUpload(@Nonnull MachineImageFormat format, @Nonnull InputStream imageStream) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image installations from upload are not currently supported");
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        MachineImage img = getMachineImage(machineImageId);

        if( img == null ) {
            return false;
        }
        String owner = img.getProviderOwnerId();

        return (owner == null || owner.equals("00000000-0000-0000-0000-000000000001"));
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return provider.getComputeServices().getVirtualMachineSupport().isSubscribed();
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String me = ctx.getAccountNumber();

        ArrayList<MachineImage> list = new ArrayList<MachineImage>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        List<Map<String,String>> matches = method.list("/drives/info");

        if( matches == null ) {
            throw new CloudException("Could not identify drive endpoint for CloudSigma");
        }
        for( Map<String,String> object : matches ) {
            String id = object.get("user");

            if( id != null && id.trim().equals("") ) {
                id = null;
            }
            if( me.equals(id) ) {
                MachineImage img = toMachineImage(object);

                if( img != null ) {
                    list.add(img);
                }
            }
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(String accountId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String me = ctx.getAccountNumber();

        if( me.equals(accountId) ) {
            return listMachineImages();
        }
        else if( accountId == null || accountId.equals("") ) {
            accountId = "00000000-0000-0000-0000-000000000001";
        }
        ArrayList<MachineImage> list = new ArrayList<MachineImage>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        List<Map<String,String>> matches = method.list("/drives/standard/img/info");

        if( matches == null ) {
            throw new CloudException("Could not identify drive endpoint for CloudSigma");
        }
        for( Map<String,String> object : matches ) {
            String id = object.get("user");

            if( id != null && id.trim().equals("") ) {
                id = null;
            }
            if( accountId.equals(id) ) {
                MachineImage img = toMachineImage(object);

                if( img != null ) {
                    list.add(img);
                }
            }
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.OVF);
    }

    private boolean matches(@Nonnull MachineImage image, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) {
        if( architecture != null && !architecture.equals(image.getArchitecture()) ) {
            return false;
        }
        if( platform != null && !platform.equals(Platform.UNKNOWN) ) {
            Platform mine = image.getPlatform();

            if( platform.isWindows() && !mine.isWindows() ) {
                return false;
            }
            if( platform.isUnix() && !mine.isUnix() ) {
                return false;
            }
            if( platform.isBsd() && !mine.isBsd() ) {
                return false;
            }
            if( platform.isLinux() && !mine.isLinux() ) {
                return false;
            }
            if( platform.equals(Platform.UNIX) ) {
                if( !mine.isUnix() ) {
                    return false;
                }
            }
            else if( !platform.equals(mine) ) {
                return false;
            }
        }
        if( keyword != null ) {
            keyword = keyword.toLowerCase();
            if( !image.getDescription().toLowerCase().contains(keyword) ) {
                if( !image.getName().toLowerCase().contains(keyword) ) {
                    if( !image.getProviderMachineImageId().toLowerCase().contains(keyword) ) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull String registerMachineImage(String atStorageLocation) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support registering machine images from cloud storage");
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        if( method.postString(toDriveURL(machineImageId, "destroy"), "") == null ) {
            throw new CloudException("Unable to identify drives endpoint for removal");
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ArrayList<MachineImage> list = new ArrayList<MachineImage>();

        for( MachineImage img : listMachineImages() ) {
            if( img != null && matches(img, keyword, platform, architecture) ) {
                list.add(img);
            }
        }
        for( MachineImage img : listMachineImagesOwnedBy(null) ) {
            if( img != null && matches(img, keyword, platform, architecture) ) {
                list.add(img);
            }
        }
        return list;
    }

    @Override
    public void shareMachineImage(@Nonnull String machineImageId, @Nonnull String withAccountId, boolean allow) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support machine image sharing");
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsImageSharing() {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() {
        return false;
    }

    @Override
    public @Nonnull String transfer(@Nonnull CloudProvider fromCloud, @Nonnull String machineImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("CloudSigma does not support image transfers to other clouds");
    }

    @Override
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    private @Nullable MachineImage toMachineImage(@Nullable Map<String,String> drive) throws CloudException, InternalException {
        if( drive == null ) {
            return null;
        }
        if( drive.containsKey("claimed") ) {
            String id = drive.get("claimed");

            if( id != null && !id.trim().equals("") && !id.contains("imaging") ) {
                return null;
            }
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String regionId = ctx.getRegionId();

        if( regionId == null ) {
            throw new CloudSigmaConfigurationException("No region was specified for this request");
        }
        MachineImage image = new MachineImage();
        Platform platform = null;

        image.setProviderRegionId(regionId);
        image.setCurrentState(MachineImageState.PENDING);
        image.setType(MachineImageType.VOLUME);

        String id = drive.get("drive");

        if( id != null && !id.equals("") ) {
            image.setProviderMachineImageId(id);
        }
        String name = drive.get("name");

        if( name != null && !name.equals("") ) {
            image.setName(name);
        }
        String description = drive.get("install_notes");

        if( description != null && !description.equals("") ) {
            image.setDescription(description);
        }
        String user = drive.get("user");

        if( user != null && !user.equals("") ) {
            image.setProviderOwnerId(drive.get("user"));
        }
        else {
            image.setProviderOwnerId("00000000-0000-0000-0000-000000000001");
        }
        String s = drive.get("status");

        if( s != null ) {
            if( s.equalsIgnoreCase("active") ) {
                image.setCurrentState(MachineImageState.ACTIVE);
            }
            else if( s.equalsIgnoreCase("inactive") ) {
                image.setCurrentState(MachineImageState.DELETED);
            }
            else if( s.startsWith("copying") ) {
                image.setCurrentState(MachineImageState.PENDING);
            }
            else {
                logger.warn("DEBUG: Unknown drive state for CloudSigma: " + s);
            }
        }
        if( MachineImageState.ACTIVE.equals(image.getCurrentState()) ) {
            s = drive.get("imaging");
            if( s != null ) {
                System.out.println("Imaging: " + s);
                image.setCurrentState(MachineImageState.PENDING);
            }
        }
        String size = drive.get("size");

        if( size != null ) {
            try {
                image.setTag("size", new Storage<org.dasein.util.uom.storage.Byte>(Long.parseLong(size), Storage.BYTE).toString());
            }
            catch( NumberFormatException ignore ) {
                logger.warn("Unknown size value: " + size);
            }
        }
        String software = drive.get("licenses");

        if( software != null ) {
            image.setSoftware(software);
        }
        else {
            image.setSoftware("");
        }
        String bits = drive.get("bits");

        if( bits != null && bits.contains("32") ) {
            image.setArchitecture(Architecture.I32);
        }
        else {
            image.setArchitecture(Architecture.I64);
        }
        String os = drive.get("os");

        if( os != null && !os.equals("") ) {
            platform = Platform.guess(os);
        }
        if( platform == null || platform.equals(Platform.UNKNOWN) ) {
            platform = Platform.guess(image.getName());
        }
        else if( platform.equals(Platform.UNIX) ) {
            Platform p = Platform.guess(image.getName());

            if( !p.equals(Platform.UNKNOWN) ) {
                platform = p;
            }
        }
        image.setPlatform(platform);
        if( image.getProviderOwnerId() == null ) {
            image.setProviderOwnerId(ctx.getAccountNumber());
        }
        if( image.getProviderMachineImageId() == null ) {
            return null;
        }
        if( image.getName() == null ) {
            image.setName(image.getProviderMachineImageId());
        }
        if( image.getDescription() == null ) {
            image.setDescription(image.getName());
        }
        return image;
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
