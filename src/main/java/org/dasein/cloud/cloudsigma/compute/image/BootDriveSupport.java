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
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.CloudSigmaConfigurationException;
import org.dasein.cloud.cloudsigma.CloudSigmaMethod;
import org.dasein.cloud.cloudsigma.NoContextException;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
public class BootDriveSupport extends AbstractImageSupport {
    static private final Logger logger = CloudSigma.getLogger(BootDriveSupport.class);

    private CloudSigma provider;

    public BootDriveSupport(@Nonnull CloudSigma provider) {
        super(provider);
        this.provider = provider;
    }

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
    protected @Nonnull MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        String vmId = options.getVirtualMachineId();

        if( vmId == null ) {
            throw new CloudException("No virtual machine was specified for capture");
        }
        VirtualMachine vm;

        vm = provider.getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);
        if( vm == null ) {
            throw new CloudException("Virtual machine not found: " + options.getVirtualMachineId());
        }
        provider.getComputeServices().getVirtualMachineSupport().stop(vmId);
        String driveId = vm.getProviderMachineImageId();

        try {
            Map<String,String> object = cloneDrive(driveId, options.getName(), vm.getPlatform());
            String id = object.get("drive");
            MachineImage img = null;

            if( id != null ) {
                img = getImage(id);
            }
            if( img == null ) {
                throw new CloudException("Drive cloning completed, but no ID was provided for clone");
            }
            if( task != null ) {
                task.completeWithResult(img);
            }
            return img;
        }
        finally {
            try {
                provider.getComputeServices().getVirtualMachineSupport().start(vmId);
            }
            catch( Throwable ignore ) {
                logger.warn("Failed to restart " + options.getVirtualMachineId() + " after drive cloning");
            }
        }
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        return toMachineImage(getDrive(providerImageId));
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        switch( cls ) {
            case KERNEL: return "kernel image";
            case RAMDISK: return "ramdisk image";
        }
        return "boot drive";
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        MachineImage img = getImage(machineImageId);

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
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        if( !cls.equals(ImageClass.MACHINE) ) {
            return Collections.emptyList();
        }
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String me = ctx.getAccountNumber();

        ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
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
                ResourceStatus img = toStatus(object);

                if( img != null ) {
                    list.add(img);
                }
            }
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String me = ctx.getAccountNumber();
        String account;

        if( options == null ) {
            account = me;
        }
        else {
            account = options.getAccountNumber();
            if( account == null ) {
                account = me;
            }
        }

        ImageClass cls = (options == null ? null : options.getImageClass());

        if( cls != null && !cls.equals(ImageClass.MACHINE) ) {
            return Collections.emptyList();
        }

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
            if( account.equals(id) ) {
                MachineImage img = toMachineImage(object);

                if( img != null ) {
                    list.add(img);
                }
            }
        }
        return list;
    }

    private @Nonnull Iterable<MachineImage> listImagesComplete(@Nullable String accountId) throws CloudException, InternalException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new NoContextException();
        }
        String me = ctx.getAccountNumber();

        if( me.equals(accountId) ) {
            return listImages(ImageClass.MACHINE);
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

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.emptyList();
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
    public @Nonnull Iterable<ImageClass> listSupportedImageClasses() throws CloudException, InternalException {
        return Collections.singletonList(ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImageType> listSupportedImageTypes() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageType.VOLUME);
    }

    @Override
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("No image registering is currently supported");
    }

    @Override
    public void remove(@Nonnull String machineImageId) throws CloudException, InternalException {
        remove(machineImageId, false);
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        if( method.postString(toDriveURL(providerImageId, "destroy"), "") == null ) {
            throw new CloudException("Unable to identify drives endpoint for removal");
        }
    }

    @Override
    @Deprecated
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ArrayList<MachineImage> list = new ArrayList<MachineImage>();

        for( MachineImage img : listImages(ImageClass.MACHINE) ) {
            if( img != null && matches(img, keyword, platform, architecture) ) {
                list.add(img);
            }
        }
        for( MachineImage img : listImagesComplete(null) ) {
            if( img != null && matches(img, keyword, platform, architecture) ) {
                list.add(img);
            }
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchImages(@Nullable String accountNumber, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        ArrayList<MachineImage> list = new ArrayList<MachineImage>();

        if( accountNumber == null ) {
            for( MachineImage img : listImages(ImageClass.MACHINE) ) {
                if( img != null && matches(img, keyword, platform, architecture) ) {
                    list.add(img);
                }
            }
            for( MachineImage img : listImagesComplete(null) ) {
                if( img != null && matches(img, keyword, platform, architecture) ) {
                    list.add(img);
                }
            }
        }
        else {
            for( MachineImage img : listImages(ImageClass.MACHINE, accountNumber) ) {
                if( img != null && matches(img, keyword, platform, architecture) ) {
                    list.add(img);
                }
            }
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass... imageClasses) throws CloudException, InternalException {
        ArrayList<MachineImage> list = new ArrayList<MachineImage>();

        for( MachineImage img : listImagesComplete(null) ) {
            if( img != null && matches(img, keyword, platform, architecture) ) {
                list.add(img);
            }
        }
        return list;
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return type.equals(MachineImageType.VOLUME);
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return cls.equals(ImageClass.MACHINE);
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

        image.setProviderRegionId(regionId);
        image.setCurrentState(MachineImageState.PENDING);
        image.setType(MachineImageType.VOLUME);
        image.setImageClass(ImageClass.MACHINE);
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
        Platform platform;

        if( os != null && !os.equals("") ) {
            platform = Platform.guess(os);
        }
        else {
            return null;  // not a machine image
        }
        if( platform.equals(Platform.UNKNOWN) ) {
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

    private @Nullable ResourceStatus toStatus(@Nullable Map<String,String> drive) throws CloudException, InternalException {
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
        String id = drive.get("drive");

        if( id == null || id.equals("") ) {
            return null;
        }
        MachineImageState state = MachineImageState.PENDING;
        String s = drive.get("status");

        if( s != null ) {
            if( s.equalsIgnoreCase("active") ) {
                state = MachineImageState.ACTIVE;
            }
            else if( s.equalsIgnoreCase("inactive") ) {
                state = MachineImageState.DELETED;
            }
            else if( s.startsWith("copying") ) {
                state = MachineImageState.PENDING;
            }
            else {
                logger.warn("DEBUG: Unknown drive state for CloudSigma: " + s);
            }
        }
        return new ResourceStatus(id, state);
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
