package org.dasein.cloud.cloudsigma.compute.block;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VolumeCapabilities;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.util.NamingConstraints;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of Cloudsigma with respect to Dasein volume operations
 * User: daniellemayne
 * Date: 05/03/2014
 * Time: 15:13
 */
public class DataDriveCapabilities extends AbstractCapabilities<CloudSigma> implements VolumeCapabilities{
    public DataDriveCapabilities(@Nonnull CloudSigma provider) {
        super(provider);
    }

    @Override
    public boolean canAttach(VmState vmState) throws InternalException, CloudException {
        return vmState.equals(VmState.STOPPED);
    }

    @Override
    public boolean canDetach(VmState vmState) throws InternalException, CloudException {
        return vmState.equals(VmState.STOPPED);
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Override
    public int getMaximumVolumeProductIOPS() throws InternalException, CloudException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMinimumVolumeProductIOPS() throws InternalException, CloudException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMaximumVolumeSizeIOPS() throws InternalException, CloudException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getMinimumVolumeSizeIOPS() throws InternalException, CloudException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nullable
    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(100, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Nonnull
    @Override
    public NamingConstraints getVolumeNamingConstraints() throws CloudException, InternalException {
        return NamingConstraints.getAlphaOnly(0, 0);
    }

    @Nonnull
    @Override
    public String getProviderTermForVolume(@Nonnull Locale locale) {
        return "drive";
    }

    @Nonnull
    @Override
    public Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return Requirement.OPTIONAL;
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    static private Collection<String> deviceIds;
    @Nonnull
    @Override
    public Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        if (deviceIds == null) {
            ArrayList<String> ids = new ArrayList<String>();

            for (int i = 0; i <= 9; i++) {
                for (int j = 0; j <= 3; j++) {
                    if (i == 0 && j == 0){} //0:0 is always the boot drive so unavailable for attaching volumes
                    else {
                        ids.add(String.valueOf(i).concat(":").concat(String.valueOf(j)));
                    }
                }
            }
            deviceIds = Collections.unmodifiableList(ids);
        }
        return deviceIds;
    }

    @Nonnull
    @Override
    public Iterable<VolumeFormat> listSupportedFormats() throws InternalException, CloudException {
        return Collections.singletonList(VolumeFormat.BLOCK);
    }

    @Nonnull
    @Override
    public Requirement requiresVMOnCreate() throws InternalException, CloudException {
        return Requirement.NONE;
    }
}
