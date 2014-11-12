package org.dasein.cloud.cloudsigma.network.ip;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.network.IPAddressCapabilities;
import org.dasein.cloud.network.IPVersion;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of Cloudsigma with respect to Dasein static ip operations.
 * <p>Created by Danielle Mayne: 5/03/14 08:45 AM</p>
 * @author Danielle Mayne
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class StaticIPCapabilities extends AbstractCapabilities<CloudSigma> implements IPAddressCapabilities{
    public StaticIPCapabilities(@Nonnull CloudSigma provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public String getProviderTermForIpAddress(@Nonnull Locale locale) {
        return "static ip";
    }

    @Nonnull
    @Override
    public Requirement identifyVlanForVlanIPRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Nonnull
    @Override
    public Requirement identifyVlanForIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean isAssigned(@Nonnull IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean canBeAssigned(@Nonnull VmState vmState) throws CloudException, InternalException {
        return vmState.equals(VmState.RUNNING);
    }

    @Override
    public boolean isAssignablePostLaunch(@Nonnull IPVersion version) throws CloudException, InternalException {
        return version.equals(IPVersion.IPV4);
    }

    @Override
    public boolean isForwarding(IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isRequestable(@Nonnull IPVersion version) throws CloudException, InternalException {
        return false;
    }

    @Nonnull
    @Override
    public Iterable<IPVersion> listSupportedIPVersions() throws CloudException, InternalException {
        return Collections.singletonList(IPVersion.IPV4);
    }

    @Override
    public boolean supportsVLANAddresses(@Nonnull IPVersion ofVersion) throws InternalException, CloudException {
        return false;
    }
}
