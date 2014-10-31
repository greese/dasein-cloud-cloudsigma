package org.dasein.cloud.cloudsigma.network.firewall;

import org.dasein.cloud.*;
import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.network.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Describes the capabilities of Cloudsigma with respect to Dasein firewall operations.
 * <p>Created by Danielle Mayne: 5/03/14 08:30 AM</p>
 * @author Danielle Mayne
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class ServerFirewallCapabilities extends AbstractCapabilities<CloudSigma> implements FirewallCapabilities{
    public ServerFirewallCapabilities(@Nonnull CloudSigma provider) {
        super(provider);
    }

    @Nonnull
    @Override
    public FirewallConstraints getFirewallConstraintsForCloud() throws InternalException, CloudException {
        return FirewallConstraints.getInstance();
    }

    @Nonnull
    @Override
    public String getProviderTermForFirewall(@Nonnull Locale locale) {
        return "firewall policy";
    }

    @Nullable
    @Override
    public VisibleScope getFirewallVisibleScope() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public Requirement identifyPrecedenceRequirement(boolean inVlan) throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isZeroPrecedenceHighest() throws InternalException, CloudException {
        return true;
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan) throws InternalException, CloudException {
        if (!inVlan) {
            Collection<RuleTargetType> destTypes = new ArrayList<RuleTargetType>();
            destTypes.add(RuleTargetType.CIDR);
            return destTypes;
        }
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Iterable<Direction> listSupportedDirections(boolean inVlan) throws InternalException, CloudException {
        if (!inVlan) {
            ArrayList<Direction>  list = new ArrayList<Direction>();

            list.add(Direction.EGRESS);
            list.add(Direction.INGRESS);
            return list;
        }
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Iterable<Permission> listSupportedPermissions(boolean inVlan) throws InternalException, CloudException {
        if (!inVlan) {
            ArrayList<Permission>  list = new ArrayList<Permission>();

            list.add(Permission.ALLOW);
            list.add(Permission.DENY);
            return list;
        }
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Iterable<Protocol> listSupportedProtocols(boolean inVlan) throws InternalException, CloudException {
        if (!inVlan) {
            List<Protocol> list = new ArrayList<Protocol>();
            list.add(Protocol.TCP);
            list.add(Protocol.UDP);
            return list;
        }
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedSourceTypes(boolean inVlan) throws InternalException, CloudException {
        if (!inVlan) {
            Collection<RuleTargetType> sourceTypes = new ArrayList<RuleTargetType>();
            sourceTypes.add(RuleTargetType.CIDR);
            return sourceTypes;
        }
        return Collections.emptyList();
    }

    @Override
    public boolean requiresRulesOnCreation() throws CloudException, InternalException {
        return false;
    }

    @Override
    public Requirement requiresVLAN() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public boolean supportsRules(@Nonnull Direction direction, @Nonnull Permission permission, boolean inVlan) throws CloudException, InternalException {
        if (!inVlan) {
            return true;
        }
        return false;
    }

    @Override
    public boolean supportsFirewallCreation(boolean inVlan) throws CloudException, InternalException {
        if (!inVlan) {
            return true;
        }
        return false;
    }

    @Override
    public boolean supportsFirewallDeletion() throws CloudException, InternalException {
        return false;
    }
}
