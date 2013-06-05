package org.dasein.cloud.cloudsigma.network.firewall;

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
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallRule;
import org.dasein.cloud.network.FirewallSupport;
import org.dasein.cloud.network.Permission;
import org.dasein.cloud.network.Protocol;
import org.dasein.cloud.network.RuleTarget;
import org.dasein.cloud.network.RuleTargetType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;

/**
 * Support for Firewall Support in CloudSigma.
 * <p>Created by Danielle Mayne: 05/30/13 11:14 AM</p>
 * @author Danielle Mayne
 * @version 2013.02 initial version
 * @since 2013.02
 */
public class ServerFirewallSupport implements FirewallSupport {
    static private final Logger logger = CloudSigma.getLogger(ServerFirewallSupport.class);

    private CloudSigma provider;

    public ServerFirewallSupport(@Nonnull CloudSigma provider) {
        this.provider = provider;
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull String cidr, @Nonnull Protocol protocol, int startPort, int endPort) throws CloudException, InternalException {
        return authorize(firewallId, Direction.INGRESS, Permission.ALLOW, RuleTarget.getCIDR(cidr), protocol, RuleTarget.getGlobal(firewallId), startPort, endPort, 0);
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        if( direction.equals(Direction.INGRESS) ) {
            return authorize(firewallId, direction, Permission.ALLOW, RuleTarget.getCIDR(cidr), protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort, 0);
        }
        else {
            return authorize(firewallId, direction, Permission.ALLOW, RuleTarget.getGlobal(firewallId), protocol, RuleTarget.getCIDR(cidr), beginPort, endPort, 0);
        }
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        if( direction.equals(Direction.INGRESS) ) {
            return authorize(firewallId, direction, permission, RuleTarget.getCIDR(cidr), protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort, 0);
        }
        else {
            return authorize(firewallId, direction, permission, RuleTarget.getGlobal(firewallId), protocol, RuleTarget.getCIDR(cidr), beginPort, endPort, 0);
        }
    }

    @Override
    public @Nonnull String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String cidr, @Nonnull Protocol protocol, @Nonnull RuleTarget destination, int beginPort, int endPort) throws CloudException, InternalException {
        if( direction.equals(Direction.INGRESS) ) {
            return authorize(firewallId, direction, permission, RuleTarget.getCIDR(cidr), protocol, destination, beginPort, endPort, 0);
        }
        else {
            return authorize(firewallId, direction, permission, destination, protocol, RuleTarget.getCIDR(cidr), beginPort, endPort, 0);
        }
    }

    @Nonnull
    @Override
    public String authorize(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull RuleTarget sourceEndpoint, @Nonnull Protocol protocol, @Nonnull RuleTarget destinationEndpoint, int beginPort, int endPort, @Nonnegative int precedence) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        try{
            JSONObject fw = new JSONObject(method.getString(toFirewallURL(firewallId, "")));
            JSONArray rules = fw.getJSONArray("rules");

            JSONObject rule = new JSONObject();
            rule.put("action", (permission == Permission.ALLOW ? "accept" : "drop"));
            rule.put("direction", (direction == Direction.INGRESS ? "in" : "out"));
            rule.put("dst_ip", destinationEndpoint.getCidr());
            rule.put("dst_port", String.valueOf(beginPort)+((endPort >= 0 && endPort!=beginPort) ? ":"+String.valueOf(endPort) : ""));
            rule.put("ip_proto", (protocol == Protocol.TCP ? "tcp" : "udp"));
            rule.put("src_ip", sourceEndpoint.getCidr());
            rules.put(rule);

            String firewallObj = method.putString(toFirewallURL(firewallId, ""), fw.toString());

            //todo need to get unique identifier of rule just created
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }

        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Nonnull
    @Override
    public String create(@Nonnull String name, @Nonnull String description) throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        try {
            JSONObject body = new JSONObject(), fwName = new JSONObject();
            JSONArray objects = new JSONArray();
            fwName.put("name", name);
            objects.put(fwName);
            body.put("objects", objects);

            JSONObject fwObj = new JSONObject(method.postString("/fwpolicies/", body.toString()));
            Firewall firewall = null;
            if (fwObj != null) {
                JSONArray arr = fwObj.getJSONArray("objects");
                JSONObject fw = arr.getJSONObject(0);
                firewall = toFirewall(fw);
            }
            if (firewall == null) {
                throw new CloudException("Firewall created but no information was provided");
            }
            return firewall.getProviderFirewallId();
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
    }

    @Nonnull
    @Override
    public String createInVLAN(@Nonnull String name, @Nonnull String description, @Nonnull String providerVlanId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Firewall policies are only applied when attached to public network interfaces");
    }

    @Override
    public void delete(@Nonnull String firewallId) throws InternalException, CloudException {
        throw new OperationNotSupportedException("Deleting firewalls is not supported in CloudSigma api");
    }

    @Nullable
    @Override
    public Firewall getFirewall(@Nonnull String firewallId) throws InternalException, CloudException {
        if (firewallId.length() > 0) {
            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            try {
                String fwObj = method.getString(toFirewallURL(firewallId, ""));
                if (fwObj != null) {
                    return toFirewall(new JSONObject(fwObj));
                }
                return null;
            }
            catch (JSONException e) {
                throw new InternalException(e);
            }
        }
        else {
            throw new InternalException("Firewall id is null/empty!");
        }
    }

    @Nonnull
    @Override
    public String getProviderTermForFirewall(@Nonnull Locale locale) {
        return "firewall policy";
    }

    @Nonnull
    @Override
    public Collection<FirewallRule> getRules(@Nonnull String firewallId) throws InternalException, CloudException {
        ArrayList<FirewallRule> list = new ArrayList<FirewallRule>();
        if (firewallId.length() > 0) {
            CloudSigmaMethod method = new CloudSigmaMethod(provider);

            try {
                String fwObj = method.getString(toFirewallURL(firewallId, ""));
                if (fwObj != null) {
                    JSONObject firewall = new JSONObject(fwObj);
                    JSONArray matches = firewall.getJSONArray("rules");
                    for (int i= 0; i<matches.length(); i++) {
                        FirewallRule rule = toFirewallRule(new JSONObject(matches.get(i)), firewallId);
                        if (rule != null) {
                            list.add(rule);
                        }
                    }
                }
                return list;
            }
            catch (JSONException e) {
                throw new InternalException(e);
            }
        }
        else {
            throw new InternalException("Firewall id is null/empty!");
        }
    }

    @Nonnull
    @Override
    public Requirement identifyPrecedenceRequirement(boolean inVlan) throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return true;
    }

    @Override
    public boolean isZeroPrecedenceHighest() throws InternalException, CloudException {
        return true;
    }

    @Nonnull
    @Override
    public Collection<Firewall> list() throws InternalException, CloudException {
        ArrayList<Firewall> list = new ArrayList<Firewall>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        boolean moreData = true;
        String baseTarget = "/fwpolicies/detail/";
        String target = "";

        while(moreData)  {
            target = baseTarget+target;

            try {
                JSONObject json = method.list(target);

                if (json == null) {
                    throw new CloudException("No firewall endpoint was found");
                }
                JSONArray objects = json.getJSONArray("objects");
                for (int i = 0; i < objects.length(); i++) {
                    JSONObject jObj = objects.getJSONObject(i);

                    Firewall fw = toFirewall(jObj);

                    if (fw != null) {
                        list.add(fw);
                    }
                }

                //dmayne 20130314: check if there are more pages
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
                }
            }
            catch (JSONException e) {
                throw new InternalException(e);
            }
        }
        return list;
    }

    @Nonnull
    @Override
    public Iterable<ResourceStatus> listFirewallStatus() throws InternalException, CloudException {
        ArrayList<ResourceStatus> list = new ArrayList<ResourceStatus>();
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        boolean moreData = true;
        String baseTarget = "/fwpolicies/";
        String target = "";

        while(moreData)  {
            target = baseTarget+target;

            try {
                JSONObject json = method.list(target);

                if (json == null) {
                    throw new CloudException("No firewall endpoint was found");
                }
                JSONArray objects = json.getJSONArray("objects");
                for (int i = 0; i < objects.length(); i++) {
                    JSONObject jObj = objects.getJSONObject(i);

                    ResourceStatus fw = toFirewallStatus(jObj);

                    if (fw != null) {
                        list.add(fw);
                    }
                }

                //dmayne 20130314: check if there are more pages
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
                }
            }
            catch (JSONException e) {
                throw new InternalException(e);
            }
        }
        return list;
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedDestinationTypes(boolean inVlan) throws InternalException, CloudException {
        if (!inVlan) {
            Collection<RuleTargetType> destTypes = new ArrayList<RuleTargetType>();
            destTypes.add(RuleTargetType.CIDR);
            destTypes.add(RuleTargetType.GLOBAL);
            return destTypes;
        }
        throw new OperationNotSupportedException("Firewall policies for vlans not supported");
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
        throw new OperationNotSupportedException("Firewall policies for vlans not supported");
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
        throw new OperationNotSupportedException("Firewall policies for vlans not supported");
    }

    @Nonnull
    @Override
    public Iterable<RuleTargetType> listSupportedSourceTypes(boolean inVlan) throws InternalException, CloudException {
        if (!inVlan) {
            Collection<RuleTargetType> sourceTypes = new ArrayList<RuleTargetType>();
            sourceTypes.add(RuleTargetType.CIDR);
            sourceTypes.add(RuleTargetType.GLOBAL);
            return sourceTypes;
        }
        throw new OperationNotSupportedException("Firewall policies for vlans not supported");
    }

    @Override
    public void revoke(@Nonnull String providerFirewallRuleId) throws InternalException, CloudException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull String cidr, @Nonnull Protocol protocol, int startPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, Direction.INGRESS, Permission.ALLOW, cidr, protocol, RuleTarget.getGlobal(firewallId), startPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, direction, Permission.ALLOW, cidr, protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String cidr, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, direction, permission, cidr, protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String cidr, @Nonnull Protocol protocol, @Nonnull RuleTarget destination, int beginPort, int endPort) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean supportsRules(@Nonnull Direction direction, @Nonnull Permission permission, boolean inVlan) throws CloudException, InternalException {
        if (!inVlan) {
            return true;
        }
        return false;
    }

    @Override
    public boolean supportsFirewallSources() throws CloudException, InternalException {
        return false;
    }

    @Nonnull
    @Override
    public String[] mapServiceAction(@Nonnull ServiceAction serviceAction) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    private Firewall toFirewall(JSONObject fw) throws CloudException, InternalException{
        if (fw == null) {
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

        Firewall firewall = new Firewall();
        try {
            String fwId = fw.getString("uuid");

            firewall.setProviderFirewallId(fwId);

            if (fw.has("name") && !fw.isNull("name")) {
                String name = fw.getString("name");
                if (name != null) {
                    firewall.setName(name);
                    firewall.setDescription(name);
                }
            }

            firewall.setActive(true);
            firewall.setAvailable(true);
            firewall.setRegionId(regionId);
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
        return firewall;
    }

    private ResourceStatus toFirewallStatus(JSONObject fw) throws CloudException, InternalException{
        if (fw == null) {
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

        String fwId;
        try {
            fwId = fw.getString("uuid");
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
        return new ResourceStatus(fwId, true);
    }

    private FirewallRule toFirewallRule(JSONObject fwRule, String fwID) throws CloudException, InternalException{
        if (fwRule == null) {
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

        String providerFirewallId = fwID;
        RuleTarget sourceEndpoint = null;
        Direction direction = null;
        Protocol protocol = null;
        Permission permission = null;
        RuleTarget destEndpoint = null;
        int startPort = -1;
        int endPort = -1;

        try {
            if (fwRule.has("src_ip")) {
                String sourceIP = fwRule.getString("src_ip");
                sourceEndpoint = RuleTarget.getCIDR(sourceIP);
            }
            if (fwRule.has("direction")) {
                String dir = fwRule.getString("direction");
                direction = (dir.equalsIgnoreCase("in") ? Direction.INGRESS : Direction.EGRESS);
            }
            if (fwRule.has("ip_proto")) {
                String proto = fwRule.getString("ip_proto");
                if (proto.equalsIgnoreCase("tcp")) {
                    protocol = Protocol.TCP;
                }
                else if (proto.equalsIgnoreCase("udp")) {
                    protocol = Protocol.UDP;
                }
            }
            if (fwRule.has("action")) {
                String action = fwRule.getString("action");
                if (action.equalsIgnoreCase("allow")) {
                    permission = Permission.ALLOW;
                }
                else if (action.equalsIgnoreCase("drop")) {
                    permission = Permission.DENY;
                }
            }
            if (fwRule.has("dst_ip")) {
                String destIP = fwRule.getString("dst_ip");
                destEndpoint = RuleTarget.getCIDR(destIP);
            }

            if(sourceEndpoint == null) {
                sourceEndpoint = RuleTarget.getGlobal(providerFirewallId);
            }
            if(destEndpoint == null) {
                destEndpoint = RuleTarget.getGlobal(providerFirewallId);
            }
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
        return FirewallRule.getInstance(null, providerFirewallId, sourceEndpoint, direction, protocol, permission, destEndpoint, startPort, endPort);
    }

    private @Nonnull String toFirewallURL(@Nonnull String firewallId, @Nonnull String action) throws InternalException {
        try {
            return ("/fwpolicies/" + URLEncoder.encode(firewallId, "utf-8") + "/" + action);
        } catch (UnsupportedEncodingException e) {
            logger.error("UTF-8 not supported: " + e.getMessage());
            throw new InternalException(e);
        }
    }
}
