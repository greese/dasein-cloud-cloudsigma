package org.dasein.cloud.cloudsigma.network.firewall;

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
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.network.Direction;
import org.dasein.cloud.network.Firewall;
import org.dasein.cloud.network.FirewallCreateOptions;
import org.dasein.cloud.network.FirewallRule;
import org.dasein.cloud.network.AbstractFirewallSupport;
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
 * Support for firewall policies in CloudSigma.
 * <p>Created by Danielle Mayne: 06/04/13 14:00 PM</p>
 * @author Danielle Mayne
 * @version 2013.04
 * @since 2013.02
 */
public class ServerFirewallSupport extends AbstractFirewallSupport {
    static private final Logger logger = CloudSigma.getLogger(ServerFirewallSupport.class);

    private CloudSigma provider;

    ServerFirewallSupport(@Nonnull CloudSigma provider) {
        super(provider);
        this.provider = provider;
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

            if (firewallObj != null) {
                FirewallRule newRule =  FirewallRule.getInstance(null, firewallId, sourceEndpoint, direction, protocol, permission, destinationEndpoint, beginPort, endPort);

                return newRule.getProviderRuleId();
            }
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }

        throw new CloudException("Firewall rule created but not found in response");
    }

    @Nonnull
    @Override
    public String create(@Nonnull FirewallCreateOptions options) throws InternalException, CloudException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);

        try {
            JSONObject body = new JSONObject(), fwName = new JSONObject();
            JSONArray objects = new JSONArray();
            fwName.put("name", options.getName());
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

    @Override
    public void delete(@Nonnull String s) throws InternalException, CloudException {
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
                        FirewallRule rule = toFirewallRule(matches.getJSONObject(i), firewallId);
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

    @Nonnull
    @Override
    public String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void removeTags(@Nonnull String volumeId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void removeTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void revoke(@Nonnull String providerFirewallRuleId) throws InternalException, CloudException {
        FirewallRule rule = null;

        for( Firewall f : list() ) {
            String fwId = f.getProviderFirewallId();

            if( fwId != null ) {
                for( FirewallRule r : getRules(fwId) ) {
                    if( providerFirewallRuleId.equals(r.getProviderRuleId()) ) {
                        rule = r;
                        break;
                    }
                }
            }
        }
        if( rule == null ) {
            throw new CloudException("Unable to parse rule ID: " + providerFirewallRuleId);
        }
        revoke(providerFirewallRuleId, rule.getFirewallId());
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull String source, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, direction, Permission.ALLOW, source, protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String source, @Nonnull Protocol protocol, int beginPort, int endPort) throws CloudException, InternalException {
        revoke(firewallId, direction, permission, source, protocol, RuleTarget.getGlobal(firewallId), beginPort, endPort);
    }

    @Override
    public void revoke(@Nonnull String firewallId, @Nonnull Direction direction, @Nonnull Permission permission, @Nonnull String source, @Nonnull Protocol protocol, @Nonnull RuleTarget target, int beginPort, int endPort) throws CloudException, InternalException {
        String tmpRuleId = FirewallRule.getRuleId(firewallId, RuleTarget.getCIDR(source), direction, protocol, permission, target, beginPort, endPort);

        revoke(tmpRuleId, firewallId);
    }

    private void revoke(@Nonnull String ruleId, @Nonnull String firewallId) throws CloudException, InternalException {
        CloudSigmaMethod method = new CloudSigmaMethod(provider);
        JSONArray newArray = new JSONArray();

        try{
            JSONObject fw = new JSONObject(method.getString(toFirewallURL(firewallId, "")));
            JSONArray rules = fw.getJSONArray("rules");
            for (int i = 0; i<rules.length(); i++) {
                JSONObject rule = rules.getJSONObject(i);
                FirewallRule r = toFirewallRule(rule, firewallId);
                if (!r.getProviderRuleId().equalsIgnoreCase(ruleId)) {
                    newArray.put(rule);
                }
            }
            fw.put("rules", newArray);
            String jsonBody = fw.toString();

            if (method.putString(toFirewallURL(firewallId, ""), jsonBody) == null) {
                throw new CloudException("Unable to locate firewall endpoint in CloudSigma");
            }
        }
        catch (JSONException e) {
            throw new InternalException(e);
        }
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
        return true;
    }

    @Override
    public boolean supportsFirewallSources() throws CloudException, InternalException {
        return false;
    }

    @Override
    public void updateTags(@Nonnull String volumeId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void updateTags(@Nonnull String[] vmIds, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of overridden methods use File | Settings | File Templates.
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
            if (fwRule.has("src_ip") && !fwRule.isNull("src_ip")) {
                String sourceIP = fwRule.getString("src_ip");
                sourceEndpoint = RuleTarget.getCIDR(sourceIP);
            }
            if (fwRule.has("direction")) {
                String dir = fwRule.getString("direction");
                direction = (dir.equalsIgnoreCase("in") ? Direction.INGRESS : Direction.EGRESS);
            }
            if (fwRule.has("ip_proto") && !fwRule.isNull("ip_proto")) {
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
                if (action.equalsIgnoreCase("accept")) {
                    permission = Permission.ALLOW;
                }
                else if (action.equalsIgnoreCase("drop")) {
                    permission = Permission.DENY;
                }
            }
            if (fwRule.has("dst_ip") && !fwRule.isNull("dst_ip")) {
                String destIP = fwRule.getString("dst_ip");
                destEndpoint = RuleTarget.getCIDR(destIP);
            }

            if (fwRule.has("dst_port") && !fwRule.isNull("dst_port")) {
                String destPort = fwRule.getString("dst_port");
                if (destPort.indexOf(":") > -1) {
                    startPort = Integer.parseInt(destPort.substring(0, destPort.indexOf(":")));
                    endPort = Integer.parseInt(destPort.substring(destPort.indexOf(":")+1, destPort.length()));
                }
                else {
                    startPort = Integer.parseInt(destPort);
                    endPort = startPort;
                }
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
        FirewallRule newRule = FirewallRule.getInstance(null, providerFirewallId, sourceEndpoint, direction, protocol, permission, destEndpoint, startPort, endPort);

        return newRule;
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
