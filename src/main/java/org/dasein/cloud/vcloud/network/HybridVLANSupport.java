/**
 * Copyright (C) 2009-2014 Dell, Inc
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

package org.dasein.cloud.vcloud.network;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.Tag;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.*;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vcloud.vCloud;
import org.dasein.cloud.vcloud.vCloudMethod;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;

/**
 * Implements support for vCloud networking.
 * <p>Created by George Reese: 9/17/12 10:59 AM</p>
 * @author George Reese
 * @version 2013.04 initial version
 * @since 2013.04
 */
public class HybridVLANSupport extends AbstractVLANSupport {

    private volatile transient HybridVLANCapabilities capabilities;
    private vCloud provider;

    HybridVLANSupport(@Nonnull vCloud provider) {
        super(provider);
        this.provider = provider;
    }

    @Override
    public @Nonnull VLAN createVlan(@Nonnull String cidr, @Nonnull String name, @Nonnull String description, @Nonnull String domainName, @Nonnull String[] dnsServers, @Nonnull String[] ntpServers) throws CloudException, InternalException {
        // TODO: implement me
        return super.createVlan(cidr, name, description, domainName, dnsServers, ntpServers);
    }

    @Override
    public VLANCapabilities getCapabilities() throws CloudException, InternalException {
        if( capabilities == null ) {
            capabilities = new HybridVLANCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nonnull String getProviderTermForNetworkInterface(@Nonnull Locale locale) {
        return "network interface";
    }

    @Override
    public @Nonnull String getProviderTermForSubnet(@Nonnull Locale locale) {
        return "subnet";
    }

    @Override
    public @Nonnull String getProviderTermForVlan(@Nonnull Locale locale) {
        return "network";
    }

    @Override
    public VLAN getVlan(@Nonnull String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.getVlan");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());

            for( DataCenter dc : method.listDataCenters() ) {
                VLAN vlan = toVlan(dc.getProviderDataCenterId(), vlanId);

                if( vlan != null ) {
                    return vlan;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nullable String getAttachedInternetGatewayId(@Nonnull String vlanId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nullable InternetGateway getInternetGatewayById(@Nonnull String gatewayId) throws CloudException, InternalException {
        return null;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.isSubscribed");
        try {
            return (getProvider().testContext() != null);
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public Collection<InternetGateway> listInternetGateways(@Nullable String s) throws CloudException, InternalException {
        return null;
    }

    @Override
    public @Nonnull Iterable<VLAN> listVlans() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.listVlans");
        try {
            Cache<VLAN> cache = Cache.getInstance(getProvider(), "networks", VLAN.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(5, TimePeriod.MINUTE));
            Iterable<VLAN> cached = cache.get(getContext());

            if( cached != null ) {
                return cached;
            }
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            ArrayList<VLAN> vlans = new ArrayList<VLAN>();

            for( DataCenter dc : method.listDataCenters() ) {
                String xml = method.get("vdc", dc.getProviderDataCenterId());

                if( xml != null && !xml.equals("") ) {
                    Document doc = method.parseXML(xml);
                    String docElementTagName = doc.getDocumentElement().getTagName();
                    String nsString = "";
                    if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                    NodeList vdcs = doc.getElementsByTagName(nsString + "Vdc");

                    if( vdcs.getLength() > 0 ) {
                        NodeList attributes = vdcs.item(0).getChildNodes();

                        for( int i=0; i<attributes.getLength(); i++ ) {
                            Node attribute = attributes.item(i);
                            if(attribute.getNodeName().contains(":"))nsString = attribute.getNodeName().substring(0, attribute.getNodeName().indexOf(":") + 1);
                            else nsString = "";

                            if( attribute.getNodeName().equalsIgnoreCase(nsString + "AvailableNetworks") && attribute.hasChildNodes() ) {
                                NodeList resources = attribute.getChildNodes();

                                for( int j=0; j<resources.getLength(); j++ ) {
                                    Node resource = resources.item(j);

                                    if( resource.getNodeName().equalsIgnoreCase(nsString + "Network") && resource.hasAttributes() ) {
                                        Node href = resource.getAttributes().getNamedItem("href");

                                        VLAN vlan = toVlan(dc.getProviderDataCenterId(), ((vCloud) getProvider()).toID(href.getNodeValue().trim()));

                                        if( vlan != null ) {
                                            vlans.add(vlan);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            cache.put(getContext(), vlans);
            return vlans;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void removeInternetGatewayById(@Nonnull String s) throws CloudException, InternalException {
    }

    @Override
    public void removeInternetGatewayTags(@Nonnull String internetGatewayId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void removeRoutingTableTags(@Nonnull String routingTableId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    private @Nullable VLAN toVlan(@Nonnull String vdcId, @Nonnull String id) throws InternalException, CloudException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

        String xml = method.get("network", id);

        if( xml == null || xml.equals("") ) {
            return null;
        }
        Document doc = method.parseXML(xml);
        String docElementTagName = doc.getDocumentElement().getTagName();
        String nsString = "";
        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
        NodeList nets = doc.getElementsByTagName(nsString + "OrgVdcNetwork");

        if( nets.getLength() < 1 ) {
            nets = doc.getElementsByTagName(nsString + "OrgNetwork");
            if( nets.getLength() < 1 ) {
                nets = doc.getElementsByTagName(nsString + "Network");
                if( nets.getLength() < 1 ) {
                    return null;
                }
            }
        }
        Node netNode = nets.item(0);
        if(netNode.getNodeName().contains(":"))nsString = netNode.getNodeName().substring(0, netNode.getNodeName().indexOf(":") + 1);
        NodeList attributes = netNode.getChildNodes();
        VLAN vlan = new VLAN();

        vlan.setProviderVlanId(id);
        vlan.setProviderDataCenterId(vdcId);
        vlan.setProviderRegionId(getContext().getRegionId());
        vlan.setProviderOwnerId(getContext().getAccountNumber());
        vlan.setSupportedTraffic(IPVersion.IPV4);
        vlan.setCurrentState(VLANState.AVAILABLE);

        Node n;

        /*
        n = netNode.getAttributes().getNamedItem("status");
        if( n == null ) {
            vlan.setCurrentState(VLANState.AVAILABLE);
        }
        else {
            vlan.setCurrentState(toState(n.getNodeValue().trim()));
        }
        */
        n = netNode.getAttributes().getNamedItem(nsString + "name");
        if( n != null ) {
            vlan.setName(n.getNodeValue().trim());
            vlan.setDescription(n.getNodeValue().trim());
        }
        HashMap<String,String> tags = new HashMap<String, String>();
        n = netNode.getAttributes().getNamedItem(nsString + "href");
        if (n != null) {
           tags.put("networkHref", n.getNodeValue().trim());
        }


        String gateway = null;
        String netmask = null;
        boolean shared = false;
        String fenceMode = null;

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            if(attribute.getNodeName().contains(":"))nsString = attribute.getNodeName().substring(0, attribute.getNodeName().indexOf(":") + 1);
            else nsString = "";

            if( attribute.getNodeName().equals(nsString + "Description") && attribute.hasChildNodes() ) {
                shared = attribute.getFirstChild().getNodeValue().trim().equalsIgnoreCase("true");
            }
            else if( attribute.getNodeName().equals(nsString + "IsShared") && attribute.hasChildNodes() ) {
                vlan.setDescription(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attribute.getNodeName().equals(nsString + "Features") && attribute.hasChildNodes() ) {
                NodeList list = attribute.getChildNodes();

                for( int j=0; j<list.getLength(); j++ ) {
                    Node feature = list.item(j);

                    if( feature.getNodeName().equalsIgnoreCase(nsString + "FenceMode") && feature.hasChildNodes() ) {
                        fenceMode = feature.getFirstChild().getNodeValue().trim();
                    }
                }
            }
            else if( attribute.getNodeName().equals(nsString + "Configuration") && attribute.hasChildNodes() ) {
                NodeList scopesList = attribute.getChildNodes();
                String[] dns = new String[10];
                String ipStart = null;
                String ipEnd = null;
                String domain = null;
                Boolean enabled = null;

                for( int j=0; j<scopesList.getLength(); j++ ) {
                    Node scopesNode = scopesList.item(j);
                    if(scopesNode.getNodeName().contains(":"))nsString = scopesNode.getNodeName().substring(0, scopesNode.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( scopesNode.getNodeName().equalsIgnoreCase(nsString + "FenceMode") && scopesNode.hasChildNodes() ) {
                        fenceMode = scopesNode.getFirstChild().getNodeValue().trim();
                    }
                    else if( (scopesNode.getNodeName().equalsIgnoreCase(nsString + "IpScope") || scopesNode.getNodeName().equalsIgnoreCase(nsString + "IpScopes")) && scopesNode.hasChildNodes() ) {
                        Node scope = null;

                        if( scopesNode.getNodeName().equalsIgnoreCase(nsString + "IpScope") ) {
                            scope = scopesNode;
                        }
                        else {
                            NodeList scopes = scopesNode.getChildNodes();

                            for( int k=0; k<scopes.getLength(); k++ ) {
                                Node node = scopes.item(k);
                                if(node.getNodeName().contains(":"))nsString = node.getNodeName().substring(0, node.getNodeName().indexOf(":") + 1);
                                else nsString = "";

                                if( node.getNodeName().equalsIgnoreCase(nsString + "IpScope") ) {
                                    scope = node;
                                    break;
                                }
                            }
                        }
                        if( scope != null ) {
                            NodeList saList = scope.getChildNodes();

                            for( int l=0; l<saList.getLength(); l++ ) {
                                Node sa = saList.item(l);
                                if(sa.getNodeName().contains(":"))nsString = sa.getNodeName().substring(0, sa.getNodeName().indexOf(":") + 1);
                                else nsString = "";

                                if( sa.getNodeName().equalsIgnoreCase(nsString + "Gateway") && sa.hasChildNodes() ) {
                                    gateway = sa.getFirstChild().getNodeValue().trim();
                                }
                                else if( sa.getNodeName().equalsIgnoreCase(nsString + "Netmask") && sa.hasChildNodes() ) {
                                    netmask = sa.getFirstChild().getNodeValue().trim();
                                }
                                else if( sa.getNodeName().equalsIgnoreCase(nsString + "DnsSuffix") && sa.hasChildNodes() ) {
                                    domain = sa.getFirstChild().getNodeValue().trim();
                                }
                                else if( sa.getNodeName().startsWith(nsString + "Dns") && sa.hasChildNodes() ) {
                                    String ns = sa.getFirstChild().getNodeValue().trim();

                                    if( sa.getNodeName().equals(ns + "Dns") ) {
                                        dns[0] = ns;
                                    }
                                    else {
                                        try {
                                            int idx = Integer.parseInt(sa.getNodeName().substring(3));

                                            dns[idx] = ns;
                                        }
                                        catch( NumberFormatException e ) {
                                            for(int z=0; i<dns.length; z++ ) {
                                                if( dns[z] == null ) {
                                                    dns[z] = ns;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                                else if( sa.getNodeName().equalsIgnoreCase(nsString + "IsEnabled") && sa.hasChildNodes() ) {
                                    enabled = sa.getFirstChild().getNodeValue().trim().equalsIgnoreCase("true");
                                }
                                else if( sa.getNodeName().equalsIgnoreCase(nsString + "IpRanges") && sa.hasChildNodes() ) {
                                    NodeList rangesList = sa.getChildNodes();

                                    for( int m=0; m<rangesList.getLength(); m++ ) {
                                        Node ranges = rangesList.item(m);
                                        if(ranges.getNodeName().contains(":"))nsString = ranges.getNodeName().substring(0, ranges.getNodeName().indexOf(":") + 1);

                                        if( ranges.getNodeName().equalsIgnoreCase(nsString + "IpRanges") && ranges.hasChildNodes() ) {
                                            NodeList rangeList = ranges.getChildNodes();

                                            for( int o=0; o<rangeList.getLength(); o++ ) {
                                                Node range = rangeList.item(o);
                                                if(range.getNodeName().contains(":"))nsString = range.getNodeName().substring(0, range.getNodeName().indexOf(":") + 1);

                                                if( range.getNodeName().equalsIgnoreCase(nsString + "IpRange") && range.hasChildNodes() ) {
                                                    NodeList addresses = range.getChildNodes();

                                                    for( int p=0; p<addresses.getLength(); p++ ) {
                                                        Node address = addresses.item(p);

                                                        if( address.getNodeName().equalsIgnoreCase(nsString + "StartAddress") && address.hasChildNodes() ) {
                                                            ipStart = address.getFirstChild().getNodeValue().trim();
                                                        }
                                                        else if( address.getNodeName().equalsIgnoreCase(nsString + "EndAddress") && address.hasChildNodes() ) {
                                                            ipEnd = address.getFirstChild().getNodeValue().trim();
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    else if( attribute.getNodeName().equalsIgnoreCase(nsString + "Gateway") && attribute.hasChildNodes() ) {
                        gateway = attribute.getFirstChild().getNodeValue().trim();
                    }
                    else if( attribute.getNodeName().equalsIgnoreCase(nsString + "Netmask") && attribute.hasChildNodes() ) {
                        netmask = attribute.getFirstChild().getNodeValue().trim();
                    }
                }
                ArrayList<String> dnsServers = new ArrayList<String>();

                for( String ns : dns ) {
                    if( ns != null ) {
                        dnsServers.add(ns);
                    }
                }
                vlan.setDnsServers(dnsServers.toArray(new String[dnsServers.size()]));
                vlan.setCurrentState(enabled == null || enabled ? VLANState.AVAILABLE : VLANState.PENDING);
                if( domain != null ) {
                    vlan.setDomainName(domain);
                }
                if( ipStart != null ) {
                    tags.put("ipStart", ipStart);
                }
                if( ipEnd != null ) {
                    tags.put("ipEnd", ipEnd);
                }
            }
        }
        if( fenceMode != null ) {
            // isolated
            // bridged
            // natRouted
            tags.put("fenceMode", fenceMode);
        }
        if( gateway != null ) {
            tags.put("gateway", gateway);
        }
        if( netmask != null ) {
            tags.put("netmask", netmask);
        }
        if( netmask != null && gateway != null ) {
            vlan.setCidr(netmask, gateway);
        }
        tags.put("shared", String.valueOf(shared));
        if( vlan.getName() == null ) {
            vlan.setName(vlan.getProviderVlanId());
        }
        if( vlan.getDescription() == null ) {
            vlan.setDescription(vlan.getName());
        }
        vlan.setTags(tags);
        return vlan;
    }

    @Override
    public void removeVlan(String vlanId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VLAN.removeVlan");
        try {
            // TODO: implement me
            super.removeVlan(vlanId);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void updateRoutingTableTags(@Nonnull String routingTableId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void updateInternetGatewayTags(@Nonnull String internetGatewayId, @Nonnull Tag... tags) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
