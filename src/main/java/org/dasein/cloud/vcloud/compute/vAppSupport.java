/**
 * Copyright (C) 2009-2013 enStratus Networks Inc
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

package org.dasein.cloud.vcloud.compute;

import org.apache.log4j.Logger;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.IPVersion;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vcloud.vCloud;
import org.dasein.cloud.vcloud.vCloudException;
import org.dasein.cloud.vcloud.vCloudMethod;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

/**
 * Implements services for interacting with virtual machines in a vCloud environment. A Dasein Cloud virtual machine
 * maps to a VM running inside a vApp in vCloud.
 * <p>Created by George Reese: 9/17/12 10:58 AM</p>
 * @author George Reese
 * @version 2013.04 initial version
 * @since 2013.04
 */
public class vAppSupport extends DefunctVM {
    static private final Logger logger = vCloud.getLogger(vAppSupport.class);

    static public final String PARENT_VAPP_ID = "parentVAppId";

    vAppSupport(@Nonnull vCloud provider) {
        super(provider);
    }

    public void deploy(@Nonnull String vmId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vmId);

        if( xml != null ) {
            Document doc = method.parseXML(xml);
            String docElementTagName = doc.getDocumentElement().getTagName();
            String nsString = "";
            if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            NodeList nodes = doc.getElementsByTagName(nsString + "VApp");

            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);
                    if(node.getNodeName().contains(":"))nsString = node.getNodeName().substring(0, node.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( node.getNodeName().equalsIgnoreCase(nsString + "Link") && node.hasAttributes() ) {
                        Node rel = node.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("deploy") ) {
                            Node href = node.getAttributes().getNamedItem("href");

                            if( href != null ) {
                                String endpoint = href.getNodeValue().trim();
                                String action = method.getAction(endpoint);
                                StringBuilder payload = new StringBuilder();

                                payload.append("<DeployVAppParams powerOn=\"false\" xmlns=\"http://www.vmware.com/vcloud/v1.5\"/>");
                                method.waitFor(method.post(action, endpoint, method.getMediaTypeForActionDeployVApp(), payload.toString()));
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public int getCostFactor(@Nonnull VmState state) throws InternalException, CloudException {
        if( !state.equals(VmState.RUNNING) ) {
            return 0;
        }
        return 100;
    }

    @Override
    public int getMaximumVirtualMachineCount() throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

        return method.getVMQuota();
    }

    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "getVMProduct");
        try {
            VirtualMachineProduct product = super.getProduct(productId);

            if( product == null && productId.startsWith("custom") ) {
                String[] parts = productId.split(":");

                product = new VirtualMachineProduct();
                product.setProviderProductId(productId);
                if( parts.length == 3 ) {
                    product.setCpuCount(Integer.parseInt(parts[1]));
                    product.setRamSize(new Storage<Megabyte>(Integer.parseInt(parts[2]), Storage.MEGABYTE));
                }
                else {
                    product.setCpuCount(1);
                    product.setRamSize(new Storage<Megabyte>(512, Storage.MEGABYTE));
                }
                product.setName(productId);
                product.setDescription(productId);
            }
            return product;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "VM";
    }

    private @Nullable String getVDC(@Nonnull String vappId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vappId);

        if( xml == null || xml.equals("") ) {
            return null;
        }
        Document doc = method.parseXML(xml);
        String docElementTagName = doc.getDocumentElement().getTagName();
        String nsString = "";
        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
        NodeList nodes = doc.getElementsByTagName(nsString + "VApp");

        if( nodes.getLength() < 1 ) {
            return null;
        }
        Node node = nodes.item(0);
        NodeList elements = node.getChildNodes();

        for( int i=0; i<elements.getLength(); i++ ) {
            Node n = elements.item(i);
            if(n.getNodeName().contains(":"))nsString = n.getNodeName().substring(0, n.getNodeName().indexOf(":") + 1);
            else nsString = "";

            if( n.getNodeName().equalsIgnoreCase(nsString + "Link") && n.hasAttributes() ) {
                Node rel = n.getAttributes().getNamedItem("rel");

                if( rel != null && rel.getNodeValue().trim().equals("up") ) {
                    Node type = n.getAttributes().getNamedItem("type");

                    if( type != null && type.getNodeValue().trim().equals(method.getMediaTypeForVDC()) ) {
                        Node href = n.getAttributes().getNamedItem("href");

                        if( href != null ) {
                            return ((vCloud)getProvider()).toID(href.getNodeValue().trim());
                        }
                    }
                }
            }
        }
        return null;
    }

    @Override
    public VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "getVirtualMachine");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("vApp", vmId);

            if( xml != null && !xml.equals("") ) {
                Document doc = method.parseXML(xml);
                String docElementTagName = doc.getDocumentElement().getTagName();
                String nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList vmNodes = doc.getElementsByTagName(nsString + "Vm");

                if( vmNodes.getLength() < 1 ) {
                    return null;
                }
                Node vmNode = vmNodes.item(0);
                NodeList vmElements = vmNode.getChildNodes();
                String vdc = null, parentVapp = null;

                for( int i=0; i<vmElements.getLength(); i++ ) {
                    Node n = vmElements.item(i);
                    if(n.getNodeName().contains(":"))nsString = n.getNodeName().substring(0, n.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( n.getNodeName().equalsIgnoreCase(nsString + "Link") && n.hasAttributes() ) {
                        Node rel = n.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equals("up") ) {
                            Node type = n.getAttributes().getNamedItem("type");

                            if( type != null && type.getNodeValue().trim().equals(method.getMediaTypeForVApp()) ) {
                                Node href = n.getAttributes().getNamedItem("href");

                                if( href != null ) {
                                    parentVapp = ((vCloud)getProvider()).toID(href.getNodeValue().trim());
                                    vdc = getVDC(parentVapp);
                                }
                            }
                        }
                    }
                }
                if( vdc != null ) {
                    return toVirtualMachine(vdc, parentVapp, vmNode, ((vCloud)getProvider()).getNetworkServices().getVlanSupport().listVlans());
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Requirement identifyImageRequirement(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return (cls.equals(ImageClass.MACHINE) ? Requirement.REQUIRED : Requirement.NONE);
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
    public @Nonnull Requirement identifyShellKeyRequirement(Platform platform) throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyStaticIPRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Requirement identifyVlanRequirement() throws CloudException, InternalException {
        return Requirement.REQUIRED;
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "isSubscribedVM");
        try {
            return (getProvider().testContext() != null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "launchVM");
        try {
            String vdcId = withLaunchOptions.getDataCenterId();

            if( vdcId == null ) {
                for( DataCenter dc : getProvider().getDataCenterServices().listDataCenters(getContext().getRegionId()) ) {
                    if( dc.isActive() && dc.isAvailable() ) {
                        vdcId = dc.getProviderDataCenterId();
                        break;
                    }
                }
            }
            if( vdcId == null ) {
                throw new CloudException("Unable to identify a target data center for deploying VM");
            }
            VirtualMachineProduct product = getProduct(withLaunchOptions.getStandardProductId());
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            MachineImage img = ((vCloud)getProvider()).getComputeServices().getImageSupport().getImage(withLaunchOptions.getMachineImageId());

            if( img == null ) {
                throw new CloudException("No such image: " + withLaunchOptions.getMachineImageId());
            }
            StringBuilder xml = new StringBuilder();

            xml.append("<ComposeVAppParams xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" name=\"").append(withLaunchOptions.getFriendlyName()).append(" Parent vApp\" xmlns=\"http://www.vmware.com/vcloud/v1.5\">");

            String vlanId = withLaunchOptions.getVlanId();
            VLAN vlan = null;

            if( vlanId != null ) {
                vlan = ((vCloud)getProvider()).getNetworkServices().getVlanSupport().getVlan(vlanId);
                if( vlan != null ) {
                    xml.append("<InstantiationParams>");
                    xml.append("<NetworkConfigSection href=\"").append(method.toURL("vAppTemplate", withLaunchOptions.getMachineImageId())).append("/networkConfigSection\" ");
                    xml.append("type=\"application/vnd.vmware.vcloud.networkConfigSection+xml\">");
                    xml.append("<Info xmlns=\"http://schemas.dmtf.org/ovf/envelope/1\">The configuration parameters for logical networks</Info>");
                    xml.append("<NetworkConfig networkName=\"").append(vlan.getName()).append("\">");
                    xml.append("<Configuration>");
                    xml.append("<ParentNetwork href=\"").append(method.toURL("network", vlan.getProviderVlanId())).append("\"/>");
                    xml.append("<FenceMode>bridged</FenceMode>");
                    xml.append("<RetainNetInfoAcrossDeployments>false</RetainNetInfoAcrossDeployments>");
                    xml.append("</Configuration>");
                    xml.append("<IsDeployed>false</IsDeployed>");
                    xml.append("</NetworkConfig>");
                    xml.append("</NetworkConfigSection>");
                    xml.append("</InstantiationParams>");
                }
            }
            String idString = (String)img.getTag("childVirtualMachineIds");
            String[] ids;

            if( idString.contains(",") ) {
                ids = idString.split(",");
            }
            else {
                ids = new String[] { idString };
            }
            int count = 1;

            for( String id : ids ) {
                String suffix = ((ids.length > 1) ? ("-" + count) : "");
                String templateVmUrl = method.toURL("vAppTemplate", id);

                count++;
                xml.append("<SourcedItem>");
                xml.append("<Source href=\"").append(templateVmUrl).append("\" name=\"").append(vCloud.escapeXml(withLaunchOptions.getFriendlyName() + suffix)).append("\"/>");
                xml.append("<InstantiationParams>");

                if( vlan != null ) {
                    xml.append("<NetworkConnectionSection href=\"").append(templateVmUrl).append("/networkConnectionSection/").append("\" ");
                    xml.append(" type=\"").append(method.getMediaTypeForNetworkConnectionSection()).append("\">");
                    xml.append("<Info xmlns=\"http://schemas.dmtf.org/ovf/envelope/1\">Specifies the available VM network connections</Info>");
                    xml.append("<PrimaryNetworkConnectionIndex>0</PrimaryNetworkConnectionIndex>");
                    xml.append("<NetworkConnection network=\"").append(vCloud.escapeXml(vlan.getName())).append("\">");
                    xml.append("<NetworkConnectionIndex>0</NetworkConnectionIndex>");
                    xml.append("<IsConnected>true</IsConnected>");
                    xml.append("<IpAddressAllocationMode>DHCP</IpAddressAllocationMode>");
                    xml.append("</NetworkConnection>");
                    xml.append("</NetworkConnectionSection>");
                }
                xml.append("<GuestCustomizationSection href=\"").append(method.toURL("vAppTemplate", id)).append("/guestCustomizationSection/\" ");
                xml.append("type=\"").append(method.getMediaTypeForGuestConnectionSection()).append("\">");

                xml.append("<Info xmlns=\"http://schemas.dmtf.org/ovf/envelope/1\">Specifies Guest OS Customization Settings</Info>");
                xml.append("<Enabled>true</Enabled>");
                xml.append("<ChangeSid>").append(String.valueOf(img.getPlatform().isWindows())).append("</ChangeSid>");
                xml.append("<VirtualMachineId>").append(UUID.randomUUID().toString()).append("</VirtualMachineId>");
                xml.append("<JoinDomainEnabled>false</JoinDomainEnabled>");
                xml.append("<UseOrgSettings>false</UseOrgSettings>");
                String pw = withLaunchOptions.getBootstrapPassword();

                xml.append("<AdminPasswordEnabled>true</AdminPasswordEnabled>");
                if( pw != null ) {
                    xml.append("<AdminPassword>").append(vCloud.escapeXml(pw)).append("</AdminPassword>");
                    //xml.append("<AdminPasswordAuto>false</AdminPasswordAuto>");
                }
                else {
                    xml.append("<AdminPasswordAuto>true</AdminPasswordAuto>");
                }
                xml.append("<ResetPasswordRequired>false</ResetPasswordRequired>");
                xml.append("<ComputerName>").append(vCloud.escapeXml(validateHostName(withLaunchOptions.getHostName() + suffix))).append("</ComputerName>");
                String userData = withLaunchOptions.getUserData();

                if( userData != null && userData.length() > 0 ) {
                    xml.append("<CustomizationScript>").append(vCloud.escapeXml(userData)).append("</CustomizationScript>");
                }
                xml.append("</GuestCustomizationSection>");

                xml.append("</InstantiationParams>");
                xml.append("</SourcedItem>");
            }
            xml.append("</ComposeVAppParams>");

            if( logger.isDebugEnabled() ) {
                try {
                    method.parseXML(xml.toString());
                    logger.debug("XML passes");
                }
                catch( Throwable t ) {
                    logger.error("XML parse failure: " + t.getMessage());
                }
            }
            String response = method.post(vCloudMethod.COMPOSE_VAPP, vdcId, xml.toString());

            Document doc = method.parseXML(response);
            String docElementTagName = doc.getDocumentElement().getTagName();
            String nsString = "";
            if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            NodeList vapps = doc.getElementsByTagName(nsString + "VApp");

            if( vapps.getLength() < 1 ) {
                throw new CloudException("The instatiation operation succeeded, but no vApp was present");
            }
            Node vapp = vapps.item(0);
            Node href = vapp.getAttributes().getNamedItem("href");

            method.waitFor(response);

            if( href != null ) {
                String vappId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());
                response = method.get("vApp", vappId);

                if( response == null || response.equals("") ) {
                    throw new CloudException("vApp " + vappId + " went away");
                }
                doc = method.parseXML(response);
                docElementTagName = doc.getDocumentElement().getTagName();
                nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                vapps = doc.getElementsByTagName(nsString + "VApp");
                if( vapps.getLength() < 1 ) {
                    throw new CloudException("No VApp in vApp request for " + vappId);
                }
                vapp = vapps.item(0);
                NodeList tasks = vapp.getChildNodes();

                for( int i=0; i<tasks.getLength(); i++ ) {
                    Node task = tasks.item(i);
                    if(task.getNodeName().contains(":"))nsString = task.getNodeName().substring(0, task.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( task.getNodeName().equalsIgnoreCase(nsString + "Task") ) {
                        href = task.getAttributes().getNamedItem("href");
                        if( href != null ) {
                            method.waitFor(href.getNodeValue().trim());
                        }
                    }
                }

                deploy(vappId);
                String x = method.get("vApp", vappId);

                if( x == null ) {
                    throw new CloudException("vApp went away");
                }
                doc = method.parseXML(response);
                docElementTagName = doc.getDocumentElement().getTagName();
                nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                vapps = doc.getElementsByTagName(nsString + "VApp");
                if( vapps.getLength() < 1 ) {
                    throw new CloudException("vApp went away");
                }
                vapp = vapps.item(0);
                NodeList attributes = vapp.getChildNodes();
                String vmId = null;

                for( int i=0; i<attributes.getLength(); i++ ) {
                    Node attribute = attributes.item(i);
                    if(attribute.getNodeName().contains(":"))nsString = attribute.getNodeName().substring(0, attribute.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( attribute.getNodeName().equals(nsString + "Children") && attribute.hasChildNodes() ) {
                        NodeList children = attribute.getChildNodes();

                        for( int j=0; j<children.getLength(); j++ ) {
                            Node vm = children.item(j);
                            if(vm.getNodeName().contains(":"))nsString = vm.getNodeName().substring(0, vm.getNodeName().indexOf(":") + 1);
                            else nsString = "";

                            if( vm.getNodeName().equalsIgnoreCase(nsString + "Vm") && vm.hasAttributes() ) {
                                href = vm.getAttributes().getNamedItem("href");
                                if( href != null ) {
                                    String vmUrl = href.getNodeValue().trim();

                                    vmId = ((vCloud)getProvider()).toID(vmUrl);
                                    if( product != null ) {
                                        xml = new StringBuilder();

                                        xml.append("<ovf:VirtualHardwareSection xmlns:ovf=\"http://schemas.dmtf.org/ovf/envelope/1\" ");
                                        xml.append("xmlns:rasd=\"http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData\" ");
                                        xml.append("xmlns:vcloud=\"http://www.vmware.com/vcloud/v1.5\" ");
                                        xml.append("xmlns:vssd=\"http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_VirtualSystemSettingData\" ");
                                        xml.append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ");
                                        xml.append("ovf:transport=\"\" ");
                                        xml.append("vcloud:href=\"").append(vmUrl).append("/virtualHardwareSection/\" ");
                                        xml.append("vcloud:type=\"application/vnd.vmware.vcloud.virtualHardwareSection+xml\">");
                                        xml.append("<ovf:Info>Virtual hardware requirements</ovf:Info>");
                                        xml.append("<ovf:Item vcloud:href=\"").append(vmUrl).append("/virtualHardwareSection/cpu\">");
                                        xml.append("<rasd:AllocationUnits>hertz * 10^6</rasd:AllocationUnits>");
                                        xml.append("<rasd:Description>Number of Virtual CPUs</rasd:Description>");
                                        xml.append("<rasd:ElementName>").append(String.valueOf(product.getCpuCount())).append(" virtual CPU(s)</rasd:ElementName>");
                                        xml.append("<rasd:InstanceID>1</rasd:InstanceID>");
                                        xml.append("<rasd:Reservation>0</rasd:Reservation>");
                                        xml.append("<rasd:ResourceType>3</rasd:ResourceType>");
                                        xml.append("<rasd:VirtualQuantity>").append(String.valueOf(product.getCpuCount())).append("</rasd:VirtualQuantity>");
                                        xml.append("<rasd:Weight>0</rasd:Weight>");
                                        xml.append("</ovf:Item>");
                                        xml.append("<ovf:Item  vcloud:href=\"").append(vmUrl).append("/virtualHardwareSection/memory\">");
                                        xml.append("<rasd:AllocationUnits>byte * 2^20</rasd:AllocationUnits>");
                                        xml.append("<rasd:Description>Memory Size</rasd:Description>");
                                        xml.append("<rasd:ElementName>").append(product.getRamSize().toString()).append("</rasd:ElementName>");
                                        xml.append("<rasd:InstanceID>2</rasd:InstanceID>");
                                        xml.append("<rasd:Reservation>0</rasd:Reservation>");
                                        xml.append("<rasd:ResourceType>4</rasd:ResourceType>");
                                        xml.append("<rasd:VirtualQuantity>").append(String.valueOf(product.getRamSize().intValue())).append("</rasd:VirtualQuantity>");
                                        xml.append("<rasd:Weight>0</rasd:Weight>");
                                        xml.append("</ovf:Item>");
                                        xml.append("</ovf:VirtualHardwareSection>");
                                        method.waitFor(method.put("virtualHardwareSection", vmUrl + "/virtualHardwareSection", method.getMediaTypeForVirtualHardwareSection(), xml.toString()));
                                        if( vlan != null ) {
                                            xml = new StringBuilder();
                                            xml.append("<NetworkConnectionSection href=\"").append(vmUrl).append("/networkConnectionSection/").append("\" ");
                                            xml.append("xmlns=\"http://www.vmware.com/vcloud/v1.5\" ");
                                            xml.append(" type=\"").append(method.getMediaTypeForNetworkConnectionSection()).append("\">");
                                            xml.append("<Info xmlns=\"http://schemas.dmtf.org/ovf/envelope/1\">Specifies the available VM network connections</Info>");
                                            xml.append("<PrimaryNetworkConnectionIndex>0</PrimaryNetworkConnectionIndex>");
                                            xml.append("<NetworkConnection network=\"").append(vCloud.escapeXml(vlan.getName())).append("\">");
                                            xml.append("<NetworkConnectionIndex>0</NetworkConnectionIndex>");
                                            xml.append("<IsConnected>true</IsConnected>");
                                            xml.append("<IpAddressAllocationMode>DHCP</IpAddressAllocationMode>");
                                            xml.append("</NetworkConnection>");
                                            xml.append("</NetworkConnectionSection>");
                                            method.waitFor(method.put("networkConnectionSection", vmUrl + "/networkConnectionSection", method.getMediaTypeForNetworkConnectionSection(), xml.toString()));
                                        }
                                    }
                                    try {
                                        Map<String,Object> metadata = withLaunchOptions.getMetaData();

                                        if( metadata == null ) {
                                            metadata = new HashMap<String, Object>();
                                        }
                                        metadata.put("dsnImageId", img.getProviderMachineImageId());
                                        metadata.put("dsnCreated", String.valueOf(System.currentTimeMillis()));
                                        method.postMetaData("vApp", vmId, metadata);
                                    }
                                    catch( Throwable ignore ) {
                                        logger.warn("Error updating meta-data on launch: " + ignore.getMessage());
                                    }
                                    break;
                                }
                            }
                        }
                    }
                    if( vmId != null ) {
                        break;
                    }
                }
                if( vmId == null ) {
                    throw new CloudException("No virtual machines exist in " + vappId);
                }
                startVapp(vappId, true);
                VirtualMachine vm = getVirtualMachine(vmId);

                if( vm == null ) {
                    throw new CloudException("Unable to identify vm " + vmId + " in " + vappId);
                }
                vm.setProviderMachineImageId(img.getProviderMachineImageId());
                return vm;
            }
            throw new CloudException("Unable to identify virtual machine in response");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "listVMProducts");
        try {
            Cache<VirtualMachineProduct> cache = Cache.getInstance(getProvider(), "products" + architecture.name(), VirtualMachineProduct.class, CacheLevel.REGION, new TimePeriod<Day>(1, TimePeriod.DAY));
            Iterable<VirtualMachineProduct> products = cache.get(getContext());

            if( products == null ) {
                ArrayList<VirtualMachineProduct> list = new ArrayList<VirtualMachineProduct>();

                try {
                    String resource = ((vCloud)getProvider()).getVMProductsResource();
                    InputStream input = vAppSupport.class.getResourceAsStream(resource);

                    if( input != null ) {
                        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                        StringBuilder json = new StringBuilder();
                        String line;

                        while( (line = reader.readLine()) != null ) {
                            json.append(line);
                            json.append("\n");
                        }
                        JSONArray arr = new JSONArray(json.toString());
                        JSONObject toCache = null;

                        for( int i=0; i<arr.length(); i++ ) {
                            JSONObject productSet = arr.getJSONObject(i);
                            String cloud, provider;

                            if( productSet.has("cloud") ) {
                                cloud = productSet.getString("cloud");
                            }
                            else {
                                continue;
                            }
                            if( productSet.has("provider") ) {
                                provider = productSet.getString("provider");
                            }
                            else {
                                continue;
                            }
                            if( !productSet.has("products") ) {
                                continue;
                            }
                            if( toCache == null || (provider.equals("default") && cloud.equals("default")) ) {
                                toCache = productSet;
                            }
                            if( provider.equalsIgnoreCase(getProvider().getProviderName()) && cloud.equalsIgnoreCase(getProvider().getCloudName()) ) {
                                toCache = productSet;
                                break;
                            }
                        }
                        if( toCache == null ) {
                            logger.warn("No products were defined");
                            return Collections.emptyList();
                        }
                        JSONArray plist = toCache.getJSONArray("products");

                        for( int i=0; i<plist.length(); i++ ) {
                            JSONObject product = plist.getJSONObject(i);
                            boolean supported = false;

                            if( product.has("architectures") ) {
                                JSONArray architectures = product.getJSONArray("architectures");

                                for( int j=0; j<architectures.length(); j++ ) {
                                    String a = architectures.getString(j);

                                    if( architecture.name().equals(a) ) {
                                        supported = true;
                                        break;
                                    }
                                }
                            }
                            if( !supported ) {
                                continue;
                            }
                            if( product.has("excludesRegions") ) {
                                JSONArray regions = product.getJSONArray("excludesRegions");

                                for( int j=0; j<regions.length(); j++ ) {
                                    String r = regions.getString(j);

                                    if( r.equals(getContext().getRegionId()) ) {
                                        supported = false;
                                        break;
                                    }
                                }
                            }
                            if( !supported ) {
                                continue;
                            }
                            VirtualMachineProduct prd = toProduct(product);

                            if( prd != null ) {
                                list.add(prd);
                            }
                        }
                    }
                    else {
                        logger.warn("No standard products resource exists for " + resource);
                    }

                    products = list;
                    cache.put(getContext(), products);
                }
                catch( IOException e ) {
                    throw new InternalException(e);
                }
                catch( JSONException e ) {
                    throw new InternalException(e);
                }
            }
            return products;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        Cache<Architecture> cache = Cache.getInstance(getProvider(), "architectures", Architecture.class, CacheLevel.CLOUD);
        Iterable<Architecture> list = cache.get(getContext());

        if( list == null) {
            ArrayList<Architecture> a = new ArrayList<Architecture>();

            a.add(Architecture.I32);
            a.add(Architecture.I64);
            list = a;
            cache.put(getContext(), Collections.unmodifiableList(a));
        }
        return list;
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "listVirtualMachines");
        try {
            Iterable<VLAN> vlans = ((vCloud)getProvider()).getNetworkServices().getVlanSupport().listVlans();
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();

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

                            if( attribute.getNodeName().equalsIgnoreCase(nsString + "ResourceEntities") && attribute.hasChildNodes() ) {
                                NodeList resources = attribute.getChildNodes();

                                for( int j=0; j<resources.getLength(); j++ ) {
                                    Node resource = resources.item(j);

                                    if( resource.getNodeName().equalsIgnoreCase(nsString + "ResourceEntity") && resource.hasAttributes() ) {
                                        Node type = resource.getAttributes().getNamedItem("type");

                                        if( type != null && type.getNodeValue().equalsIgnoreCase(method.getMediaTypeForVApp()) ) {
                                            Node href = resource.getAttributes().getNamedItem("href");

                                            loadVmsFor(dc.getProviderDataCenterId(), ((vCloud)getProvider()).toID(href.getNodeValue().trim()), vms, vlans);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return vms;
        }
        finally {
            APITrace.end();
        }
    }

    private void loadVmsFor(@Nonnull String vdcId, @Nonnull String id, @Nonnull ArrayList<VirtualMachine> vms, @Nonnull Iterable<VLAN> vlans) throws InternalException, CloudException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

        String xml = method.get("vApp", id);

        if( xml == null || xml.equals("") ) {
            return;
        }
        Document doc = method.parseXML(xml);
        String docElementTagName = doc.getDocumentElement().getTagName();
        String nsString = "";
        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
        NodeList vapps = doc.getElementsByTagName(nsString + "VApp");

        if( vapps.getLength() < 1 ) {
            return;
        }
        NodeList attributes = vapps.item(0).getChildNodes();

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);
            if(attribute.getNodeName().contains(":"))nsString = attribute.getNodeName().substring(0, attribute.getNodeName().indexOf(":") + 1);
            else nsString = "";

            if( attribute.getNodeName().equals(nsString + "Children") && attribute.hasChildNodes() ) {
                NodeList children = attribute.getChildNodes();

                for( int j=0; j<children.getLength(); j++ ) {
                    Node vmNode = children.item(j);

                    if( vmNode.getNodeName().equalsIgnoreCase(nsString + "Vm") && vmNode.hasAttributes() ) {
                        VirtualMachine vm = toVirtualMachine(vdcId, id, vmNode, vlans);

                        if( vm != null ) {
                            vms.add(vm);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "rebootVM");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("vApp", vmId);

            if( xml != null ) {
                Document doc = method.parseXML(xml);
                String docElementTagName = doc.getDocumentElement().getTagName();
                String nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList nodes = doc.getElementsByTagName(nsString + "VApp");

                for( int i=0; i<nodes.getLength(); i++ ) {
                    Node node = nodes.item(i);
                    if(node.getNodeName().contains(":"))nsString = node.getNodeName().substring(0, node.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( node.getNodeName().equalsIgnoreCase(nsString + "Link") && node.hasAttributes() ) {
                        Node rel = node.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("power:reboot") ) {
                            Node href = node.getAttributes().getNamedItem("href");

                            if( href != null ) {
                                String endpoint = href.getNodeValue().trim();
                                String action = method.getAction(endpoint);

                                method.post(action, endpoint, null, null);
                                break;
                            }
                        }
                    }
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void resume(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "resumeVM");
        try {
            startVapp(vmId, true);
        }
        finally {
            APITrace.end();
        }
    }
    @Override
    public void start(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "startVM");
        try {
            startVapp(vmId, true);
        }
        finally {
            APITrace.end();
        }
    }

    private void startVapp(@Nonnull String vappId, boolean wait) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vappId);

        if( xml != null ) {
            Document doc = method.parseXML(xml);
            String docElementTagName = doc.getDocumentElement().getTagName();
            String nsString = "";
            if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            NodeList nodes = doc.getElementsByTagName(nsString + "VApp");

            if( nodes.getLength() < 1 ) {
                nodes = doc.getElementsByTagName(nsString + "Vm");
            }
            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);
                    if(node.getNodeName().contains(":"))nsString = node.getNodeName().substring(0, node.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( node.getNodeName().equalsIgnoreCase(nsString + "Link") && node.hasAttributes() ) {
                        Node rel = node.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("power:powerOn") ) {
                            Node href = node.getAttributes().getNamedItem("href");

                            if( href != null ) {
                                String endpoint = href.getNodeValue().trim();
                                String action = method.getAction(endpoint);

                                String task = method.post(action, endpoint, null, null);

                                if( wait ) {
                                    method.waitFor(task);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "stopVM");
        try {
            stop(vmId, force, true);
        }
        finally {
            APITrace.end();
        }
    }

    public void stop(@Nonnull String vmId, boolean force, boolean wait) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vmId);

        if( xml != null ) {
            Document doc = method.parseXML(xml);
            String docElementTagName = doc.getDocumentElement().getTagName();
            String nsString = "";
            if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            NodeList nodes = doc.getElementsByTagName(nsString + "VApp");

            if( nodes.getLength() < 1 ) {
                nodes = doc.getElementsByTagName(nsString + "Vm");
            }
            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);
                    if(node.getNodeName().contains(":"))nsString = node.getNodeName().substring(0, node.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( node.getNodeName().equalsIgnoreCase(nsString + "Link") && node.hasAttributes() ) {
                        Node rel = node.getAttributes().getNamedItem("rel");

                        if( force && rel != null && rel.getNodeValue().trim().equalsIgnoreCase("power:powerOff") ) {
                            Node href = node.getAttributes().getNamedItem("href");

                            if( href != null ) {
                                String endpoint = href.getNodeValue().trim();
                                String action = method.getAction(endpoint);

                                String task = method.post(action, endpoint, null, null);

                                if( wait ) {
                                    method.waitFor(task);
                                }
                                break;
                            }
                        }
                        else if( !force && rel != null && rel.getNodeValue().trim().equalsIgnoreCase("power:shutdown") ) {
                            Node href = node.getAttributes().getNamedItem("href");

                            if( href != null ) {
                                String endpoint = href.getNodeValue().trim();
                                String action = method.getAction(endpoint);

                                String task = method.post(action, endpoint, null, null);

                                if( wait ) {
                                    method.waitFor(task);
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return true;
    }

    @Override
    public boolean supportsSuspendResume(@Nonnull VirtualMachine vm) {
        return true;
    }

    @Override
    public void suspend(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "suspendVM");
        try {
            suspendVapp(vmId);
        }
        finally {
            APITrace.end();
        }
    }

    private void suspendVapp(@Nonnull String vappId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vappId);

        if( xml != null ) {
            Document doc = method.parseXML(xml);
            String docElementTagName = doc.getDocumentElement().getTagName();
            String nsString = "";
            if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            NodeList nodes = doc.getElementsByTagName(nsString + "VApp");

            if( nodes.getLength() < 1 ) {
                nodes = doc.getElementsByTagName(nsString + "Vm");
            }
            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);
                    if(node.getNodeName().contains(":"))nsString = node.getNodeName().substring(0, node.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( node.getNodeName().equalsIgnoreCase(nsString + "Link") && node.hasAttributes() ) {
                        Node rel = node.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("power:suspend") ) {
                            Node href = node.getAttributes().getNamedItem("href");

                            if( href != null ) {
                                String endpoint = href.getNodeValue().trim();
                                String action = method.getAction(endpoint);

                                method.post(action, endpoint, null, null);
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "terminateVM");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            String vappId = (String)vm.getTag(PARENT_VAPP_ID);
            ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();
            boolean contains = false;

            loadVmsFor(vm.getProviderDataCenterId(), vappId, vms, ((vCloud)getProvider()).getNetworkServices().getVlanSupport().listVlans());

            for( VirtualMachine v : vms ) {
                if( v.getProviderVirtualMachineId().equals(vmId) ) {
                    contains = true;
                    break;
                }
            }
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());

            if( vms.size() == 1 && contains ) {
                try { undeploy(vappId); }
                catch( Throwable ignore ) { }
                method.delete("vApp", vappId);
            }
            else {
                try { undeploy(vmId); }
                catch( Throwable ignore ) { }
                method.delete("vApp", vmId);
            }
        }
        finally {
            APITrace.end();
        }
    }

    private @Nullable VirtualMachineProduct toProduct(@Nonnull JSONObject json) throws InternalException {
        VirtualMachineProduct prd = new VirtualMachineProduct();

        try {
            if( json.has("id") ) {
                prd.setProviderProductId(json.getString("id"));
            }
            else {
                return null;
            }
            if( json.has("name") ) {
                prd.setName(json.getString("name"));
            }
            else {
                prd.setName(prd.getProviderProductId());
            }
            if( json.has("description") ) {
                prd.setDescription(json.getString("description"));
            }
            else {
                prd.setDescription(prd.getName());
            }
            if( json.has("cpuCount") ) {
                prd.setCpuCount(json.getInt("cpuCount"));
            }
            else {
                prd.setCpuCount(1);
            }
            if( json.has("rootVolumeSizeInGb") ) {
                prd.setRootVolumeSize(new Storage<Gigabyte>(json.getInt("rootVolumeSizeInGb"), Storage.GIGABYTE));
            }
            else {
                prd.setRootVolumeSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
            }
            if( json.has("ramSizeInMb") ) {
                prd.setRamSize(new Storage<Megabyte>(json.getInt("ramSizeInMb"), Storage.MEGABYTE));
            }
            else {
                prd.setRamSize(new Storage<Megabyte>(512, Storage.MEGABYTE));
            }
            if( json.has("standardHourlyRates") ) {
                JSONArray rates = json.getJSONArray("standardHourlyRates");

                for( int i=0; i<rates.length(); i++ ) {
                    JSONObject rate = rates.getJSONObject(i);

                    if( rate.has("rate") ) {
                        prd.setStandardHourlyRate((float)rate.getDouble("rate"));
                    }
                }
            }
        }
        catch( JSONException e ) {
            throw new InternalException(e);
        }
        return prd;
    }

    private @Nonnull VmState toState(@Nonnull String status) throws CloudException {
        try {
            int s = Integer.parseInt(status);

            switch( s ) {
                case 0: case 1: case 5: case 6: case 7: case 9: return VmState.PENDING;
                case 3: return VmState.SUSPENDED;
                case 4: return VmState.RUNNING;
                case 8: return VmState.STOPPED;
            }
            logger.warn("DEBUG: Unknown vCloud status string for " + getContext().getAccountNumber() + ": " + status);
            return VmState.PENDING;
        }
        catch( NumberFormatException e ) {
            logger.error("DEBUG: Invalid status from vCloud for " + getContext().getAccountNumber() + ": " + status);
            return VmState.PENDING;
        }
    }

    private @Nullable VirtualMachine toVirtualMachine(@Nonnull String vdcId, @Nonnull String parentVAppId, @Nonnull Node vmNode, @Nonnull Iterable<VLAN> vlans) throws CloudException, InternalException {
        Node n = vmNode.getAttributes().getNamedItem("href");
        VirtualMachine vm = new VirtualMachine();

        vm.setProviderMachineImageId("unknown");
        vm.setArchitecture(Architecture.I64);
        vm.setClonable(true);
        vm.setCreationTimestamp(0L);
        vm.setCurrentState(VmState.PENDING);
        vm.setImagable(true);
        vm.setLastBootTimestamp(0L);
        vm.setLastPauseTimestamp(0L);
        vm.setPausable(false);
        vm.setPersistent(true);
        vm.setPlatform(Platform.UNKNOWN);
        vm.setProviderOwnerId(getContext().getAccountNumber());
        vm.setRebootable(true);
        vm.setProviderRegionId(getContext().getRegionId());
        vm.setProviderDataCenterId(vdcId);

        if( n != null ) {
            vm.setProviderVirtualMachineId(((vCloud)getProvider()).toID(n.getNodeValue().trim()));
        }
        n = vmNode.getAttributes().getNamedItem("status");
        if( n != null ) {
            vm.setCurrentState(toState(n.getNodeValue().trim()));
        }
        n = vmNode.getAttributes().getNamedItem("name");
        if( n != null ) {
            vm.setName(n.getNodeValue().trim());
        }
        NodeList attributes = vmNode.getChildNodes();

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);

            if( attribute.getNodeName().equalsIgnoreCase("Description") && attribute.hasChildNodes() ) {
                vm.setDescription(attribute.getFirstChild().getNodeValue().trim());
            }
            else if( attribute.getNodeName().equalsIgnoreCase("GuestCustomizationSection") && attribute.hasChildNodes() ) {
                NodeList elements = attribute.getChildNodes();
                String adminPassword = null;

                for( int j=0; j<elements.getLength(); j++ ) {
                    Node element = elements.item(j);

                    if( element.getNodeName().equalsIgnoreCase("AdminPassword") && element.hasChildNodes() ) {
                        adminPassword = element.getFirstChild().getNodeValue().trim();
                    }
                }
                if( adminPassword != null ) {
                    vm.setRootUser(vm.getPlatform().isWindows() ? "administrator" : "root");
                    vm.setRootPassword(adminPassword);
                }
            }
            else if( attribute.getNodeName().equalsIgnoreCase("DateCreated") && attribute.hasChildNodes() ) {
                vm.setCreationTimestamp(((vCloud)getProvider()).parseTime(attribute.getFirstChild().getNodeValue().trim()));
            }
            else if( attribute.getNodeName().equalsIgnoreCase("NetworkConnectionSection") && attribute.hasChildNodes() ) {
                NodeList elements = attribute.getChildNodes();
                TreeSet<String> addrs = new TreeSet<String>();

                for( int j=0; j<elements.getLength(); j++ ) {
                    Node element = elements.item(j);

                    if( element.getNodeName().equalsIgnoreCase("NetworkConnection") ) {
                        if( element.hasChildNodes() ) {
                            NodeList parts = element.getChildNodes();
                            Boolean connected = null;
                            String addr = null;

                            for( int k=0; k<parts.getLength(); k++ ) {
                                Node part = parts.item(k);

                                if( part.getNodeName().equalsIgnoreCase("IpAddress") && part.hasChildNodes() ) {
                                    addr = part.getFirstChild().getNodeValue().trim();
                                }
                                if( part.getNodeName().equalsIgnoreCase("IsConnected") && part.hasChildNodes() ) {
                                    connected = part.getFirstChild().getNodeValue().trim().equalsIgnoreCase("true");
                                }
                            }
                            if( (connected == null || connected) && addr != null ) {
                                addrs.add(addr);
                            }
                        }
                        if( element.hasAttributes() ) {
                            Node net = element.getAttributes().getNamedItem("network");

                            if( net != null ) {
                                String netNameOrId = net.getNodeValue().trim();
                                boolean compat = ((vCloud)getProvider()).isCompat();

                                for( VLAN vlan : vlans ) {
                                    boolean matches = false;

                                    if( !compat && vlan.getProviderVlanId().equals(netNameOrId) ) {
                                        matches = true;
                                    }
                                    else if( compat && vlan.getProviderVlanId().equals("/network/" + netNameOrId) ) {
                                        matches = true;
                                    }
                                    else if( vlan.getName().equals(netNameOrId) ) {
                                        matches = true;
                                    }
                                    if( matches ) {
                                        vm.setProviderVlanId(vlan.getProviderVlanId());
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                if( addrs.size() > 0 ) {
                    if( addrs.size() == 1 ) {
                        RawAddress a = new RawAddress(addrs.iterator().next());

                        if( isPublicIpAddress(a) ) {
                            vm.setPublicAddresses(a);
                        }
                        else {
                            vm.setPrivateAddresses(a);
                        }
                    }
                    else {
                        ArrayList<RawAddress> pub = new ArrayList<RawAddress>();
                        ArrayList<RawAddress> priv = new ArrayList<RawAddress>();

                        for( String addr : addrs ) {
                            RawAddress r = new RawAddress(addr);

                            if( isPublicIpAddress(r) ) {
                                pub.add(r);
                            }
                            else {
                                priv.add(r);
                            }
                        }
                        if( priv.size() > 0 ) {
                            vm.setPrivateAddresses(priv.toArray(new RawAddress[priv.size()]));
                        }
                        if( pub.size() > 0 ) {
                            vm.setPublicAddresses(pub.toArray(new RawAddress[pub.size()]));
                        }
                    }
                }
            }
           else if( attribute.getNodeName().equalsIgnoreCase("ovf:OperatingSystemSection") && attribute.hasChildNodes() ) {
                NodeList os = attribute.getChildNodes();

                for( int j=0; j<os.getLength(); j++ ) {
                    Node osdesc = os.item(j);

                    if( osdesc.getNodeName().equalsIgnoreCase("ovf:Description") && osdesc.hasChildNodes() ) {
                        String desc = osdesc.getFirstChild().getNodeValue();

                        vm.setPlatform(Platform.guess(desc));

                        if( desc.contains("32") || (desc.contains("x86") && !desc.contains("64")) ) {
                            vm.setArchitecture(Architecture.I32);
                        }
                    }
                }
            }
            else if( attribute.getNodeName().equalsIgnoreCase("ovf:VirtualHardwareSection") && attribute.hasChildNodes() ) {
                    NodeList hardware = attribute.getChildNodes();
                int memory = 0, cpu = 0;

                for( int j=0; j<hardware.getLength(); j++ ) {
                    Node item = hardware.item(j);

                    if( item.getNodeName().equalsIgnoreCase("ovf:item") && item.hasChildNodes() ) {
                        NodeList bits = item.getChildNodes();
                        String rt = null;
                        int qty = 0;

                        for( int k=0; k<bits.getLength(); k++ ) {
                            Node bit = bits.item(k);

                            if( bit.getNodeName().equalsIgnoreCase("rasd:ResourceType") && bit.hasChildNodes() ) {
                                rt = bit.getFirstChild().getNodeValue().trim();
                            }
                            else if( bit.getNodeName().equalsIgnoreCase("rasd:VirtualQuantity") && bit.hasChildNodes() ) {
                                try {
                                    qty = Integer.parseInt(bit.getFirstChild().getNodeValue().trim());
                                }
                                catch( NumberFormatException ignore ){
                                    // ignore
                                }
                            }

                        }
                        if( rt != null ) {
                            if( rt.equals("3") ) { // cpu
                                cpu = qty;
                            }
                            else if( rt.equals("4") ) {     // memory
                                memory = qty;
                            }
                            /*
                            else if( rt.equals("10") ) {     // NIC

                            }
                            else if( rt.equals("17") ) {     // disk

                            }
                            */
                        }
                    }
                }
                VirtualMachineProduct product = null;

                for( VirtualMachineProduct prd : listProducts(Architecture.I64) ) {
                    if( prd.getCpuCount() == cpu && memory == prd.getRamSize().intValue() ) {
                        product = prd;
                        break;
                    }
                }
                if( product == null ) {
                    vm.setProductId("custom:" + cpu + ":" + memory);
                }
                else {
                    vm.setProductId(product.getProviderProductId());
                }
            }
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
        Platform p = vm.getPlatform();

        if( p == null || p.equals(Platform.UNKNOWN) || p.equals(Platform.UNIX) ) {
            p = Platform.guess(vm.getName() + " " + vm.getDescription());
            if( Platform.UNIX.equals(vm.getPlatform()) ) {
                if( p.isUnix() ) {
                    vm.setPlatform(p);
                }
            }
            else {
                vm.setPlatform(p);
            }
        }
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("vApp", vm.getProviderVirtualMachineId() + "/metadata");

            if( xml != null && !xml.equals("") ) {
                method.parseMetaData(vm, xml);

                String t;

                if( vm.getCreationTimestamp() < 1L ) {
                    t = (String)vm.getTag("dsnCreated");
                    if( t != null ) {
                        try { vm.setCreationTimestamp(Long.parseLong(t)); }
                        catch( Throwable ignore ) { }
                    }
                }
                t = (String)vm.getTag("dsnImageId");
                if( t != null && "unknown".equals(vm.getProviderMachineImageId()) ) {
                    vm.setProviderMachineImageId(t);
                }
            }
        }
        catch( Throwable ignore ) {
            // ignore
        }
        vm.setTag(PARENT_VAPP_ID, parentVAppId);
        return vm;
    }

    public void undeploy(@Nonnull String vmId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vmId);

        if( xml != null ) {
            Document doc = method.parseXML(xml);
            String docElementTagName = doc.getDocumentElement().getTagName();
            String nsString = "";
            if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            NodeList nodes = doc.getElementsByTagName(nsString + "VApp");

            if( nodes.getLength() < 1 ) {
                nodes = doc.getElementsByTagName(nsString + "Vm");
            }
            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);
                    if(node.getNodeName().contains(":"))nsString = node.getNodeName().substring(0, node.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( node.getNodeName().equalsIgnoreCase(nsString + "Link") && node.hasAttributes() ) {
                        Node rel = node.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("undeploy") ) {
                            Node href = node.getAttributes().getNamedItem("href");

                            if( href != null ) {
                                String endpoint = href.getNodeValue().trim();
                                String action = method.getAction(endpoint);
                                StringBuilder payload = new StringBuilder();

                                payload.append("<UndeployVAppParams xmlns=\"http://www.vmware.com/vcloud/v1.5\"/>");
                                try {
                                    method.waitFor(method.post(action, endpoint, method.getMediaTypeForActionUndeployVApp(), payload.toString()));
                                }
                                catch( vCloudException e ) {
                                    if( e.getProviderCode().contains("BUSY_ENTITY") ) {
                                        try { Thread.sleep(15000L); }
                                        catch( InterruptedException ignore ) { }
                                        undeploy(vmId);
                                        return;
                                    }
                                    throw e;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    private @Nonnull String validateHostName(@Nonnull String src) {
        StringBuilder str = new StringBuilder();
        src = src.toLowerCase();
        for( int i=0; i<src.length(); i++ ) {
            char c = src.charAt(i);

            if( str.length() < 1 ) {
                if( Character.isLetterOrDigit(c) ) {
                    str.append(c);
                }
            }
            else {
                if( Character.isLetterOrDigit(c) ) {
                    str.append(c);
                }
                else if( c == '-' ) {
                    str.append(c);
                }
                else if( c == ' ' ) {
                    str.append('-');
                }
            }
        }
        if( str.length() < 1 ) {
            str.append("unnamed");
        }
        return str.toString();
    }

    public boolean isPublicIpAddress(RawAddress addr) {
        String ipAddress = addr.getIpAddress().toLowerCase();

        if( addr.getVersion().equals(IPVersion.IPV4) ) {
            if( ipAddress.startsWith("10.") || ipAddress.startsWith("192.168") || ipAddress.startsWith("169.254") ) {
                return false;
            }
            else if( ipAddress.startsWith("172.") ) {
                String[] parts = ipAddress.split("\\.");

                if( parts.length != 4 ) {
                    return true;
                }
                int x = Integer.parseInt(parts[1]);

                if( x >= 16 && x <= 31 ) {
                    return false;
                }
            }
        }
        else {
            if( ipAddress.startsWith("fd") || ipAddress.startsWith("fc00:")) {
                return false;
            }
        }
        return true;
    }
}
