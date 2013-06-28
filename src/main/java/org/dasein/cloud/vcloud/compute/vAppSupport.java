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
import org.dasein.cloud.compute.AbstractVMSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.network.VLAN;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vcloud.vCloud;
import org.dasein.cloud.vcloud.vCloudException;
import org.dasein.cloud.vcloud.vCloudMethod;
import org.dasein.util.Jiterator;
import org.dasein.util.JiteratorPopulator;
import org.dasein.util.PopulatorThread;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
public class vAppSupport extends AbstractVMSupport {
    static private final Logger logger = vCloud.getLogger(vAppSupport.class);

    static public final String PARENT_VAPP_ID = "parentVAppId";

    vAppSupport(@Nonnull vCloud provider) {
        super(provider);
    }

    public void deploy(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.deploy");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("vApp", vmId);

            if( xml != null ) {
                NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

                for( int i=0; i<nodes.getLength(); i++ ) {
                    NodeList links = nodes.item(i).getChildNodes();

                    for( int j=0; j<links.getLength(); j++ ) {
                        Node node = links.item(j);

                        if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
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
        finally {
            APITrace.end();
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
        APITrace.begin(getProvider(), "VM.getMaximumVirtualMachineCount");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());

            return method.getVMQuota();
        }
        finally {
            APITrace.end();
        }
    }

    public @Nullable VirtualMachineProduct getProduct(@Nonnull String productId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "VM.getProduct");
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
        NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

        if( nodes.getLength() < 1 ) {
            return null;
        }
        Node node = nodes.item(0);
        NodeList elements = node.getChildNodes();

        for( int i=0; i<elements.getLength(); i++ ) {
            Node n = elements.item(i);

            if( n.getNodeName().equalsIgnoreCase("Link") && n.hasAttributes() ) {
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
        APITrace.begin(getProvider(), "VM.getVirtualMachine");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("vApp", vmId);

            if( xml != null && !xml.equals("") ) {
                NodeList vmNodes = method.parseXML(xml).getElementsByTagName("Vm");

                if( vmNodes.getLength() < 1 ) {
                    return null;
                }
                Node vmNode = vmNodes.item(0);
                NodeList vmElements = vmNode.getChildNodes();
                String vdc = null, parentVapp = null;

                for( int i=0; i<vmElements.getLength(); i++ ) {
                    Node n = vmElements.item(i);

                    if( n.getNodeName().equalsIgnoreCase("Link") && n.hasAttributes() ) {
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
        APITrace.begin(getProvider(), "VM.isSubscribed");
        try {
            return (getProvider().testContext() != null);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull final VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.launch");
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
            final VirtualMachineProduct product = getProduct(withLaunchOptions.getStandardProductId());
            final vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            final MachineImage img = ((vCloud)getProvider()).getComputeServices().getImageSupport().getImage(withLaunchOptions.getMachineImageId());

            if( img == null ) {
                throw new CloudException("No such image: " + withLaunchOptions.getMachineImageId());
            }
            StringBuilder xml = new StringBuilder();

            xml.append("<InstantiateVAppTemplateParams xmlns:ovf=\"http://schemas.dmtf.org/ovf/envelope/1\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" name=\"").append(withLaunchOptions.getFriendlyName()).append(" Parent vApp\" xmlns=\"http://www.vmware.com/vcloud/v1.5\" deploy=\"false\" powerOn=\"false\">");
            xml.append("<Description>").append(img.getProviderMachineImageId()).append("</Description>");
            String vlanId = withLaunchOptions.getVlanId();
            final VLAN vlan;

            if( vlanId != null ) {
                vlan = ((vCloud)getProvider()).getNetworkServices().getVlanSupport().getVlan(vlanId);
                if( vlan != null ) {
                    String vAppTemplateUrl = method.toURL("vAppTemplate", img.getProviderMachineImageId());
                    xml.append("<Source href=\"").append(vAppTemplateUrl).append("\"/>");
                }
                else {
                    throw new CloudException("Failed to find vlan " + vlanId);
                }
            }
            else {
                throw new CloudException("No vlan specified.");
            }
            xml.append("<AllEULAsAccepted>true</AllEULAsAccepted>");
            xml.append("</InstantiateVAppTemplateParams>");

            if( logger.isDebugEnabled() ) {
                try {
                    method.parseXML(xml.toString());
                    logger.debug("XML passes");
                }
                catch( Throwable t ) {
                    logger.error("XML parse failure: " + t.getMessage());
                }
            }
            String instantiateResponse = method.post(vCloudMethod.INSTANTIATE_VAPP, vdcId, xml.toString());

            try {
                method.waitFor(instantiateResponse);
            } catch (CloudException e) {
                logger.error("Error waiting for " + vCloudMethod.INSTANTIATE_VAPP + " task to complete", e);
                throw new CloudException("Error waiting for " + vCloudMethod.INSTANTIATE_VAPP + " task to complete");
            }

            Document composeDoc = method.parseXML(instantiateResponse);
            String docElementTagName = composeDoc.getDocumentElement().getTagName();
            final String nsString;
            if(docElementTagName.contains(":")) {
                nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            }
            else {
                nsString = "";
            }
            NodeList vapps = composeDoc.getElementsByTagName(nsString + "VApp");

            if( vapps.getLength() < 1 ) {
                throw new CloudException("The instantiation operation succeeded, but no vApp was present");
            }
            Node vapp = vapps.item(0);
            Node href = vapp.getAttributes().getNamedItem("href");

            String vappId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());

            String vAppResponse = method.get("vApp", vappId);

            if( vAppResponse == null ) {
                throw new CloudException("vApp went away");
            }

            final Document doc = method.parseXML(vAppResponse);

            final String vmId;
            Node vmNode = doc.getElementsByTagName(nsString + "Vm").item(0);

            if( vmNode != null && vmNode.hasAttributes() ) {
                Node vmHref = vmNode.getAttributes().getNamedItem("href");
                if( vmHref != null ) {
                    String vmUrl = vmHref.getNodeValue().trim();
                    vmId = ((vCloud)getProvider()).toID(vmUrl);
                }
                else {
                    vmId = null;
                }
            }
            else {
                vmId = null;
            }

            if( vmId == null ) {
                throw new CloudException("No virtual machines exist in response");
            }
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("Unable to identify vm " + vmId + ".");
            }

            Thread t = new Thread() {
                public void run() {
                    try {
                        Map<String,Object> metadata = withLaunchOptions.getMetaData();

                        if( metadata == null ) {
                            metadata = new HashMap<String, Object>();
                        }
                        metadata.put("dsnImageId", img.getProviderMachineImageId());
                        metadata.put("dsnCreated", String.valueOf(System.currentTimeMillis()));
                        method.postMetaData("vApp", vmId, metadata);
                    }
                    catch( Throwable warn ) {
                        logger.warn("Error updating meta-data on launch: " + warn.getMessage());
                    }

                    NodeList vapps = doc.getElementsByTagName(nsString + "VApp");

                    if( vapps.getLength() < 1 ) {
                        logger.error("The instantiation operation succeeded, but no vApp was present");
                    }
                    Node vapp = vapps.item(0);
                    Node href = vapp.getAttributes().getNamedItem("href");

                    if( href != null ) {
                        String vappId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());
                        String vAppResponse;
                        try {
                            vAppResponse = method.get("vApp", vappId);
                        } catch (CloudException e) {
                            logger.error("Error getting vApp " + vappId, e);
                            return;
                        } catch (InternalException e) {
                            logger.error("Error getting vApp " + vappId, e);
                            return;
                        }

                        if( vAppResponse == null || vAppResponse.equals("") ) {
                            logger.error("vApp " + vappId + " went away");
                        }
                        Document vAppDoc;
                        try {
                            vAppDoc = method.parseXML(vAppResponse);
                        } catch (CloudException e) {
                            logger.error("Error parsing vApp " + vappId + " xml: ", e);
                            return;
                        } catch (InternalException e) {
                            logger.error("Error parsing vApp " + vappId + " xml: ", e);
                            return;
                        }
                        String docElementTagName = vAppDoc.getDocumentElement().getTagName();
                        String nsString = "";
                        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                        vapps = vAppDoc.getElementsByTagName(nsString + "VApp");
                        if( vapps.getLength() < 1 ) {
                            logger.error("No VApp in vApp request for " + vappId);
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
                                    try {
                                        method.waitFor(href.getNodeValue().trim());
                                    } catch (CloudException e) {
                                        logger.error("Error waiting for task to complete.", e);
                                    }
                                }
                            }
                        }

                        try {
                            deploy(vappId);
                        } catch (CloudException e) {
                            logger.error("Error deploying vApp " + vappId, e);
                            return;
                        } catch (InternalException e) {
                            logger.error("Error deploying vApp " + vappId, e);
                            return;
                        }
                        String vAppGetResponse;
                        try {
                            vAppGetResponse = method.get("vApp", vappId);
                        } catch (CloudException e) {
                            logger.error("Error getting vApp " + vappId, e);
                            return;
                        } catch (InternalException e) {
                            logger.error("Error getting vApp " + vappId, e);
                            return;
                        }

                        if( vAppGetResponse == null ) {
                            logger.error("vApp went away");
                        }

                        try {
                            vAppDoc = method.parseXML(vAppGetResponse);
                        } catch (CloudException e) {
                            logger.error("Error parsing vApp " + vappId + " xml: ", e);
                            return;
                        } catch (InternalException e) {
                            logger.error("Error parsing vApp " + vappId + " xml: ", e);
                            return;
                        }
                        docElementTagName = vAppDoc.getDocumentElement().getTagName();
                        nsString = "";
                        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                        vapps = vAppDoc.getElementsByTagName(nsString + "VApp");
                        if( vapps.getLength() < 1 ) {
                            logger.error("vApp went away");
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
                                int count = 1;
                                for( int j=0; j<children.getLength(); j++ ) {
                                    Node vm = children.item(j);
                                    String suffix = ((children.getLength() > 1) ? ("-" + count) : "");
                                    count++;

                                    if(vm.getNodeName().contains(":"))nsString = vm.getNodeName().substring(0, vm.getNodeName().indexOf(":") + 1);
                                    else nsString = "";

                                    if( vm.getNodeName().equalsIgnoreCase(nsString + "Vm") && vm.hasAttributes() ) {
                                        href = vm.getAttributes().getNamedItem("href");
                                        if( href != null ) {
                                            String vmUrl = href.getNodeValue().trim();

                                            vmId = ((vCloud)getProvider()).toID(vmUrl);

                                            StringBuilder guestXml = new StringBuilder();
                                            guestXml.append("<GuestCustomizationSection xmlns=\"http://www.vmware.com/vcloud/v1.5\" ");
                                            guestXml.append(" xmlns:ovf=\"http://schemas.dmtf.org/ovf/envelope/1\" ovf:required=\"false\">");

                                            guestXml.append("<Info xmlns=\"http://schemas.dmtf.org/ovf/envelope/1\">Specifies Guest OS Customization Settings</Info>");
                                            guestXml.append("<Enabled>true</Enabled>");
                                            guestXml.append("<ChangeSid>").append(String.valueOf(img.getPlatform().isWindows())).append("</ChangeSid>");
                                            guestXml.append("<VirtualMachineId>").append(UUID.randomUUID().toString()).append("</VirtualMachineId>");
                                            guestXml.append("<JoinDomainEnabled>false</JoinDomainEnabled>");
                                            guestXml.append("<UseOrgSettings>false</UseOrgSettings>");
                                            String pw = withLaunchOptions.getBootstrapPassword();

                                            guestXml.append("<AdminPasswordEnabled>true</AdminPasswordEnabled>");
                                            if( pw != null ) {
                                                guestXml.append("<AdminPassword>").append(vCloud.escapeXml(pw)).append("</AdminPassword>");
                                                //guestXml.append("<AdminPasswordAuto>false</AdminPasswordAuto>");
                                            }
                                            else {
                                                guestXml.append("<AdminPasswordAuto>true</AdminPasswordAuto>");
                                            }
                                            guestXml.append("<ResetPasswordRequired>false</ResetPasswordRequired>");
                                            guestXml.append("<ComputerName>").append(vCloud.escapeXml(validateHostName(withLaunchOptions.getHostName() + suffix))).append("</ComputerName>");
                                            String userData = withLaunchOptions.getUserData();

                                            if( userData != null && userData.length() > 0 ) {
                                                guestXml.append("<CustomizationScript>").append(vCloud.escapeXml(userData)).append("</CustomizationScript>");
                                            }
                                            guestXml.append("</GuestCustomizationSection>");

                                            try {
                                                method.waitFor(method.put("guestCustomizationSection", vmUrl + "/guestCustomizationSection", method.getMediaTypeForGuestCustomizationSection(), guestXml.toString()));
                                            } catch (CloudException e) {
                                                logger.error("Error configuring guest for vApp " + vappId, e);
                                                return;
                                            } catch (InternalException e) {
                                                logger.error("Error configuring guest for vApp " + vappId, e);
                                                return;
                                            }

                                            if( product != null ) {

                                                StringBuilder xml = new StringBuilder();

                                                xml.append("<vcloud:Item " +
                                                        "xmlns:vcloud=\"http://www.vmware.com/vcloud/v1.5\" " +
                                                        "xmlns:rasd=\"http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData\" " +
                                                        "vcloud:type=\"application/vnd.vmware.vcloud.rasdItem+xml\" " +
                                                        "vcloud:href=\"").append(vmUrl).append("/virtualHardwareSection/cpu\">");
                                                xml.append("<rasd:AllocationUnits>hertz * 10^6</rasd:AllocationUnits>");
                                                xml.append("<rasd:Description>Number of Virtual CPUs</rasd:Description>");
                                                xml.append("<rasd:ElementName>").append(String.valueOf(product.getCpuCount())).append(" virtual CPU(s)</rasd:ElementName>");
                                                xml.append("<rasd:InstanceID>1</rasd:InstanceID>");
                                                xml.append("<rasd:Reservation>0</rasd:Reservation>");
                                                xml.append("<rasd:ResourceType>3</rasd:ResourceType>");
                                                xml.append("<rasd:VirtualQuantity>").append(String.valueOf(product.getCpuCount())).append("</rasd:VirtualQuantity>");
                                                xml.append("<rasd:Weight>0</rasd:Weight>");
                                                xml.append("<vcloud:Link href=\"").append(vmUrl).append("/virtualHardwareSection/cpu\" rel=\"edit\" type=\"application/vnd.vmware.vcloud.rasdItem+xml\"/>");
                                                xml.append("</vcloud:Item>");

                                                try {
                                                    method.waitFor(method.put("virtualHardwareSection/cpu", vmUrl + "/virtualHardwareSection/cpu", method.getMediaTypeForRasdItem(), xml.toString()));
                                                } catch (CloudException e) {
                                                    logger.error("Error configuring virtual hardware cpu for vApp " + vappId, e);
                                                    return;
                                                } catch (InternalException e) {
                                                    logger.error("Error configuring virtual hardware cpu for vApp " + vappId, e);
                                                    return;
                                                }

                                                xml = new StringBuilder();

                                                xml.append("<vcloud:Item " +
                                                        "xmlns:vcloud=\"http://www.vmware.com/vcloud/v1.5\" " +
                                                        "xmlns:rasd=\"http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/CIM_ResourceAllocationSettingData\" " +
                                                        "vcloud:type=\"application/vnd.vmware.vcloud.rasdItem+xml\" " +
                                                        "vcloud:href=\"").append(vmUrl).append("/virtualHardwareSection/memory\">");
                                                xml.append("<rasd:AllocationUnits>byte * 2^20</rasd:AllocationUnits>");
                                                xml.append("<rasd:Description>Memory Size</rasd:Description>");
                                                xml.append("<rasd:ElementName>").append(product.getRamSize().toString()).append("</rasd:ElementName>");
                                                xml.append("<rasd:InstanceID>2</rasd:InstanceID>");
                                                xml.append("<rasd:Reservation>0</rasd:Reservation>");
                                                xml.append("<rasd:ResourceType>4</rasd:ResourceType>");
                                                xml.append("<rasd:VirtualQuantity>").append(String.valueOf(product.getRamSize().intValue())).append("</rasd:VirtualQuantity>");
                                                xml.append("<rasd:Weight>0</rasd:Weight>");
                                                xml.append("<vcloud:Link href=\"").append(vmUrl).append("/virtualHardwareSection/memory\" rel=\"edit\" type=\"application/vnd.vmware.vcloud.rasdItem+xml\"/>");
                                                xml.append("</vcloud:Item>");
                                                try {
                                                    method.waitFor(method.put("virtualHardwareSection/memory", vmUrl + "/virtualHardwareSection/memory", method.getMediaTypeForRasdItem(), xml.toString()));
                                                } catch (CloudException e) {
                                                    logger.error("Error configuring virtual hardware memory for vApp " + vappId, e);
                                                    return;
                                                } catch (InternalException e) {
                                                    logger.error("Error configuring virtual hardware memory for vApp " + vappId, e);
                                                    return;
                                                }


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
                                                    xml.append("<IpAddressAllocationMode>POOL</IpAddressAllocationMode>");
                                                    xml.append("</NetworkConnection>");
                                                    xml.append("</NetworkConnectionSection>");
                                                    try {
                                                        method.waitFor(method.put("networkConnectionSection", vmUrl + "/networkConnectionSection", method.getMediaTypeForNetworkConnectionSection(), xml.toString()));
                                                    } catch (CloudException e) {
                                                        logger.error("Error configuring virtual hardware for vApp " + vappId, e);
                                                        return;
                                                    } catch (InternalException e) {
                                                        logger.error("Error configuring virtual hardware for vApp " + vappId, e);
                                                        return;
                                                    }
                                                }
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
                            logger.error("No virtual machines exist in " + vappId);
                        }
                        try {
                            startVapp(vappId, true);
                        } catch (CloudException e) {
                            logger.error("Error starting vApp " + vappId, e);
                        } catch (InternalException e) {
                            logger.error("Error starting vApp " + vappId, e);
                        }
                    }
                }
            };

            t.setName("Configure vCloud VM " + vm.getProviderVirtualMachineId());
            t.setDaemon(true);
            t.start();
            vm.setProviderMachineImageId(img.getProviderMachineImageId());
            return vm;
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
        getProvider().hold();
        PopulatorThread<VirtualMachine> populator = new PopulatorThread<VirtualMachine>(new JiteratorPopulator<VirtualMachine>() {
            @Override
            public void populate(@Nonnull Jiterator<VirtualMachine> iterator) throws Exception {
                try {
                    APITrace.begin(getProvider(), "VM.listVirtualMachines");
                    try {
                        Iterable<VLAN> vlans = ((vCloud)getProvider()).getNetworkServices().getVlanSupport().listVlans();
                        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

                        for( DataCenter dc : method.listDataCenters() ) {
                            String xml = method.get("vdc", dc.getProviderDataCenterId());

                            if( xml != null && !xml.equals("") ) {
                                NodeList vdcs = method.parseXML(xml).getElementsByTagName("Vdc");

                                if( vdcs.getLength() > 0 ) {
                                    NodeList attributes = vdcs.item(0).getChildNodes();

                                    for( int i=0; i<attributes.getLength(); i++ ) {
                                        Node attribute = attributes.item(i);

                                        if( attribute.getNodeName().equalsIgnoreCase("ResourceEntities") && attribute.hasChildNodes() ) {
                                            NodeList resources = attribute.getChildNodes();

                                            for( int j=0; j<resources.getLength(); j++ ) {
                                                Node resource = resources.item(j);

                                                if( resource.getNodeName().equalsIgnoreCase("ResourceEntity") && resource.hasAttributes() ) {
                                                    Node type = resource.getAttributes().getNamedItem("type");

                                                    if( type != null && type.getNodeValue().equalsIgnoreCase(method.getMediaTypeForVApp()) ) {
                                                        Node href = resource.getAttributes().getNamedItem("href");

                                                        loadVmsFor(dc.getProviderDataCenterId(), ((vCloud)getProvider()).toID(href.getNodeValue().trim()), iterator, vlans);
                                                    }
                                                }
                                            }
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
                finally {
                    getProvider().release();
                }
            }
        });
        populator.populate();
        return populator.getResult();
    }

    private void loadVmsFor(@Nonnull String vdcId, @Nonnull String id, @Nonnull Jiterator<VirtualMachine> vms, @Nonnull Iterable<VLAN> vlans) throws InternalException, CloudException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

        String xml = method.get("vApp", id);

        if( xml == null || xml.equals("") ) {
            return;
        }
        NodeList vapps = method.parseXML(xml).getElementsByTagName("VApp");

        if( vapps.getLength() < 1 ) {
            return;
        }
        NodeList attributes = vapps.item(0).getChildNodes();

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);

            if( attribute.getNodeName().equals("Children") && attribute.hasChildNodes() ) {
                NodeList children = attribute.getChildNodes();

                for( int j=0; j<children.getLength(); j++ ) {
                    Node vmNode = children.item(j);

                    if( vmNode.getNodeName().equalsIgnoreCase("Vm") && vmNode.hasAttributes() ) {
                        VirtualMachine vm = toVirtualMachine(vdcId, id, vmNode, vlans);

                        if( vm != null ) {
                            vms.push(vm);
                        }
                    }
                }
            }
        }
    }

    @Override
    public void reboot(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.reboot");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("vApp", vmId);

            if( xml != null ) {
                NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

                for( int i=0; i<nodes.getLength(); i++ ) {
                    Node node = nodes.item(i);

                    if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
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
        APITrace.begin(getProvider(), "VM.resume");
        try {
            startVapp(vmId, true);
        }
        finally {
            APITrace.end();
        }
    }
    @Override
    public void start(@Nonnull String vmId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.start");
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
            NodeList nodes = doc.getElementsByTagName("VApp");

            if( nodes.getLength() < 1 ) {
                nodes = doc.getElementsByTagName("Vm");
            }
            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);

                    if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
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
        stop(vmId, force, true);
    }

    public void stop(@Nonnull String vmId, boolean force, boolean wait) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "VM.stop");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("vApp", vmId);

            if( xml != null ) {
                Document doc = method.parseXML(xml);
                NodeList nodes = doc.getElementsByTagName("VApp");

                if( nodes.getLength() < 1 ) {
                    nodes = doc.getElementsByTagName("Vm");
                }
                for( int i=0; i<nodes.getLength(); i++ ) {
                    NodeList links = nodes.item(i).getChildNodes();

                    for( int j=0; j<links.getLength(); j++ ) {
                        Node node = links.item(j);

                        if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
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
        finally {
            APITrace.end();
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
        APITrace.begin(getProvider(), "VM.suspend");
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
            NodeList nodes = doc.getElementsByTagName("VApp");

            if( nodes.getLength() < 1 ) {
                nodes = doc.getElementsByTagName("Vm");
            }
            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);

                    if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
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
        APITrace.begin(getProvider(), "VM.terminate");
        try {
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            String vappId = (String)vm.getTag(PARENT_VAPP_ID);
            Jiterator<VirtualMachine> vms = new Jiterator<VirtualMachine>();
            boolean contains = false;
            int count = 0;

            loadVmsFor(vm.getProviderDataCenterId(), vappId, vms, ((vCloud)getProvider()).getNetworkServices().getVlanSupport().listVlans());

            for( VirtualMachine v : vms ) {
                count++;
                if( v.getProviderVirtualMachineId().equals(vmId) ) {
                    contains = true;
                    break;
                }
            }
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());

            if( count == 1 && contains ) {
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

                        if( a.isPublicIpAddress() ) {
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

                            if( r.isPublicIpAddress() ) {
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
            NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

            if( nodes.getLength() < 1 ) {
                nodes = method.parseXML(xml).getElementsByTagName("Vm");
            }
            for( int i=0; i<nodes.getLength(); i++ ) {
                NodeList links = nodes.item(i).getChildNodes();

                for( int j=0; j<links.getLength(); j++ ) {
                    Node node = links.item(j);

                    if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
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
}
