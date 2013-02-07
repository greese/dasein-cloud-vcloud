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
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VMLaunchOptions;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineProduct;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.network.RawAddress;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vcloud.vCloud;
import org.dasein.cloud.vcloud.vCloudMethod;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Megabyte;
import org.dasein.util.uom.storage.Storage;
import org.dasein.util.uom.time.Day;
import org.dasein.util.uom.time.TimePeriod;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
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
import java.util.Locale;
import java.util.TreeSet;

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

    vAppSupport(@Nonnull vCloud provider) {
        super(provider);
    }

    private void deploy(@Nonnull String vmId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vmId);

        if( xml != null ) {
            NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

            for( int i=0; i<nodes.getLength(); i++ ) {
                Node node = nodes.item(i);

                if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
                    Node rel = node.getAttributes().getNamedItem("rel");

                    if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("deploy") ) {
                        Node href = node.getAttributes().getNamedItem("href");

                        if( href != null ) {
                            String endpoint = href.getNodeValue().trim();
                            String action = method.getAction(endpoint);
                            StringBuilder payload = new StringBuilder();

                            payload.append("<DeployVAppParams powerOn=\"false\" xmlns=\"http://www.vmware.com/vcloud/v1\"/>");
                            method.waitFor(method.post(action, endpoint, method.getMediaTypeForActionDeployVApp(), payload.toString()));
                            break;
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

    @Override
    public @Nonnull String getProviderTermForServer(@Nonnull Locale locale) {
        return "VM";
    }

    @Override
    public VirtualMachine getVirtualMachine(@Nonnull String vmId) throws InternalException, CloudException {
        for( VirtualMachine vm : listVirtualMachines() ) { // TODO: be more efficient
            if( vm.getProviderVirtualMachineId().equals(vmId) ) {
                return vm;
            }
        }
        return null;
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
        return Requirement.NONE; // TODO: fix
    }

    @Override
    public boolean isAPITerminationPreventable() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (getProvider().testContext() != null);
    }

    @Override
    public @Nonnull VirtualMachine launch(@Nonnull VMLaunchOptions withLaunchOptions) throws CloudException, InternalException {
        StringBuilder xml = new StringBuilder();
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
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

        xml.append("<InstantiateVAppTemplateParams name=\"").append(vCloud.escapeXml(withLaunchOptions.getFriendlyName())).append("\" xmlns=\"http://www.vmware.com/vcloud/v1\"");
        xml.append(" xmlns:ovf=\"http://schemas.dmtf.org/ovf/envelope/1\">\n");
        xml.append("<Description>").append(vCloud.escapeXml(withLaunchOptions.getDescription())).append("</Description>");
        xml.append("<Source href=\"").append(method.toURL("vAppTemplate", withLaunchOptions.getMachineImageId())).append("\"/>");
        xml.append("</InstantiateVAppTemplateParams>");
        //<InstantiationParams>
        //<NetworkConfigSection>
        //<ovf:Info>Configuration parameters for vAppNetwork</ovf:Info>
        //<NetworkConfig networkName="vAppNetwork">
        //<Configuration>
        //<ParentNetwork href="http://vcloud.example.com/api/v1.0/network/54"/> <FenceMode>bridged</FenceMode>
        //</Configuration>
        //</NetworkConfig>
        //</NetworkConfigSection>
        //</InstantiationParams>
        // TODO: finish this stuff

        NodeList vapps = method.parseXML(method.post(vCloudMethod.INSTANTIATE_VAPP, vdcId, xml.toString())).getElementsByTagName("VApp");


        if( vapps.getLength() < 1 ) {
            throw new CloudException("The instatiation operation succeeded, but no vApp was present");
        }
        Node vapp = vapps.item(0);
        Node href = vapp.getAttributes().getNamedItem("href");

        if( href != null ) {
            String vappId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());
            NodeList tasks = vapp.getChildNodes();

            for( int i=0; i<tasks.getLength(); i++ ) {
                Node task = tasks.item(i);

                if( task.getNodeName().equalsIgnoreCase("Task") ) {
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
            vapps = method.parseXML(x).getElementsByTagName("VApp");
            if( vapps.getLength() < 1 ) {
                throw new CloudException("vApp went away");
            }
            vapp = vapps.item(0);
            NodeList attributes = vapp.getChildNodes();
            String vmId = null;

            for( int i=0; i<attributes.getLength(); i++ ) {
                Node attribute = attributes.item(i);

                if( attribute.getNodeName().equals("Children") && attribute.hasChildNodes() ) {
                    NodeList children = attribute.getChildNodes();

                    for( int j=0; j<children.getLength(); j++ ) {
                        Node vm = children.item(j);

                        if( vm.getNodeName().equalsIgnoreCase("Vm") && vm.hasAttributes() ) {
                            href = vm.getAttributes().getNamedItem("href");
                            if( href != null ) {
                                vmId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());
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
            startVapp(vappId);
            VirtualMachine vm = getVirtualMachine(vmId);

            if( vm == null ) {
                throw new CloudException("Unable to identify vm " + vmId + " in " + vappId);
            }
            return vm;
        }
        throw new CloudException("Unable to identify virtual machine in response");
    }

    @Override
    public @Nonnull Iterable<String> listFirewalls(@Nonnull String vmId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public Iterable<VirtualMachineProduct> listProducts(Architecture architecture) throws InternalException, CloudException {
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

    @Override
    public Iterable<Architecture> listSupportedArchitectures() throws InternalException, CloudException {
        return Collections.singletonList(Architecture.I64);
    }

    @Override
    public @Nonnull Iterable<VirtualMachine> listVirtualMachines() throws InternalException, CloudException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        ArrayList<VirtualMachine> vms = new ArrayList<VirtualMachine>();

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

                                        loadVmsFor(dc.getProviderDataCenterId(), ((vCloud)getProvider()).toID(href.getNodeValue().trim()), vms);
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

    private void loadVmsFor(@Nonnull String vdcId, @Nonnull String id, @Nonnull ArrayList<VirtualMachine> vms) throws InternalException, CloudException {
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
                        VirtualMachine vm = toVirtualMachine(vdcId, vmNode);

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

    @Override
    public void start(@Nonnull String vmId) throws CloudException, InternalException {
        startVapp(vmId);
    }

    private void startVapp(@Nonnull String vappId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vappId);

        if( xml != null ) {
            NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

            for( int i=0; i<nodes.getLength(); i++ ) {
                Node node = nodes.item(i);

                if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
                    Node rel = node.getAttributes().getNamedItem("rel");

                    if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("power:powerOn") ) {
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

    @Override
    public void stop(@Nonnull String vmId, boolean force) throws CloudException, InternalException {
        stop(vmId, force, false);
    }

    private void stop(@Nonnull String vmId, boolean force, boolean wait) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vmId);

        if( xml != null ) {
            NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

            for( int i=0; i<nodes.getLength(); i++ ) {
                Node node = nodes.item(i);

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

    @Override
    public void terminate(@Nonnull String vmId) throws InternalException, CloudException {
        stop(vmId, true, true);
        undeploy(vmId);

        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

        method.delete("vApp", vmId);
    }

    @Override
    public boolean supportsStartStop(@Nonnull VirtualMachine vm) {
        return true;
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

    private @Nullable VirtualMachine toVirtualMachine(@Nonnull String vdcId, @Nonnull Node vmNode) throws CloudException, InternalException {
        Node n = vmNode.getAttributes().getNamedItem("href");
        VirtualMachine vm = new VirtualMachine();

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
            else if( attribute.getNodeName().equalsIgnoreCase("NetworkConnectionSection") && attribute.hasChildNodes() ) {
                NodeList elements = attribute.getChildNodes();
                TreeSet<String> addrs = new TreeSet<String>();

                for( int j=0; j<elements.getLength(); j++ ) {
                    Node element = elements.item(j);

                    if( element.getNodeName().equalsIgnoreCase("NetworkConnection") && element.hasChildNodes() ) {
                        NodeList parts = element.getChildNodes();
                        boolean connected = false;
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
                        if( connected && addr != null ) {
                            addrs.add(addr);
                        }
                    }
                }
                RawAddress[] addresses = new RawAddress[addrs.size()];
                int idx=0;

                for( String addr : addrs ) {
                    addresses[idx++] = new RawAddress(addr);
                }
                // TODO: public v private
                vm.setPrivateAddresses(addresses);
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
        return vm;
    }

    private void undeploy(@Nonnull String vmId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("vApp", vmId);

        if( xml != null ) {
            NodeList nodes = method.parseXML(xml).getElementsByTagName("VApp");

            for( int i=0; i<nodes.getLength(); i++ ) {
                Node node = nodes.item(i);

                if( node.getNodeName().equalsIgnoreCase("Link") && node.hasAttributes() ) {
                    Node rel = node.getAttributes().getNamedItem("rel");

                    if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("undeploy") ) {
                        Node href = node.getAttributes().getNamedItem("href");

                        if( href != null ) {
                            String endpoint = href.getNodeValue().trim();
                            String action = method.getAction(endpoint);
                            StringBuilder payload = new StringBuilder();

                            payload.append("<UndeployVAppParams saveState=\"false\" xmlns=\"http://www.vmware.com/vcloud/v1\"/>");
                            method.waitFor(method.post(action, endpoint, method.getMediaTypeForActionUndeployVApp(), payload.toString()));
                            break;
                        }
                    }
                }
            }
        }
    }
}
