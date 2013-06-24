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
import org.dasein.cloud.AsynchronousTask;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.Tag;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ComputeServices;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageSupport;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VirtualMachine;
import org.dasein.cloud.compute.VirtualMachineSupport;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.util.Cache;
import org.dasein.cloud.util.CacheLevel;
import org.dasein.cloud.vcloud.vCloud;
import org.dasein.cloud.vcloud.vCloudMethod;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.time.Minute;
import org.dasein.util.uom.time.TimePeriod;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.util.TreeSet;

/**
 * Implements vApp Template support in accordance with the Dasein Cloud image support model. Dasein Cloud images map
 * to vApp templates in a vCloud catalog.
 * <p>Created by George Reese: 9/17/12 10:58 AM</p>
 * @author George Reese
 * @version 2013.04 initial version
 * @since 2013.04
 */
public class TemplateSupport implements MachineImageSupport {
    static private final Logger logger = vCloud.getLogger(TemplateSupport.class);

    static public class Catalog {
        public String catalogId;
        public String name;
        public boolean published;
        public String owner;
    }

    private vCloud provider;

    public TemplateSupport(@Nonnull vCloud cloud) {
        this.provider = cloud;
    }

    protected @Nonnull
    ProviderContext getContext() throws CloudException {
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        return ctx;
    }

    protected final @Nonnull  CloudProvider getProvider() {
        return provider;
    }

    protected MachineImage capture(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "captureImageFromVM");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String vmId = options.getVirtualMachineId();

            if( vmId == null ) {
                throw new CloudException("A capture operation requires a valid VM ID");
            }
            VirtualMachine vm = ((vCloud)getProvider()).getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);
            String vAppId = (vm == null ? null : (String)vm.getTag(vAppSupport.PARENT_VAPP_ID));

            if( vm == null ) {
                throw new CloudException("No such virtual machine: " + vmId);
            }
            else if( vAppId == null ) {
                throw new CloudException("Unable to determine virtual machine vApp for capture: " + vmId);
            }
            long timeout = (System.currentTimeMillis() + CalendarWrapper.MINUTE * 10L);

            while( timeout > System.currentTimeMillis() ) {
                if( vm == null ) {
                    throw new CloudException("VM " + vmId + " went away");
                }
                if( !vm.getCurrentState().equals(VmState.PENDING) ) {
                    break;
                }
                try { Thread.sleep(15000L); }
                catch( InterruptedException ignore ) { }
                try { vm = ((vCloud)getProvider()).getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId); }
                catch( Throwable ignore ) { }
            }
            boolean running = !vm.getCurrentState().equals(VmState.STOPPED);
            String vappId = (String)vm.getTag(vAppSupport.PARENT_VAPP_ID);

            if( running ) {
                ((vCloud)getProvider()).getComputeServices().getVirtualMachineSupport().undeploy(vappId);
            }
            try {
                String endpoint = method.toURL("vApp", vAppId);
                StringBuilder xml = new StringBuilder();

                xml.append("<CaptureVAppParams xmlns=\"http://www.vmware.com/vcloud/v1.5\" xmlns:ovf=\"http://schemas.dmtf.org/ovf/envelope/1\" name=\"").append(vCloud.escapeXml(options.getName())).append("\">");
                xml.append("<Description>").append(options.getDescription()).append("</Description>");
                xml.append("<Source href=\"").append(endpoint).append("\" type=\"").append(method.getMediaTypeForVApp()).append("\"/>");
                xml.append("</CaptureVAppParams>");

                String response = method.post(vCloudMethod.CAPTURE_VAPP, vm.getProviderDataCenterId(), xml.toString());

                if( response.equals("") ) {
                    throw new CloudException("No error or other information was in the response");
                }
                Document doc = method.parseXML(response);

                try {
                    method.checkError(doc);
                }
                catch( CloudException e ) {
                    if( e.getMessage().contains("Stop the vApp and try again") ) {
                        logger.warn("The cloud thinks the vApp or VM is still running; going to check what's going on: " + e.getMessage());
                        vm = ((vCloud)getProvider()).getComputeServices().getVirtualMachineSupport().getVirtualMachine(vmId);
                        if( vm == null ) {
                            throw new CloudException("Virtual machine went away");
                        }
                        if( !vm.getCurrentState().equals(VmState.STOPPED) ) {
                            logger.warn("Current state of VM: " + vm.getCurrentState());
                            ((vCloud)getProvider()).getComputeServices().getVirtualMachineSupport().undeploy(vappId);
                        }
                        response = method.post(vCloudMethod.CAPTURE_VAPP, vm.getProviderDataCenterId(), xml.toString());
                        if( response.equals("") ) {
                            throw new CloudException("No error or other information was in the response");
                        }
                        doc = method.parseXML(response);
                        method.checkError(doc);
                    }
                    else {
                        throw e;
                    }
                }

                NodeList vapps = doc.getElementsByTagName("VAppTemplate");

                if( vapps.getLength() < 1 ) {
                    throw new CloudException("No vApp templates were found in response");
                }
                Node vapp = vapps.item(0);
                String imageId = null;
                Node href = vapp.getAttributes().getNamedItem("href");

                if( href != null ) {
                    imageId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());
                }
                if( imageId == null || imageId.length() < 1 ) {
                    throw new CloudException("No imageId was found in response");
                }
                MachineImage img = new MachineImage();

                img.setName(options.getName());
                img.setDescription(options.getDescription());
                img.setProviderMachineImageId(imageId);
                img = loadVapp(img);
                if( img == null ) {
                    throw new CloudException("Image was lost");
                }
                method.waitFor(response);
                publish(img);
                return img;
            }
            finally {
                if( running ) {
                    ((vCloud)getProvider()).getComputeServices().getVirtualMachineSupport().deploy(vappId);
                }
            }
        }
        finally {
            APITrace.end();
        }
    }

    private void publish(@Nonnull MachineImage img) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        Catalog c = null;

        for( Catalog catalog : listPrivateCatalogs() ) {
            if( catalog.owner.equals(getContext().getAccountNumber()) ) {
                c = catalog;
                if( catalog.name.equals("Standard Catalog") ) {
                    break;
                }
            }
        }
        StringBuilder xml;

        if( c == null ) {
            xml = new StringBuilder();
            xml.append("<AdminCatalog xmlns=\"http://www.vmware.com/vcloud/v1.5\" name=\"Standard Catalog\">");
            xml.append("<Description>Standard catalog for custom vApp templates</Description>");
            xml.append("<IsPublished>false</IsPublished>");
            xml.append("</AdminCatalog>");
            String response = method.post("createCatalog", method.toAdminURL("org", getContext().getRegionId()) + "/catalogs", method.getMediaTypeForActionAddCatalog(), xml.toString());
            String href = null;

            method.waitFor(response);
            if( !response.equals("") ) {
                Document doc = method.parseXML(response);
                String docElementTagName = doc.getDocumentElement().getTagName();
                String nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList matches = doc.getElementsByTagName(nsString + "AdminCatalog");

                for( int i=0; i<matches.getLength(); i++ ) {
                    Node m = matches.item(i);
                    Node h = m.getAttributes().getNamedItem("href");

                    if( h != null ) {
                        href = h.getNodeValue().trim();
                        break;
                    }
                }
            }
            if( href == null ) {
                throw new CloudException("No catalog could be identified for publishing vApp template " + img.getProviderMachineImageId());
            }
            c = getCatalog(false, href);
            if( c == null ) {
                throw new CloudException("No catalog could be identified for publishing vApp template " + img.getProviderMachineImageId());
            }
        }

        xml = new StringBuilder();
        xml.append("<CatalogItem xmlns=\"http://www.vmware.com/vcloud/v1.5\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" ");
        xml.append("name=\"").append(vCloud.escapeXml(img.getName())).append("\">");
        xml.append("<Description>").append(vCloud.escapeXml(img.getDescription())).append("</Description>");
        xml.append("<Entity href=\"").append(method.toURL("vAppTemplate", img.getProviderMachineImageId())).append("\" ");
        xml.append("name=\"").append(vCloud.escapeXml(img.getName())).append("\" ");
        xml.append("type=\"").append(method.getMediaTypeForVAppTemplate()).append("\" xsi:type=\"").append("ResourceReferenceType\"/>");
        xml.append("</CatalogItem>");

        method.waitFor(method.post("publish", method.toURL("catalog", c.catalogId) + "/catalogItems", method.getMediaTypeForCatalogItem(), xml.toString()));
    }

    private @Nullable Catalog getCatalog(boolean published, @Nonnull String href) throws CloudException, InternalException {
        String catalogId = ((vCloud)getProvider()).toID(href);
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("catalog", catalogId);

        if( xml == null ) {
            logger.warn("Unable to find catalog " + catalogId + " indicated by org " + getContext().getAccountNumber());
            return null;
        }
        Document doc = method.parseXML(xml);
        String docElementTagName = doc.getDocumentElement().getTagName();
        String nsString = "";
        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
        NodeList cNodes = doc.getElementsByTagName(nsString + "Catalog");

        for( int i=0; i<cNodes.getLength(); i++ ) {
            Node cnode = cNodes.item(i);

            Node name = cnode.getAttributes().getNamedItem("name");
            String catalogName = null;

            if( name != null ) {
                catalogName = name.getNodeValue().trim();
            }
            if( cnode.hasChildNodes() ) {
                NodeList attributes = cnode.getChildNodes();
                String owner = "--public--";
                boolean p = false;

                for( int j=0; j<attributes.getLength(); j++ ) {
                    Node attribute = attributes.item(j);
                    if(attribute.getNodeName().contains(":"))nsString = attribute.getNodeName().substring(0, attribute.getNodeName().indexOf(":") + 1);
                    else nsString = "";

                    if( attribute.getNodeName().equalsIgnoreCase(nsString + "IsPublished") ) {
                        p = (attribute.hasChildNodes() && attribute.getFirstChild().getNodeValue().trim().equalsIgnoreCase("true"));
                    }
                    else if( attribute.getNodeName().equalsIgnoreCase(nsString + "Link") && attribute.hasAttributes() ) {
                        Node rel = attribute.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("up") ) {
                            Node type = attribute.getAttributes().getNamedItem("type");

                            if( type != null && type.getNodeValue().trim().equalsIgnoreCase(method.getMediaTypeForOrg()) ) {
                                Node h = attribute.getAttributes().getNamedItem("href");

                                if( h != null ) {
                                    owner = method.getOrgName(h.getNodeValue().trim());
                                }
                            }
                        }
                    }
                }
                if( p == published ) {
                    Catalog catalog = new Catalog();
                    catalog.catalogId = ((vCloud)getProvider()).toID(href);
                    catalog.published = p;
                    catalog.owner = owner;
                    catalog.name = catalogName;
                    return catalog;
                }
            }

        }
        return null;
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "getImage");
        try {
            for( MachineImage image : listImages(null) ) {
                if( image.getProviderMachineImageId().equals(providerImageId) ) {
                    return image;
                }
            }
            for( MachineImage image : searchPublicImages(null, null, null) ) {
                if( image.getProviderMachineImageId().equals(providerImageId) ) {
                    return image;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return "vApp Template";
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "isImageSharedWithPublic");
        try {
            MachineImage img = getImage(machineImageId);

            if( img == null ) {
                return false;
            }
            Boolean p = (Boolean)img.getTag("public");

            return (p != null && p);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "isSubscribedImage");
        try {
            return (getProvider().testContext() != null);
        }
        finally {
            APITrace.end();
        }
    }

    private Iterable<Catalog> listPublicCatalogs() throws CloudException, InternalException {
        Cache<Catalog> cache = Cache.getInstance(getProvider(), "publicCatalogs", Catalog.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(30, TimePeriod.MINUTE));
        Iterable<Catalog> catalogs = cache.get(getContext());

        if( catalogs == null ) {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("org", getContext().getRegionId());

            if( xml == null ) {
                catalogs = Collections.emptyList();
            }
            else {
                ArrayList<Catalog> list = new ArrayList<Catalog>();
                Document doc = method.parseXML(xml);
                String docElementTagName = doc.getDocumentElement().getTagName();
                String nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList links = doc.getElementsByTagName(nsString + "Link");

                for( int i=0; i<links.getLength(); i++ ) {
                    Node link = links.item(i);

                    if( link.hasAttributes() ) {
                        Node rel = link.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("down") ) {
                            Node type = link.getAttributes().getNamedItem("type");

                            if( type != null && type.getNodeValue().trim().equals(method.getMediaTypeForCatalog()) ) {
                                Node href = link.getAttributes().getNamedItem("href");
                                Catalog c = getCatalog(true, href.getNodeValue().trim());

                                if( c != null ) {
                                    list.add(c);
                                }
                            }
                        }
                    }
                }
                catalogs = list;
            }
            cache.put(getContext(), catalogs);
        }
        return catalogs;
    }

    private Iterable<Catalog> listPrivateCatalogs() throws CloudException, InternalException {
        Cache<Catalog> cache = Cache.getInstance(getProvider(), "privateCatalogs", Catalog.class, CacheLevel.REGION_ACCOUNT, new TimePeriod<Minute>(30, TimePeriod.MINUTE));
        Iterable<Catalog> catalogs = cache.get(getContext());

        if( catalogs == null ) {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String xml = method.get("org", getContext().getRegionId());

            if( xml == null ) {
                catalogs = Collections.emptyList();
            }
            else {
                ArrayList<Catalog> list = new ArrayList<Catalog>();
                Document doc = method.parseXML(xml);
                String docElementTagName = doc.getDocumentElement().getTagName();
                String nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList links = doc.getElementsByTagName(nsString + "Link");

                for( int i=0; i<links.getLength(); i++ ) {
                    Node link = links.item(i);

                    if( link.hasAttributes() ) {
                        Node rel = link.getAttributes().getNamedItem("rel");

                        if( rel != null && rel.getNodeValue().trim().equalsIgnoreCase("down") ) {
                            Node type = link.getAttributes().getNamedItem("type");

                            if( type != null && type.getNodeValue().trim().equals(method.getMediaTypeForCatalog()) ) {
                                Node href = link.getAttributes().getNamedItem("href");
                                Catalog c = getCatalog(false, href.getNodeValue().trim());

                                if( c != null ) {
                                    list.add(c);
                                }
                            }
                        }
                    }
                }
                catalogs = list;
            }
            cache.put(getContext(), catalogs);
        }
        return catalogs;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageClass cls) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "listImages");
        try {
            if( cls != null && !cls.equals(ImageClass.MACHINE) ) {
                return Collections.emptyList();
            }
            ArrayList<MachineImage> images = new ArrayList<MachineImage>();

            for( Catalog catalog : listPrivateCatalogs() ) {
                vCloudMethod method = new vCloudMethod((vCloud)getProvider());
                String xml = method.get("catalog", catalog.catalogId);

                if( xml == null ) {
                    logger.warn("Unable to find catalog " + catalog.catalogId + " indicated by org " + getContext().getAccountNumber());
                    continue;
                }
                Document doc = method.parseXML(xml);
                String docElementTagName = doc.getDocumentElement().getTagName();
                String nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList cNodes = doc.getElementsByTagName(nsString + "Catalog");

                for( int i=0; i<cNodes.getLength(); i++ ) {
                    Node cnode = cNodes.item(i);

                    if( cnode.hasChildNodes() ) {
                        NodeList items = cnode.getChildNodes();

                        for( int j=0; j<items.getLength(); j++ ) {
                            Node wrapper = items.item(j);
                            if(wrapper.getNodeName().contains(":"))nsString = wrapper.getNodeName().substring(0, wrapper.getNodeName().indexOf(":") + 1);
                            else nsString = "";

                            if( wrapper.getNodeName().equalsIgnoreCase(nsString + "CatalogItems") && wrapper.hasChildNodes() ) {
                                NodeList entries = wrapper.getChildNodes();

                                for( int k=0; k<entries.getLength(); k++ ) {
                                    Node item = entries.item(k);
                                    if(item.getNodeName().contains(":"))nsString = item.getNodeName().substring(0, item.getNodeName().indexOf(":") + 1);
                                    else nsString = "";

                                    if( item.getNodeName().equalsIgnoreCase(nsString + "CatalogItem") && item.hasAttributes() ) {
                                        Node href = item.getAttributes().getNamedItem("href");

                                        if( href != null ) {
                                            MachineImage image = loadTemplate(((vCloud)getProvider()).toID(href.getNodeValue().trim()));

                                            if( image != null ) {
                                                image.setProviderOwnerId(catalog.owner);
                                                images.add(image);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                }
            }
            return images;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.VMDK);
    }

    @Override
    public @Nonnull Iterable<String> listShares(@Nonnull String forMachineImageId) throws CloudException, InternalException {
        return Collections.emptyList();
    }

    private @Nullable MachineImage loadTemplate(@Nonnull String catalogItemId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        String xml = method.get("catalogItem", catalogItemId);

        if( xml == null ) {
            logger.warn("Catalog item " + catalogItemId + " is missing from the catalog");
            return null;
        }
        Document doc = method.parseXML(xml);
        String docElementTagName = doc.getDocumentElement().getTagName();
        String nsString = "";
        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
        NodeList items = doc.getElementsByTagName(nsString + "CatalogItem");

        if( items.getLength() < 1 ) {
            return null;
        }
        Node item = items.item(0);

        if( item.hasAttributes() && item.hasChildNodes() ) {
            MachineImage image = new MachineImage();
            Node name = item.getAttributes().getNamedItem("name");

            if( name != null ) {
                String n = name.getNodeValue().trim();

                if( n.length() > 0 ) {
                    image.setName(n);
                    image.setDescription(n);
                }
            }
            NodeList entries = item.getChildNodes();
            String vappId = null;

            for( int i=0; i<entries.getLength(); i++ ) {
                Node entry = entries.item(i);

                if( entry.getNodeName().equalsIgnoreCase(nsString + "description") && entry.hasChildNodes() ) {
                    String d = entry.getFirstChild().getNodeValue().trim();

                    if( d.length() > 0 ) {
                        image.setDescription(d);
                        if( image.getName() == null ) {
                            image.setName(d);
                        }
                    }
                }
                else if( entry.getNodeName().equalsIgnoreCase(nsString + "entity") && entry.hasAttributes() ) {
                    Node href = entry.getAttributes().getNamedItem("href");

                    if( href != null ) {
                        vappId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());
                    }
                }
            }
            if( vappId != null ) {
                image.setProviderMachineImageId(vappId);
                return loadVapp(image);
            }
        }
        return null;
    }

    private @Nullable MachineImage loadVapp(@Nonnull MachineImage image) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());

        String xml = method.get("vAppTemplate", image.getProviderMachineImageId());

        if( xml == null ) {
            return null;
        }
        Document doc = method.parseXML(xml);
        String docElementTagName = doc.getDocumentElement().getTagName();
        String nsString = "";
        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
        NodeList templates = doc.getElementsByTagName(nsString + "VAppTemplate");

        if( templates.getLength() < 1 ) {
            return null;
        }
        Node template = templates.item(0);
        TreeSet<String> childVms = new TreeSet<String>();

        if( image.getName() == null ) {
            Node node = template.getAttributes().getNamedItem("name");

            if( node != null ) {
                String n = node.getNodeValue().trim();

                if( n.length() > 0 ) {
                    image.setName(n);
                    if( image.getDescription() == null ) {
                        image.setDescription(n);
                    }
                }
            }
        }
        NodeList attributes = template.getChildNodes();

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);

            if(attribute.getNodeName().contains(":")){
                nsString = attribute.getNodeName().substring(0, attribute.getNodeName().indexOf(":") + 1);
            }
            else{
                nsString="";
            }

            if( attribute.getNodeName().equalsIgnoreCase(nsString + "description") && image.getDescription() == null && attribute.hasChildNodes() ) {
                String d = attribute.getFirstChild().getNodeValue().trim();

                if( d.length() > 0 ) {
                    image.setDescription(d);
                    if( image.getName() == null ) {
                        image.setName(d);
                    }
                }
            }
            else if( attribute.getNodeName().equalsIgnoreCase(nsString + "children") && attribute.hasChildNodes() ) {
                NodeList children = attribute.getChildNodes();

                for( int j=0; j<children.getLength(); j++ ) {
                    Node child = children.item(j);

                    if( child.getNodeName().equalsIgnoreCase(nsString + "vm") && child.hasChildNodes() ) {
                        Node childHref = child.getAttributes().getNamedItem("href");

                        if( childHref != null ) {
                            childVms.add(((vCloud)getProvider()).toID(childHref.getNodeValue().trim()));
                        }
                        NodeList vmAttrs = child.getChildNodes();

                        for( int k=0; k<vmAttrs.getLength(); k++ ) {
                            Node vmAttr = vmAttrs.item(k);
                            if(vmAttr.getNodeName().contains(":"))nsString = vmAttr.getNodeName().substring(0, vmAttr.getNodeName().indexOf(":") + 1);

                            if( vmAttr.getNodeName().equalsIgnoreCase(nsString + "guestcustomizationsection") && vmAttr.hasChildNodes() ) {
                                NodeList custList = vmAttr.getChildNodes();

                                for( int l=0; l<custList.getLength(); l++ ) {
                                    Node cust = custList.item(l);

                                    if( cust.getNodeName().equalsIgnoreCase(nsString + "computername") && cust.hasChildNodes() ) {
                                        String n = cust.getFirstChild().getNodeValue().trim();

                                        if( n.length() > 0 ) {
                                            if( image.getName() == null ) {
                                                image.setName(n);
                                            }
                                            else {
                                                image.setName(image.getName() + " - " + n);
                                            }
                                        }
                                    }
                                }
                            }
                            else if( vmAttr.getNodeName().equalsIgnoreCase(nsString + "ProductSection") && vmAttr.hasChildNodes() ) {
                                NodeList prdList = vmAttr.getChildNodes();

                                for( int l=0; l<prdList.getLength(); l++ ) {
                                    Node prd = prdList.item(l);

                                    if( prd.getNodeName().equalsIgnoreCase(nsString + "Product") && prd.hasChildNodes() ) {
                                        String n = prd.getFirstChild().getNodeValue().trim();

                                        if( n.length() > 0 ) {
                                            image.setPlatform(Platform.guess(n));
                                        }
                                    }
                                }
                            }
                            else if( vmAttr.getNodeName().equalsIgnoreCase(nsString + "OperatingSystemSection") && vmAttr.hasChildNodes() ) {
                                NodeList os = vmAttr.getChildNodes();

                                for( int l=0; l<os.getLength(); l++ ) {
                                    Node osdesc = os.item(l);

                                    if( osdesc.getNodeName().equalsIgnoreCase(nsString + "Description") && osdesc.hasChildNodes() ) {
                                        String desc = osdesc.getFirstChild().getNodeValue();

                                        image.setPlatform(Platform.guess(desc));

                                        if( desc.contains("32") || (desc.contains("x86") && !desc.contains("64")) ) {
                                            image.setArchitecture(Architecture.I32);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if (attribute.getNodeName().equalsIgnoreCase(nsString + "LeaseSettingsSection") && attribute.hasChildNodes()){
            	if (logger.isTraceEnabled()){
            		logger.trace("Checking lease settings for VAppTemplate : " +  image.getName());
            	}
            	NodeList children = attribute.getChildNodes();
                for( int j=0; j<children.getLength(); j++ ) {
                    Node child = children.item(j);
                    if( child.getNodeName().equalsIgnoreCase(nsString + "StorageLeaseExpiration") && child.hasChildNodes() ) {
                    	String expiryDateString = child.getFirstChild().getNodeValue().trim();
                    	Date expiryDate = vCloud.parseIsoDate(expiryDateString);
                    	if (expiryDate != null){
                    		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                    		if (cal.getTimeInMillis() > expiryDate.getTime()){
                    			if (logger.isTraceEnabled()){
                    				logger.trace("vAppTemplate " + image.getName() + " has an expired storage lease.");
                    			}
                    			return null;
                    		}
                    	}
                    }
                }
                
            }
        }
        if( image.getName() == null ) {
            image.setName(image.getProviderMachineImageId());
        }
        if( image.getDescription() == null ) {
            image.setDescription(image.getName());
        }
        Platform p = image.getPlatform();

        if( p == null || p.equals(Platform.UNKNOWN) ) {
            image.setPlatform(Platform.guess(image.getName() + " " + image.getDescription()));
        }
        image.setArchitecture(Architecture.I64);
        image.setProviderRegionId(getContext().getRegionId());
        image.setSoftware("");
        image.setType(MachineImageType.VOLUME);
        image.setCurrentState(MachineImageState.ACTIVE);
        image.setImageClass(ImageClass.MACHINE);
        StringBuilder ids = new StringBuilder();

        for( String id : childVms ) {
            if( ids.length() > 0 ) {
                ids.append(",");
            }
            ids.append(id);
        }
        image.setTag("childVirtualMachineIds", ids.toString());
        return image;
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "removeImage");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());

            method.delete("vAppTemplate", providerImageId);
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchPublicImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass ... imageClasses) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "searchPublicImages");
        try {
            if( imageClasses != null ) {
                boolean ok = false;

                for( ImageClass cls : imageClasses ) {
                    if( cls.equals(ImageClass.MACHINE) ) {
                        ok = true;
                    }
                }
                if( !ok ) {
                    return Collections.emptyList();
                }
            }
            ArrayList<MachineImage> images = new ArrayList<MachineImage>();

            for( Catalog catalog : listPublicCatalogs() ) {
                vCloudMethod method = new vCloudMethod((vCloud)getProvider());
                String xml = method.get("catalog", catalog.catalogId);

                if( xml == null ) {
                    logger.warn("Unable to find catalog " + catalog.catalogId + " indicated by org " + getContext().getAccountNumber());
                    continue;
                }
                Document doc = method.parseXML(xml);
                String docElementTagName = doc.getDocumentElement().getTagName();
                String nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList cNodes = doc.getElementsByTagName(nsString + "Catalog");

                for( int i=0; i<cNodes.getLength(); i++ ) {
                    Node cnode = cNodes.item(i);

                    if( cnode.hasChildNodes() ) {
                        NodeList items = cnode.getChildNodes();

                        for( int j=0; j<items.getLength(); j++ ) {
                            Node wrapper = items.item(j);
                            if(wrapper.getNodeName().contains(":"))nsString = wrapper.getNodeName().substring(0, wrapper.getNodeName().indexOf(":") + 1);
                            else nsString = "";

                            if( wrapper.getNodeName().equalsIgnoreCase(nsString + "CatalogItems") && wrapper.hasChildNodes() ) {
                                NodeList entries = wrapper.getChildNodes();

                                for( int k=0; k<entries.getLength(); k++ ) {
                                    Node item = entries.item(k);
                                    if(item.getNodeName().contains(":"))nsString = item.getNodeName().substring(0, item.getNodeName().indexOf(":") + 1);
                                    else nsString = "";

                                    if( item.getNodeName().equalsIgnoreCase(nsString + "CatalogItem") && item.hasAttributes() ) {
                                        Node href = item.getAttributes().getNamedItem("href");

                                        if( href != null ) {
                                            MachineImage image = loadTemplate(((vCloud)getProvider()).toID(href.getNodeValue().trim()));

                                            if( image != null ) {
                                                image.setProviderOwnerId(catalog.owner);
                                                if( matches(image, keyword, platform, architecture, imageClasses) ) {
                                                    images.add(image);
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
            return images;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean supportsCustomImages() {
        return true;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return type.equals(MachineImageType.VOLUME);
    }

    protected boolean matches(@Nonnull MachineImage image, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass ... classes) {
        if( architecture != null && !architecture.equals(image.getArchitecture()) ) {
            return false;
        }
        if( classes != null && classes.length > 0 ) {
            boolean matches = false;

            for( ImageClass cls : classes ) {
                if( cls.equals(image.getImageClass()) ) {
                    matches = true;
                    break;
                }
            }
            if( !matches ) {
                return false;
            }
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
    public void addImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Support for image sharing is not currently implemented");
    }

    @Override
    public void addPublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Support for image sharing is not currently implemented");
    }

    @Override
    public @Nonnull String bundleVirtualMachine(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image bundling is not currently implemented");
    }

    @Override
    public void bundleVirtualMachineAsync(@Nonnull String virtualMachineId, @Nonnull MachineImageFormat format, @Nonnull String bucket, @Nonnull String name, @Nonnull AsynchronousTask<String> trackingTask) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image bundling is not currently implemented");
    }

    @Override
    public final @Nonnull MachineImage captureImage(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        boolean supported = false;

        for( MachineImageType type : MachineImageType.values() ) {
            if( supportsImageCapture(type) ) {
                supported = true;
            }
        }
        if( !supported ) {
            throw new OperationNotSupportedException("Image capture is not supported in " + getProvider().getCloudName());
        }
        return capture(options);
    }

    @Override
    public final void captureImageAsync(final @Nonnull ImageCreateOptions options, final @Nonnull AsynchronousTask<MachineImage> taskTracker) throws CloudException, InternalException {
        boolean supported = false;

        for( MachineImageType type : MachineImageType.values() ) {
            if( supportsImageCapture(type) ) {
                supported = true;
            }
        }
        if( !supported ) {
            throw new OperationNotSupportedException("Image capture is not supported in " + getProvider().getCloudName());
        }
        getProvider().hold();
        Thread t = new Thread() {
            public void run() {
                try {
                    MachineImage img = capture(options);

                    if( !taskTracker.isComplete() ) {
                        taskTracker.completeWithResult(img);
                    }
                }
                catch( Throwable t ) {
                    taskTracker.complete(t);
                }
                finally {
                    getProvider().release();
                }
            }
        };

        t.setName("Capture of " + options.getVirtualMachineId() + " in " + getProvider().getCloudName());
        t.setDaemon(true);
        t.start();
    }

    @Override
    public final @Nullable MachineImage getMachineImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        return getImage(providerImageId);
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale) {
        return getProviderTermForImage(locale, ImageClass.MACHINE);
    }

    @Override
    public @Nonnull String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return getProviderTermForImage(locale, cls);
    }

    @Override
    public boolean hasPublicLibrary() {
        try {
            return supportsPublicLibrary(ImageClass.MACHINE);
        }
        catch( Throwable t ) {
            throw new RuntimeException(t);
        }
    }

    @Override
    public @Nonnull
    Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public final @Nonnull AsynchronousTask<String> imageVirtualMachine(@Nonnull String vmId, @Nonnull String name, @Nonnull String description) throws CloudException, InternalException {
        ComputeServices services = getProvider().getComputeServices();

        if( services == null ) {
            throw new CloudException("No virtual machine " + vmId + " exists to image in this cloud");
        }
        VirtualMachineSupport support = services.getVirtualMachineSupport();

        if( support == null ) {
            throw new CloudException("No virtual machine " + vmId + " exists to image in this cloud");
        }
        VirtualMachine vm = support.getVirtualMachine(vmId);

        if( vm == null ) {
            throw new CloudException("No virtual machine " + vmId + " exists to image in this cloud");
        }

        final ImageCreateOptions options = ImageCreateOptions.getInstance(vm, name, description);
        final AsynchronousTask<String> task = new AsynchronousTask<String>();

        getProvider().hold();
        Thread t = new Thread() {
            public void run() {
                try {
                    task.completeWithResult(capture(options).getProviderMachineImageId());
                }
                catch( Throwable t ) {
                    task.complete(t);
                }
                finally {
                    getProvider().release();
                }
            }
        };

        t.setName("Capture Image from " + vm.getProviderVirtualMachineId() + " in " + getProvider().getCloudName());
        t.setDaemon(true);
        t.start();

        return task;
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listImageStatus(@Nonnull ImageClass cls) throws CloudException, InternalException {
        ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

        for( MachineImage img : listImages(ImageClass.MACHINE) ) {
            status.add(new ResourceStatus(img.getProviderMachineImageId(), img.getCurrentState()));
        }
        return status;
    }

    @Override
    public @Nonnull Iterable<MachineImage> listImages(@Nonnull ImageClass cls, @Nonnull String ownedBy) throws CloudException, InternalException {
        ArrayList<MachineImage> images = new ArrayList<MachineImage>();

        for( MachineImage img : listImages(ImageClass.MACHINE) ) {
            if( ownedBy.equals(img.getProviderOwnerId()) ) {
                images.add(img);
            }
        }
        return images;
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImages() throws CloudException, InternalException {
        return listImages(ImageClass.MACHINE);
    }

    @Override
    public @Nonnull Iterable<MachineImage> listMachineImagesOwnedBy(@Nullable String accountId) throws CloudException, InternalException {
        if( accountId == null ) {
            return listImages(ImageClass.MACHINE);
        }
        else {
            return listImages(ImageClass.MACHINE, accountId);
        }
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
    public @Nonnull String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];
    }

    @Override
    public @Nonnull MachineImage registerImageBundle(@Nonnull ImageCreateOptions options) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image bundling is not currently implemented");
    }

    @Override
    public void remove(@Nonnull String providerImageId) throws CloudException, InternalException {
        remove(providerImageId, false);
    }

    @Override
    public void removeAllImageShares(@Nonnull String providerImageId) throws CloudException, InternalException {
        // NO-OP (does not error even when not supported)
    }

    @Override
    public void removeImageShare(@Nonnull String providerImageId, @Nonnull String accountNumber) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not currently implemented");
    }

    @Override
    public void removePublicShare(@Nonnull String providerImageId) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image sharing is not currently supported");
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchImages(@Nullable String accountNumber, @Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture, @Nullable ImageClass ... imageClasses) throws CloudException, InternalException {
        ArrayList<MachineImage> matches = new ArrayList<MachineImage>();

        if( imageClasses == null || imageClasses.length < 2 ) {
            for( MachineImage img : listImages(null) ) {
                if( matches(img, keyword, platform, architecture) ) {
                    if( accountNumber != null && accountNumber.equals(img.getProviderOwnerId()) ) {
                        matches.add(img);
                    }
                }
            }
        }
        else {
            for( ImageClass cls : imageClasses ) {
                for( MachineImage img : listImages(cls) ) {
                    if( matches(img, keyword, platform, architecture) ) {
                        if( accountNumber != null && accountNumber.equals(img.getProviderOwnerId()) ) {
                            matches.add(img);
                        }
                    }
                }
            }
        }
        return matches;
    }

    @Override
    public @Nonnull Iterable<MachineImage> searchMachineImages(@Nullable String keyword, @Nullable Platform platform, @Nullable Architecture architecture) throws CloudException, InternalException {
        ArrayList<MachineImage> matches = new ArrayList<MachineImage>();

        for( MachineImage img : searchImages(null, keyword, platform, architecture, ImageClass.MACHINE) ) {
            matches.add(img);
        }
        for( MachineImage img : searchPublicImages(keyword, platform, architecture, ImageClass.MACHINE) ) {
            if( !matches.contains(img) ) {
                matches.add(img);
            }
        }
        return matches;
    }

    @Override
    public final void shareMachineImage(@Nonnull String providerImageId, @Nullable String withAccountId, boolean allow) throws CloudException, InternalException {
        if( withAccountId == null ) {
            if( allow ) {
                addPublicShare(providerImageId);
            }
            else {
                removePublicShare(providerImageId);
            }
        }
        else if( allow ) {
            addImageShare(providerImageId, withAccountId);
        }
        else {
            removeImageShare(providerImageId, withAccountId);
        }
    }

    @Override
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageSharing() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageSharingWithPublic() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsPublicLibrary(@Nonnull ImageClass cls) throws CloudException, InternalException {
        return cls.equals(ImageClass.MACHINE);
    }

    @Override
    public void updateTags(@Nonnull String imageId, @Nonnull Tag... tags) throws CloudException, InternalException {
        // NO-OP
    }
}
