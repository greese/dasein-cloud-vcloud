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
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.compute.AbstractImageSupport;
import org.dasein.cloud.compute.Architecture;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.ImageCreateOptions;
import org.dasein.cloud.compute.ImageFilterOptions;
import org.dasein.cloud.compute.MachineImage;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageState;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.Platform;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * Implements vApp Template support in accordance with the Dasein Cloud image support model. Dasein Cloud images map
 * to vApp templates in a vCloud catalog.
 * <p>Created by George Reese: 9/17/12 10:58 AM</p>
 * @author George Reese
 * @version 2013.04 initial version
 * @since 2013.04
 */
public class TemplateSupport extends AbstractImageSupport {
    static private final Logger logger = vCloud.getLogger(TemplateSupport.class);

    static public class Catalog {
        public String catalogId;
        public boolean published;
        public String owner;
    }

    public TemplateSupport(@Nonnull vCloud cloud) {
        super(cloud);
    }

    @Override
    protected MachineImage capture(@Nonnull ImageCreateOptions options, @Nullable AsynchronousTask<MachineImage> task) throws CloudException, InternalException {
        throw new OperationNotSupportedException("Image capture is not currently implemented");
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
        NodeList cNodes = doc.getElementsByTagName("Catalog");

        for( int i=0; i<cNodes.getLength(); i++ ) {
            Node cnode = cNodes.item(i);

            if( cnode.hasChildNodes() ) {
                NodeList attributes = cnode.getChildNodes();
                String owner = "--public--";
                boolean p = false;

                for( int j=0; j<attributes.getLength(); j++ ) {
                    Node attribute = attributes.item(j);

                    if( attribute.getNodeName().equalsIgnoreCase("IsPublished") ) {
                        p = (attribute.hasChildNodes() && attribute.getFirstChild().getNodeValue().trim().equalsIgnoreCase("true"));
                    }
                    else if( attribute.getNodeName().equalsIgnoreCase("Link") && attribute.hasAttributes() ) {
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
                    catalog.catalogId = href;
                    catalog.published = p;
                    catalog.owner = owner;
                    return catalog;
                }
            }

        }
        return null;
    }

    @Override
    public MachineImage getImage(@Nonnull String providerImageId) throws CloudException, InternalException {
        for( MachineImage image : listImages((ImageFilterOptions)null) ) {
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

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass cls) {
        return "vApp Template";
    }

    @Override
    public boolean isImageSharedWithPublic(@Nonnull String machineImageId) throws CloudException, InternalException {
        MachineImage img = getImage(machineImageId);

        if( img == null ) {
            return false;
        }
        Boolean p = (Boolean)img.getTag("public");

        return (p != null && p);
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        return (getProvider().testContext() != null);
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
                NodeList links = doc.getElementsByTagName("Link");

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
                NodeList links = doc.getElementsByTagName("Link");

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
    public @Nonnull Iterable<MachineImage> listImages(@Nullable ImageFilterOptions options) throws CloudException, InternalException {
        APITrace.begin(getProvider(), "listImages");
        try {
            ImageClass cls = (options == null ? null : options.getImageClass());

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
                NodeList cNodes = doc.getElementsByTagName("Catalog");

                for( int i=0; i<cNodes.getLength(); i++ ) {
                    Node cnode = cNodes.item(i);

                    if( cnode.hasChildNodes() ) {
                        NodeList items = cnode.getChildNodes();

                        for( int j=0; j<items.getLength(); j++ ) {
                            Node wrapper = items.item(j);

                            if( wrapper.getNodeName().equalsIgnoreCase("CatalogItems") && wrapper.hasChildNodes() ) {
                                NodeList entries = wrapper.getChildNodes();

                                for( int k=0; k<entries.getLength(); k++ ) {
                                    Node item = entries.item(k);

                                    if( item.getNodeName().equalsIgnoreCase("CatalogItem") && item.hasAttributes() ) {
                                        Node href = item.getAttributes().getNamedItem("href");

                                        if( href != null ) {
                                            MachineImage image = loadTemplate(((vCloud)getProvider()).toID(href.getNodeValue().trim()));

                                            if( image != null ) {
                                                image.setProviderOwnerId(catalog.owner);
                                                if( options == null || options.matches(image) ) {
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
        NodeList items = doc.getElementsByTagName("CatalogItem");

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

                if( entry.getNodeName().equalsIgnoreCase("description") && entry.hasChildNodes() ) {
                    String d = entry.getFirstChild().getNodeValue().trim();

                    if( d.length() > 0 ) {
                        image.setDescription(d);
                        if( image.getName() == null ) {
                            image.setName(d);
                        }
                    }
                }
                else if( entry.getNodeName().equalsIgnoreCase("datecreated") && entry.hasChildNodes() ) {
                    image.setCreationTimestamp(((vCloud)getProvider()).parseTime(entry.getFirstChild().getNodeValue().trim()));
                }
                else if( entry.getNodeName().equalsIgnoreCase("entity") && entry.hasAttributes() ) {
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
        NodeList templates = doc.getElementsByTagName("VAppTemplate");

        if( templates.getLength() < 1 ) {
            return null;
        }
        Node template = templates.item(0);

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

            if( attribute.getNodeName().equalsIgnoreCase("description") && image.getDescription() == null && attribute.hasChildNodes() ) {
                String d = attribute.getFirstChild().getNodeValue().trim();

                if( d.length() > 0 ) {
                    image.setDescription(d);
                    if( image.getName() == null ) {
                        image.setName(d);
                    }
                }
            }
            else if( attribute.getNodeName().equalsIgnoreCase("children") && attribute.hasChildNodes() ) {
                NodeList children = attribute.getChildNodes();

                for( int j=0; j<children.getLength(); j++ ) {
                    Node child = children.item(j);

                    if( child.getNodeName().equalsIgnoreCase("vm") && child.hasChildNodes() ) {
                        NodeList vmAttrs = child.getChildNodes();

                        for( int k=0; k<vmAttrs.getLength(); k++ ) {
                            Node vmAttr = vmAttrs.item(k);

                            if( vmAttr.getNodeName().equalsIgnoreCase("guestcustomizationsection") && vmAttr.hasChildNodes() ) {
                                NodeList custList = vmAttr.getChildNodes();

                                for( int l=0; l<custList.getLength(); l++ ) {
                                    Node cust = custList.item(l);

                                    if( cust.getNodeName().equalsIgnoreCase("computername") && cust.hasChildNodes() ) {
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
                            else if( vmAttr.getNodeName().equalsIgnoreCase("ovf:ProductSection") && vmAttr.hasChildNodes() ) {
                                NodeList prdList = vmAttr.getChildNodes();

                                for( int l=0; l<prdList.getLength(); l++ ) {
                                    Node prd = prdList.item(l);

                                    if( prd.getNodeName().equalsIgnoreCase("ovf:Product") && prd.hasChildNodes() ) {
                                        String n = prd.getFirstChild().getNodeValue().trim();

                                        if( n.length() > 0 ) {
                                            image.setPlatform(Platform.guess(n));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            else if( attribute.getNodeName().equalsIgnoreCase("datecreated") && attribute.hasChildNodes() ) {
                image.setCreationTimestamp(((vCloud)getProvider()).parseTime(attribute.getFirstChild().getNodeValue().trim()));
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
        image.setStorageFormat(MachineImageFormat.OVF);
        image.setType(MachineImageType.STORAGE);
        image.setCurrentState(MachineImageState.ACTIVE);
        image.setImageClass(ImageClass.MACHINE);
        return image;
    }

    @Override
    public void remove(@Nonnull String providerImageId, boolean checkState) throws CloudException, InternalException {
        //To change body of implemented methods use File | Settings | File Templates.
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
                if( ok ) {
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
                NodeList cNodes = doc.getElementsByTagName("Catalog");

                for( int i=0; i<cNodes.getLength(); i++ ) {
                    Node cnode = cNodes.item(i);

                    if( cnode.hasChildNodes() ) {
                        NodeList items = cnode.getChildNodes();

                        for( int j=0; j<items.getLength(); j++ ) {
                            Node wrapper = items.item(j);

                            if( wrapper.getNodeName().equalsIgnoreCase("CatalogItems") && wrapper.hasChildNodes() ) {
                                NodeList entries = wrapper.getChildNodes();

                                for( int k=0; k<entries.getLength(); k++ ) {
                                    Node item = entries.item(k);

                                    if( item.getNodeName().equalsIgnoreCase("CatalogItem") && item.hasAttributes() ) {
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
        return false;
    }
}
