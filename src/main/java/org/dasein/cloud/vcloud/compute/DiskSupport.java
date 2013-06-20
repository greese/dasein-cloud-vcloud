package org.dasein.cloud.vcloud.compute;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.OperationNotSupportedException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.ResourceStatus;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.Volume;
import org.dasein.cloud.compute.VolumeCreateOptions;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.compute.VolumeProduct;
import org.dasein.cloud.compute.VolumeState;
import org.dasein.cloud.compute.VolumeSupport;
import org.dasein.cloud.compute.VolumeType;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.identity.ServiceAction;
import org.dasein.cloud.util.APITrace;
import org.dasein.cloud.vcloud.vCloud;
import org.dasein.cloud.vcloud.vCloudMethod;
import org.dasein.util.CalendarWrapper;
import org.dasein.util.uom.storage.*;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Locale;

/**
 * Implements support for disks in vCloud 5.1 and beyond.
 * <p>Created by George Reese: 2/10/13 12:10 PM</p>
 * @author George Reese
 */
public class DiskSupport implements VolumeSupport {
    private vCloud provider;

    DiskSupport(@Nonnull vCloud provider) { this.provider = provider; }

    protected @Nonnull ProviderContext getContext() throws CloudException {
        ProviderContext ctx = getProvider().getContext();

        if( ctx == null ) {
            throw new CloudException("No context was set for this request");
        }
        return ctx;
    }

    protected final @Nonnull CloudProvider getProvider() {
        return provider;
    }

    @Override
    public void attach(@Nonnull String volumeId, @Nonnull String toServer, @Nonnull String deviceId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "attachVolume");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            StringBuilder xml = new StringBuilder();

            xml.append("<DiskAttachOrDetachParams xmlns=\"http://www.vmware.com/vcloud/v1.5\">");
            xml.append("<Disk type=\"application/vnd.vmware.vcloud.disk+xml\" href=\"").append(method.toURL("disk", volumeId)).append("\" />");
            xml.append("</DiskAttachOrDetachParams>");
            method.waitFor(method.post("attachVolume", method.toURL("vApp", toServer) + "/disk/action/attach", method.getMediaTypeForActionAttachVolume(), xml.toString()));
        }
        finally {
            APITrace.end();
        }
    }

    @Nonnull
    @Override
    public String create(@Nullable String fromSnapshot, @Nonnegative int sizeInGb, @Nonnull String inZone) throws InternalException, CloudException {
        Storage<Gigabyte> storage = new Storage<Gigabyte>(sizeInGb, Storage.GIGABYTE);

        if( getVolumeProductRequirement().equals(Requirement.REQUIRED) ) {
            VolumeProduct lastChance = null;
            VolumeProduct closest = null;

            for( VolumeProduct product : listVolumeProducts() ) {
                if( lastChance == null ) {
                    lastChance = product;
                }
                else {
                    Float l = lastChance.getMonthlyGigabyteCost();
                    Float t = product.getMonthlyGigabyteCost();

                    if( l != null && t != null && t < l ) {
                        lastChance = product;
                    }
                }
                if( isVolumeSizeDeterminedByProduct() ) {
                    Storage<Gigabyte> size = product.getVolumeSize();
                    int sz = (size == null ? 0 : size.intValue());

                    if( sz >= sizeInGb ) {
                        if( closest == null ) {
                            closest = product;
                        }
                        else {
                            size = closest.getVolumeSize();
                            if( size == null || size.intValue() > sz ) {
                                closest = product;
                            }
                        }
                    }
                }
                else {
                    if( closest == null ) {
                        closest = product;
                    }
                    else {
                        Float c = closest.getMonthlyGigabyteCost();
                        Float t = product.getMonthlyGigabyteCost();

                        if( c != null && t != null && t < c ) {
                            closest = product;
                        }
                    }
                }
            }
            if( closest == null ) {
                closest = lastChance;
            }
            if( closest != null ) {
                if( fromSnapshot != null ) {
                    String name = "Volume from Snapshot " + fromSnapshot;
                    String description = "Volume created from snapshot #" + fromSnapshot + " on " + (new Date());

                    return createVolume(VolumeCreateOptions.getInstanceForSnapshot(closest.getProviderProductId(), fromSnapshot, storage, name, description, 0));
                }
                else {
                    String name = "New Volume " + System.currentTimeMillis();
                    String description = "New Volume (created " + (new Date()) + ")";

                    return createVolume(VolumeCreateOptions.getInstance(closest.getProviderProductId(), storage, name, description, 0));
                }
            }
        }
        if( fromSnapshot != null ) {
            String name = "Volume from Snapshot " + fromSnapshot;
            String description = "Volume created from snapshot #" + fromSnapshot + " on " + (new Date());

            return createVolume(VolumeCreateOptions.getInstanceForSnapshot(fromSnapshot, storage, name, description));
        }
        else {
            String name = "New Volume " + System.currentTimeMillis();
            String description = "New Volume (created " + (new Date()) + ")";

            return createVolume(VolumeCreateOptions.getInstance(storage, name, description));
        }
    }

    @Override
    public @Nonnull String createVolume(@Nonnull VolumeCreateOptions options) throws InternalException, CloudException {
        if( options.getFormat().equals(VolumeFormat.NFS) ) {
            throw new OperationNotSupportedException("NFS volumes are not currently implemented for " + getProvider().getCloudName());
        }
        if( options.getSnapshotId() != null ) {
            throw new OperationNotSupportedException("Volumes created from snapshots make no sense when there are no snapshots");
        }
        APITrace.begin(getProvider(), "createVolume");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            String vdcId = options.getDataCenterId();

            if( vdcId == null ) {
                vdcId = getProvider().getDataCenterServices().listDataCenters(getContext().getRegionId()).iterator().next().getProviderDataCenterId();
            }
            long size = options.getVolumeSize().convertTo(Storage.BYTE).longValue();
            StringBuilder xml = new StringBuilder();

            xml.append("<DiskCreateParams xmlns=\"http://www.vmware.com/vcloud/v1.5\">");

            xml.append("<Disk name=\"").append(vCloud.escapeXml(options.getName())).append("\" ");
            xml.append("size=\"").append(String.valueOf(size)).append("\">");
            xml.append("<Description>").append(vCloud.escapeXml(options.getDescription())).append("</Description>");
            xml.append("</Disk>");
            xml.append("</DiskCreateParams>");

            String response = method.post(vCloudMethod.CREATE_DISK, vdcId, xml.toString());

            if( response.length() < 1 ) {
                throw new CloudException("No error, but no volume");
            }

            Document doc = method.parseXML(response);
            String docElementTagName = doc.getDocumentElement().getTagName();
            String nsString = "";
            if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
            NodeList disks = doc.getElementsByTagName(nsString + "Disk");

            if( disks.getLength() < 1 ) {
                throw new CloudException("No error, but no volume");
            }
            Node disk = disks.item(0);
            Node href = disk.getAttributes().getNamedItem("href");

            if( href != null ) {
                String volumeId = ((vCloud)getProvider()).toID(href.getNodeValue().trim());

                String vmId = options.getVlanId();

                if( vmId != null ) {
                    long timeout = System.currentTimeMillis() + (CalendarWrapper.MINUTE*10L);

                    while( timeout > System.currentTimeMillis() ) {
                        try { Thread.sleep(15000L); }
                        catch( InterruptedException ignore ) { }
                        try {
                            Volume v = getVolume(volumeId);

                            if( v != null && v.getCurrentState().equals(VolumeState.AVAILABLE) ) {
                                break;
                            }
                        }
                        catch( Throwable ignore ) {
                            // ignore
                        }
                    }
                    try { attach(volumeId, vmId, options.getDeviceId()); }
                    catch( Throwable ignore ) { }
                }
                return volumeId;
            }
            throw new CloudException("No ID provided in Disk XML");
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void detach(@Nonnull String volumeId) throws InternalException, CloudException {
        detach(volumeId, true);
    }

    @Override
    public void detach(@Nonnull String volumeId, boolean force) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "detachVolume");
        try {
            Volume volume = getVolume(volumeId);

            if( volume == null ) {
                throw new CloudException("No such volume: " + volumeId);
            }
            String serverId = volume.getProviderVirtualMachineId();

            if( serverId == null ) {
                throw new CloudException("No virtual machine is attached to this volume");
            }
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            StringBuilder xml = new StringBuilder();

            xml.append("<DiskAttachOrDetachParams xmlns=\"http://www.vmware.com/vcloud/v1.5\">");
            xml.append("<Disk href=\"").append(method.toURL("disk", volumeId)).append("\" />");
            xml.append("</DiskAttachOrDetachParams>");
            method.waitFor(method.post("detachVolume",  method.toURL("vApp", serverId) + "/disk/action/detach", method.getMediaTypeForActionAttachVolume(), xml.toString()));
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return -2;
    }

    @Override
    public Storage<Gigabyte> getMaximumVolumeSize() throws InternalException, CloudException {
        return null;
    }

    @Override
    public @Nonnull Storage<Gigabyte> getMinimumVolumeSize() throws InternalException, CloudException {
        return new Storage<Gigabyte>(1, Storage.GIGABYTE);
    }

    @Override
    public @Nonnull String getProviderTermForVolume(@Nonnull Locale locale) {
        return "disk";
    }

    @Override
    public Volume getVolume(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "getVolume");
        try {
            for( Volume v : listVolumes() ) {
                if( v.getProviderVolumeId().equals(volumeId) ) {
                    return v;
                }
            }
            return null; // TODO: optimize
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Requirement getVolumeProductRequirement() throws InternalException, CloudException {
        return Requirement.NONE;
    }

    @Override
    public boolean isVolumeSizeDeterminedByProduct() throws InternalException, CloudException {
        return false;
    }

    @Override
    public @Nonnull Iterable<String> listPossibleDeviceIds(@Nonnull Platform platform) throws InternalException, CloudException {
        ArrayList<String> ids = new ArrayList<String>();

        for( int i=5; i<10; i++ ) {
            for( int j=0; j<10; j++ ) {
                ids.add(i + ":" + j);
            }
        }
        return ids;
    }

    @Override
    public @Nonnull Iterable<VolumeFormat> listSupportedFormats() throws InternalException, CloudException {
        return Collections.singletonList(VolumeFormat.BLOCK);
    }

    @Nonnull
    @Override
    public Iterable<VolumeProduct> listVolumeProducts() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public @Nonnull Iterable<ResourceStatus> listVolumeStatus() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "listVolumeStatus");
        try {
            ArrayList<ResourceStatus> status = new ArrayList<ResourceStatus>();

            for( Volume v : listVolumes() ) {
                status.add(new ResourceStatus(v.getProviderVolumeId(), v.getCurrentState()));
            }
            return status;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Iterable<Volume> listVolumes() throws InternalException, CloudException {
        APITrace.begin(getProvider(), "listVolumes");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());
            ArrayList<Volume> volumes = new ArrayList<Volume>();

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
                                    if(resource.getNodeName().contains(":"))nsString = resource.getNodeName().substring(0, resource.getNodeName().indexOf(":") + 1);
                                    else nsString = "";

                                    if( resource.getNodeName().equalsIgnoreCase(nsString + "ResourceEntity") && resource.hasAttributes() ) {
                                        Node type = resource.getAttributes().getNamedItem("type");

                                        if( type != null && type.getNodeValue().equals(method.getMediaTypeForDisk()) ) {
                                            Node href = resource.getAttributes().getNamedItem("href");
                                            Volume volume = toVolume(dc.getProviderDataCenterId(), ((vCloud)getProvider()).toID(href.getNodeValue().trim()));

                                            if( volume != null ) {
                                                volumes.add(volume);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return volumes;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public boolean isSubscribed() throws CloudException, InternalException {
        APITrace.begin(getProvider(), "isSubscribedDisk");
        try {
            if( getProvider().testContext() != null ) {
                vCloudMethod method = new vCloudMethod(((vCloud)getProvider()));


                return vCloudMethod.matches(method.getAPIVersion(), "5.1", null);
            }
            return false;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public void remove(@Nonnull String volumeId) throws InternalException, CloudException {
        APITrace.begin(getProvider(), "removeVolume");
        try {
            vCloudMethod method = new vCloudMethod((vCloud)getProvider());

            method.delete("disk", volumeId);
        }
        finally {
            APITrace.end();
        }
    }

    private @Nonnull VolumeState toState(@Nonnull String status) {
        if( status.equals("1") ) {
            return VolumeState.AVAILABLE;
        }
        else if( status.equals("0") ) {
            return VolumeState.PENDING;
        }
        return VolumeState.PENDING;
    }

    private @Nullable Volume toVolume(@Nonnull String dcId, @Nonnull String volumeId) throws CloudException, InternalException {
        vCloudMethod method = new vCloudMethod((vCloud)getProvider());
        Volume volume = new Volume();

        volume.setProviderVolumeId(volumeId);
        volume.setCurrentState(VolumeState.AVAILABLE);
        volume.setFormat(VolumeFormat.BLOCK);
        volume.setType(VolumeType.HDD);
        volume.setProviderRegionId(getContext().getRegionId());
        volume.setProviderDataCenterId(dcId);
        volume.setRootVolume(false);

        String xml = method.get("disk", volumeId);

        if( xml == null || xml.length() < 1 ) {
            return null;
        }
        Document doc = method.parseXML(xml);
        String docElementTagName = doc.getDocumentElement().getTagName();
        String nsString = "";
        if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
        NodeList disks = doc.getElementsByTagName(nsString + "Disk");

        if( disks.getLength() < 1 ) {
            return null;
        }
        Node diskNode = disks.item(0);
        Node n = diskNode.getAttributes().getNamedItem("name");

        if( n != null ) {
            volume.setName(n.getNodeValue().trim());
        }
        n = diskNode.getAttributes().getNamedItem("size");
        if( n != null ) {
            try {
                volume.setSize(new Storage<org.dasein.util.uom.storage.Byte>(Integer.parseInt(n.getNodeValue().trim()), Storage.BYTE));
            }
            catch( NumberFormatException ignore ) {
                // ignore
            }
        }
        if( volume.getSize() == null ) {
            volume.setSize(new Storage<Gigabyte>(1, Storage.GIGABYTE));
        }
        n = diskNode.getAttributes().getNamedItem("status");
        if( n != null ) {
            volume.setCurrentState(toState(n.getNodeValue().trim()));
        }
        NodeList attributes = diskNode.getChildNodes();

        for( int i=0; i<attributes.getLength(); i++ ) {
            Node attribute = attributes.item(i);

            if( attribute.getNodeName().equalsIgnoreCase(nsString + "Description") && attribute.hasChildNodes() ) {
                volume.setDescription(attribute.getFirstChild().getNodeValue().trim());
            }
        }
        try {
            xml = method.get("disk", volumeId + "/attachedVms");

            if( xml != null && !xml.equals("") ) {
                doc = method.parseXML(xml);
                docElementTagName = doc.getDocumentElement().getTagName();
                nsString = "";
                if(docElementTagName.contains(":"))nsString = docElementTagName.substring(0, docElementTagName.indexOf(":") + 1);
                NodeList vms = doc.getElementsByTagName(nsString + "VmReference");

                if( vms.getLength() > 0 ) {
                    Node vm = vms.item(0);
                    Node href = vm.getAttributes().getNamedItem("href");

                    if( href != null ) {
                        volume.setProviderVirtualMachineId(((vCloud)getProvider()).toID(href.getNodeValue().trim()));
                    }
                }
            }
        }
        catch( Throwable ignore ) {
            // ignore
        }
        if( volume.getName() == null ) {
            volume.setName(volume.getProviderVolumeId());
        }
        if( volume.getDescription() == null ) {
            volume.setDescription(volume.getName());
        }
        return volume;
    }

    @Nonnull
    @Override
    public String[] mapServiceAction(@Nonnull ServiceAction action) {
        return new String[0];  //To change body of implemented methods use File | Settings | File Templates.
    }
}
