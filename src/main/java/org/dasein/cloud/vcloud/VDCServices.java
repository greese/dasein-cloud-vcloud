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

package org.dasein.cloud.vcloud;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.*;
import org.dasein.cloud.util.APITrace;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * vCloud VDC support to describe the data centers in a specific vCloud-based cloud. Dasein Cloud maps your
 * vCloud organization to both an accountNumber and a Dasein Cloud region. Dasein Cloud data centers map to
 * vCloud VDCs. There's a further nuance in the ID format of regions and data centers. They can take the format
 * /org/ORG_ID and /vdc/VDC_ID, respectively, OR they can simply be ORG_ID and VDC_ID respectively. By default,
 * the {@link Region#getProviderRegionId()} == ORG_ID. However, if the {@link ProviderContext#getCustomProperties()}
 * property "compat" or system property "vCloudCompat" are set to true, Dasein Cloud will mimic the old
 * Dasein Cloud + jclouds behavior in which {@link Region#getProviderRegionId()} == /org/ORG_ID.
 * <p>Created by George Reese: 9/17/12 11:00 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class VDCServices implements DataCenterServices {
    private vCloud provider;

    VDCServices(vCloud provider) { this.provider = provider; }

    private transient volatile VDCCapabilities capabilities;
    @Nonnull
    @Override
    public DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new VDCCapabilities(provider);
        }
        return capabilities;
    }

    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String providerDataCenterId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getDataCenter");
        try {
            for( Region region : listRegions() ) {
                for( DataCenter dc : listDataCenters(region.getProviderRegionId()) ) {
                    if( providerDataCenterId.equals(dc.getProviderDataCenterId()) ) {
                        return dc;
                    }
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "VDC";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "Org";
    }

    @Override
    public @Nullable Region getRegion(@Nonnull String providerRegionId) throws InternalException, CloudException {
        APITrace.begin(provider, "DC.getRegion");
        try {
            for( Region region : listRegions() ) {
                if( providerRegionId.equals(region.getProviderRegionId()) ) {
                    return region;
                }
            }
            return null;
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String providerRegionId) throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was specified for this request");
        }
        if( !providerRegionId.equals(ctx.getRegionId()) ) {
            return Collections.emptyList();
        }
        APITrace.begin(provider, "DC.listDataCenters");
        try {
            return (new vCloudMethod(provider)).listDataCenters();
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public @Nonnull Collection<Region> listRegions() throws InternalException, CloudException {
        APITrace.begin(provider, "DC.listRegions");
        try {
            return Collections.singletonList((new vCloudMethod(provider)).getRegion());
        }
        finally {
            APITrace.end();
        }
    }

    @Override
    public Collection<ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Override
    public ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        return null;
    }

    @Override
    public Collection<StoragePool> listStoragePools() throws InternalException, CloudException {
        return Collections.emptyList();
    }
}
