/**
 * ========= CONFIDENTIAL =========
 *
 * Copyright (C) 2012 enStratus Networks Inc - ALL RIGHTS RESERVED
 *
 * ====================================================================
 *  NOTICE: All information contained herein is, and remains the
 *  property of enStratus Networks Inc. The intellectual and technical
 *  concepts contained herein are proprietary to enStratus Networks Inc
 *  and may be covered by U.S. and Foreign Patents, patents in process,
 *  and are protected by trade secret or copyright law. Dissemination
 *  of this information or reproduction of this material is strictly
 *  forbidden unless prior written permission is obtained from
 *  enStratus Networks Inc.
 * ====================================================================
 */
package org.dasein.cloud.vcloud;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.dc.DataCenter;
import org.dasein.cloud.dc.DataCenterServices;
import org.dasein.cloud.dc.Region;

import java.util.Collection;
import java.util.Locale;

/**
 * vCloud VDC support to describe the data centers in a specific vCloud-based cloud.
 * <p>Created by George Reese: 9/17/12 11:00 AM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class VDCServices implements DataCenterServices {
    private vCloud provider;

    VDCServices(vCloud provider) { this.provider = provider; }

    public DataCenter getDataCenter(String providerDataCenterId) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getProviderTermForDataCenter(Locale locale) {
        return "VDC Unit";
    }

    public String getProviderTermForRegion(Locale locale) {
        return "VDC";
    }

    public Region getRegion(String providerRegionId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            if( providerRegionId.equals(region.getProviderRegionId()) ) {
                return region;
            }
        }
        return null;
    }

    public Collection<DataCenter> listDataCenters(String providerRegionId) throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public Collection<Region> listRegions() throws InternalException, CloudException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
