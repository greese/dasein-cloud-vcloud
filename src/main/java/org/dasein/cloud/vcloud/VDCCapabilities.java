package org.dasein.cloud.vcloud;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.dc.DataCenterCapabilities;

import javax.annotation.Nonnull;
import java.util.Locale;

/**
 * User: daniellemayne
 * Date: 04/07/2014
 * Time: 16:30
 */
public class VDCCapabilities extends AbstractCapabilities<vCloud> implements DataCenterCapabilities {
    public VDCCapabilities(@Nonnull vCloud provider) {
        super(provider);
    }
    @Override
    public String getProviderTermForDataCenter(Locale locale) {
        return "VDC";
    }

    @Override
    public String getProviderTermForRegion(Locale locale) {
        return "Org";
    }

    @Override
    public boolean supportsResourcePools() {
        return false;
    }
}
