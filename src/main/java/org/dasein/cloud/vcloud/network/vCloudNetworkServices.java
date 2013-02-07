package org.dasein.cloud.vcloud.network;

import org.dasein.cloud.network.AbstractNetworkServices;
import org.dasein.cloud.vcloud.vCloud;

import javax.annotation.Nonnull;

/**
 * Access into vCloud networking services.
 * <p>Created by George Reese: 9/17/12 10:58 AM</p>
 * @author George Reese
 * @version 2013.04 initial version
 * @since 2013.04
 */
public class vCloudNetworkServices extends AbstractNetworkServices {
    private vCloud provider;

    public vCloudNetworkServices(@Nonnull vCloud provider) {
        this.provider = provider;
    }

    @Override
    public @Nonnull HybridVLANSupport getVlanSupport() {
        return new HybridVLANSupport(provider);
    }
}
