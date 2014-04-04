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
