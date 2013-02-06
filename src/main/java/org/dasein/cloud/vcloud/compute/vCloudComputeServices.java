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

import org.dasein.cloud.CloudProvider;
import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.vcloud.vCloud;

import javax.annotation.Nonnull;

/**
 * Dasein Cloud compute services for vCloud Director.
 * <p>Created by George Reese: 9/17/12 10:58 AM</p>
 * @author George Reese
 * @since 2013.04
 * @version 2013.04 initial version
 */
public class vCloudComputeServices extends AbstractComputeServices {
    private vCloud provider;

    public vCloudComputeServices(@Nonnull vCloud provider) {
        this.provider = provider;
    }

    @Override
    public @Nonnull TemplateSupport getImageSupport() {
        return new TemplateSupport(provider);
    }

    @Override
    public @Nonnull vAppSupport getVirtualMachineSupport() {
        return new vAppSupport(provider);
    }
}
