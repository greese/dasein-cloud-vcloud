/*
 * Copyright (C) 2014 enStratus Networks Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.dasein.cloud.vcloud.compute;

import org.dasein.cloud.AbstractCapabilities;
import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.Requirement;
import org.dasein.cloud.compute.Platform;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.compute.VolumeCapabilities;
import org.dasein.cloud.compute.VolumeFormat;
import org.dasein.cloud.vcloud.vCloud;
import org.dasein.util.uom.storage.Gigabyte;
import org.dasein.util.uom.storage.Storage;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities for supporting Dasein Cloud volumes as implemented in this Dasein Cloud
 * implementation for vCloud.
 * <p>Created by George Reese: 3/5/14 10:29 PM</p>
 * @author George Reese
 * @version 2014.03 initial version
 * @since 2014.03
 */
public class DiskCapabilities extends AbstractCapabilities<vCloud> implements VolumeCapabilities {
    public DiskCapabilities(vCloud provider) {
        super(provider);
    }

    @Override
    public boolean canAttach(VmState vmState) throws InternalException, CloudException {
        return vmState.equals(VmState.STOPPED);
    }

    @Override
    public boolean canDetach(VmState vmState) throws InternalException, CloudException {
        return vmState.equals(VmState.STOPPED);
    }

    @Override
    public int getMaximumVolumeCount() throws InternalException, CloudException {
        return AbstractCapabilities.LIMIT_UNKNOWN;
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

    @Override
    public @Nonnull Requirement requiresVMOnCreate() throws InternalException, CloudException {
        return Requirement.OPTIONAL;
    }
}
