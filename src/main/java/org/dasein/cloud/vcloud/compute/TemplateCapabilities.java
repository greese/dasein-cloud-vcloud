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

package org.dasein.cloud.vcloud.compute;

import org.dasein.cloud.*;
import org.dasein.cloud.compute.ImageCapabilities;
import org.dasein.cloud.compute.ImageClass;
import org.dasein.cloud.compute.MachineImageFormat;
import org.dasein.cloud.compute.MachineImageType;
import org.dasein.cloud.compute.VmState;
import org.dasein.cloud.vcloud.vCloud;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Locale;

/**
 * Describes the capabilities of Dasein Cloud template support for vCloud.
 * <p>Created by George Reese: 3/5/14 10:20 PM</p>
 * @author George Reese
 * @version 2014.03
 * @since 2014.03
 */
public class TemplateCapabilities extends AbstractCapabilities<vCloud> implements ImageCapabilities {
    public TemplateCapabilities(vCloud provider) { super(provider); }

    @Override
    public boolean canBundle(@Nonnull VmState fromState) throws CloudException, InternalException {
        return false;

    }

    @Override
    public boolean canImage(@Nonnull VmState fromState) throws CloudException, InternalException {
        return (fromState.equals(VmState.STOPPED) || fromState.equals(VmState.PAUSED) || fromState.equals(VmState.SUSPENDED));
    }

    @Override
    public @Nonnull String getProviderTermForImage(@Nonnull Locale locale, @Nonnull ImageClass imageClass) {
        return "template";
    }

    @Override
    public @Nonnull String getProviderTermForCustomImage(@Nonnull Locale locale, @Nonnull ImageClass imageClass) {
        return "template";
    }

    @Override
    public @Nullable VisibleScope getImageVisibleScope() {
        return null;
    }

    @Override
    public @Nonnull Requirement identifyLocalBundlingRequirement() throws CloudException, InternalException {
        return Requirement.NONE;
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormats() throws CloudException, InternalException {
        return Collections.singletonList(MachineImageFormat.VMDK);
    }

    @Override
    public @Nonnull Iterable<MachineImageFormat> listSupportedFormatsForBundling() throws CloudException, InternalException {
        return Collections.emptyList();
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
    public boolean supportsDirectImageUpload() throws CloudException, InternalException {
        return false;
    }

    @Override
    public boolean supportsImageCapture(@Nonnull MachineImageType type) throws CloudException, InternalException {
        return type.equals(MachineImageType.VOLUME);
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
    public boolean supportsPublicLibrary(@Nonnull ImageClass imageClass) throws CloudException, InternalException {
        return imageClass.equals(ImageClass.MACHINE);
    }
}
