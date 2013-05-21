/**
 * Copyright (C) 2012-2013 Dell, Inc.
 * See annotations for authorship information
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

package org.dasein.cloud.cloudsigma.compute;

import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.compute.block.DataDriveSupport;
import org.dasein.cloud.cloudsigma.compute.image.BootDriveSupport;
import org.dasein.cloud.cloudsigma.compute.vm.ServerSupport;
import org.dasein.cloud.compute.AbstractComputeServices;
import org.dasein.cloud.compute.VolumeSupport;

import javax.annotation.Nonnull;

/**
 * Implements compute services against CloudSigma.
 * <p>Created by George Reese: 10/25/12 11:02 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09 initial version
 */
public class CloudSigmaComputeServices extends AbstractComputeServices {
    private CloudSigma provider;

    public CloudSigmaComputeServices(@Nonnull CloudSigma provider) { this.provider = provider; }

    @Override
    public @Nonnull  BootDriveSupport getImageSupport() {
        return new BootDriveSupport(provider);
    }

    @Override
    public @Nonnull ServerSupport getVirtualMachineSupport() {
        return new ServerSupport(provider);
    }

    @Override
    public @Nonnull VolumeSupport getVolumeSupport() {
        return new DataDriveSupport(provider);
    }
}
