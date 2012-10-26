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
package org.dasein.cloud.cloudsigma.compute;

import org.dasein.cloud.cloudsigma.CloudSigma;
import org.dasein.cloud.cloudsigma.compute.vm.ServerSupport;
import org.dasein.cloud.compute.AbstractComputeServices;

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
    public @Nonnull ServerSupport getVirtualMachineSupport() {
        return new ServerSupport(provider);
    }
}
