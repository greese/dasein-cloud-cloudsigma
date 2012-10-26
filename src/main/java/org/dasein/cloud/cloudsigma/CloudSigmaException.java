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
package org.dasein.cloud.cloudsigma;

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * An error extending the basic cloud exception representing an error that has occurred in CloudSigma.
 * <p>Created by George Reese: 10/25/12 7:45 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class CloudSigmaException extends CloudException {

    public CloudSigmaException(@Nonnull Throwable cause) {
        super(cause);
    }

    public CloudSigmaException(@Nonnull CloudErrorType type, @Nonnegative int httpCode, @Nonnull String providerCode, @Nonnull String message) {
        super(type, httpCode, providerCode, message);
    }
}
