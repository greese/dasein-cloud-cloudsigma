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

package org.dasein.cloud.cloudsigma;

import org.dasein.cloud.CloudException;
import org.dasein.cloud.InternalException;
import org.dasein.cloud.ProviderContext;
import org.dasein.cloud.dc.*;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Locale;

/**
 * Implements data center services for CloudSigma describing the different CloudSigma regions.
 * <p>Created by George Reese: 10/25/12 7:18 PM</p>
 * @author George Reese
 * @version 2012.09 initial version
 * @since 2012.09
 */
public class CloudSigmaDataCenterServices implements DataCenterServices {
    private CloudSigma provider;

    private transient volatile CSDataCenterCapabilities capabilities;
    @Nonnull
    @Override
    public DataCenterCapabilities getCapabilities() throws InternalException, CloudException {
        if( capabilities == null ) {
            capabilities = new CSDataCenterCapabilities(provider);
        }
        return capabilities;
    }

    @Nonnull
    @Override
    public Collection<ResourcePool> listResourcePools(String providerDataCenterId) throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nullable
    @Override
    public ResourcePool getResourcePool(String providerResourcePoolId) throws InternalException, CloudException {
        return null;
    }

    @Nonnull
    @Override
    public Collection<StoragePool> listStoragePools() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public StoragePool getStoragePool(String providerStoragePoolId) throws InternalException, CloudException {
        return null;
    }

    @Nonnull
    @Override
    public Collection<Folder> listVMFolders() throws InternalException, CloudException {
        return Collections.emptyList();
    }

    @Nonnull
    @Override
    public Folder getVMFolder(String providerVMFolderId) throws InternalException, CloudException {
        return null;
    }

    CloudSigmaDataCenterServices(@Nonnull CloudSigma provider) { this.provider = provider; }

    @Override
    public @Nullable DataCenter getDataCenter(@Nonnull String dataCenterId) throws InternalException, CloudException {
        for( Region region : listRegions() ) {
            for( DataCenter dc : listDataCenters(region.getProviderRegionId()) ) {
                if( dataCenterId.equals(dc.getProviderDataCenterId()) ) {
                    return dc;
                }
            }
        }
        return null;
    }

    @Override
    public @Nonnull String getProviderTermForDataCenter(@Nonnull Locale locale) {
        return "data center";
    }

    @Override
    public @Nonnull String getProviderTermForRegion(@Nonnull Locale locale) {
        return "region";
    }

    @Override
    public @Nullable Region getRegion(@Nonnull String providerRegionId) throws InternalException, CloudException {
        for( Region r : listRegions() ) {
            if( providerRegionId.equals(r.getProviderRegionId()) ) {
                return r;
            }
        }
        return null;
    }

    @Override
    public @Nonnull Collection<DataCenter> listDataCenters(@Nonnull String providerRegionId) throws InternalException, CloudException {
        Region r = getRegion(providerRegionId);

        if( r == null ) {
            return Collections.emptyList();
        }
        DataCenter dc = new DataCenter();

        dc.setActive(r.isActive());
        dc.setAvailable(r.isAvailable());
        if( providerRegionId.equals("eu-ch1") ) {
            dc.setActive(true);
            dc.setAvailable(true);
            dc.setName("Zurich");
            dc.setProviderDataCenterId(providerRegionId+ "-a");
            dc.setRegionId(providerRegionId);
        }
        else if( providerRegionId.equals("us-nv1") ) {
            dc.setActive(true);
            dc.setAvailable(true);
            dc.setName("Las Vegas");
            dc.setProviderDataCenterId(providerRegionId+ "-a");
            dc.setRegionId(providerRegionId);
        }
        return Collections.singletonList(dc);
    }

    @Override
    public Collection<Region> listRegions() throws InternalException, CloudException {
        ProviderContext ctx = provider.getContext();

        if( ctx == null ) {
            throw new CloudException("No context was defined for this request");
        }
        String endpoint = ctx.getEndpoint();
        Region region = new Region();
        URI uri;

        try {
            if( endpoint == null || endpoint.trim().equals("") || endpoint.contains("api.cloudsigma.com") ) {
                uri = new URI("https://zrh.cloudsigma.com");
            }
            else {
                uri = new URI(endpoint);
            }
        }
        catch( URISyntaxException e ) {
            throw new CloudException("Unknown region endpoint: " + endpoint);
        }
        if( uri.getHost().endsWith("zrh.cloudsigma.com") ) {
            region.setActive(true);
            region.setAvailable(true);
            region.setName("Switzerland 1");
            region.setProviderRegionId("eu-ch1");
            region.setJurisdiction("CH");
        }
        else if( uri.getHost().endsWith("lvs.cloudsigma.com") ) {
            region.setActive(true);
            region.setAvailable(true);
            region.setName("Nevada 1");
            region.setProviderRegionId("us-nv1");
            region.setJurisdiction("US");
        }
        else {
            String[] parts = uri.getHost().split("\\.");


            if( parts.length == 4 && parts[0].equals("api") && parts[2].equals("cloudsigma") && parts[3].equals("com") ) {
                region.setActive(true);
                region.setAvailable(true);
                region.setName(parts[1]);
                region.setProviderRegionId(parts[1]);
                region.setJurisdiction("EU");
            }
            else {
                throw new CloudException("Unknown region endpoint: " + endpoint);
            }
        }
        return Collections.singletonList(region);
    }
}
