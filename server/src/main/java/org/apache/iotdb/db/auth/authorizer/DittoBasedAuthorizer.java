/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.auth.authorizer;

import com.nimbusds.oauth2.sdk.ParseException;
import net.minidev.json.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.eclipse.ditto.model.policies.PoliciesModelFactory;
import org.eclipse.ditto.model.policies.Policy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DittoBasedAuthorizer extends OpenIdAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(DittoBasedAuthorizer.class);

    public static final int READ_TIMESERIES = PrivilegeType.READ_TIMESERIES.ordinal();
    private final String dittoUrl;

    public DittoBasedAuthorizer(String dittoUrl) throws AuthException, ParseException, IOException, URISyntaxException {
        this.dittoUrl = dittoUrl;
    }

    DittoBasedAuthorizer(JSONObject jwk, String dittoUrl) throws AuthException {
        super(jwk);
        this.dittoUrl = dittoUrl;
    }

    DittoBasedAuthorizer(String providerUrl, String dittoUrl) throws AuthException, URISyntaxException, ParseException, IOException {
        super(providerUrl);
        this.dittoUrl = dittoUrl;
    }

    @Override
    public Set<Integer> getPrivileges(String username, String path) throws AuthException {
        return super.getPrivileges(username, path);
    }

    @Override
    public boolean checkUserPrivileges(String username, String path, int privilegeId) throws AuthException {
        // Map Path to Ditto
        final String[] pathElements = path.split("\\.");
        assert pathElements.length >= 4;
        assert pathElements[0].equals("root");
        final String namespace = pathElements[1];
        final String deviceId = pathElements[2];
        final String feature = pathElements[3];
        final List<String> featureMapping = Arrays.asList(Arrays.copyOfRange(pathElements, 4, pathElements.length));
        if (PrivilegeType.READ_TIMESERIES.ordinal() == privilegeId) {
            // Do check with ditto
            return false;
        }
        return super.checkUserPrivileges(username, path, privilegeId);
    }

    public Optional<String> fetchPolicyForThing(String username, String namespace, String thingId) {
        try {
            HttpClient client = new HttpClient();
            final GetMethod getMethod = new GetMethod(new URI(dittoUrl).resolve(String.format("api/2/things/%s:%s/policyId", namespace, thingId)).toString());
            // getMethod.addRequestHeader("Authorization", "Bearer " + super.accessTokenMap.get(username));
            getMethod.addRequestHeader("Authorization", "Basic bXF0dDptcXR0");
            client.executeMethod(getMethod);

            if (getMethod.getStatusCode() != 200) {
                logger.warn("Request returned HTTP Code {} for policy request for thing {}:{}", getMethod.getStatusCode(), namespace, thingId);
                return Optional.empty();
            }
            final String policyId = getMethod.getResponseBodyAsString();
            final String unescaped = StringEscapeUtils.unescapeJava(policyId);
            return Optional.of(unescaped.substring(1, unescaped.length() - 1));
        } catch (IOException | URISyntaxException e) {
            logger.warn("Unable to get policy id for thing {}:{}!", namespace, thingId, e);
            return Optional.empty();
        }
    }

    public Optional<Policy> fetchPolicy(String username, String namespace, String policyId) {
        try {
            HttpClient client = new HttpClient();
            final GetMethod getMethod = new GetMethod(new URI(dittoUrl).resolve(String.format("api/2/policies/%s:%s", namespace, policyId)).toString());
            // getMethod.addRequestHeader("Authorization", "Bearer " + super.accessTokenMap.get(username));
            getMethod.addRequestHeader("Authorization", "Basic bXF0dDptcXR0");
            client.executeMethod(getMethod);

            if (getMethod.getStatusCode() != 200) {
                logger.warn("Request returned HTTP Code {} for policy request for policy {}:{}", getMethod.getStatusCode(), namespace, policyId);
                return Optional.empty();
            }
            final String policyContent = getMethod.getResponseBodyAsString();
            final Policy policy = PoliciesModelFactory.newPolicy(policyContent);
            return Optional.of(policy);
        } catch (IOException | URISyntaxException e) {
            logger.warn("Unable to get policy id for policy {}:{}!", namespace, policyId, e);
            return Optional.empty();
        }
    }
}
