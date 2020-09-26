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
import org.apache.iotdb.tsfile.read.common.Path;
import org.eclipse.ditto.model.base.auth.AuthorizationContext;
import org.eclipse.ditto.model.base.auth.AuthorizationSubject;
import org.eclipse.ditto.model.enforcers.Enforcer;
import org.eclipse.ditto.model.enforcers.PolicyEnforcers;
import org.eclipse.ditto.model.policies.PoliciesModelFactory;
import org.eclipse.ditto.model.policies.PoliciesResourceType;
import org.eclipse.ditto.model.policies.Policy;
import org.eclipse.ditto.model.policies.ResourceKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Authorizer based on Eclipse Ditto.
 * It is based on Open ID Connect for authentication and Dittos PolicyEnforcer for authorization.
 * It is especially useful in scenarios where Ditto data is forwarded to IoTDB for time-series storage.
 *
 * @see <a href="https://www.eclipse.org/ditto/index.html">Eclipse Ditto</a>
 * @see <a href="https://en.wikipedia.org/wiki/OpenID_Connect">Open ID Connect</a>
 */
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
        final Path parsedPath = new Path(path);
        // User wants to read, is the user allowed to read?
        final String thingId = parsedPath.getDevice();
        if (READ_TIMESERIES == privilegeId) {
            // Do check with ditto
            logger.debug("Fetching policy name for thing {}", thingId);
            final Optional<String> policyId = fetchPolicyForThing(username, thingId);
            if (!policyId.isPresent()) {
                // No policy found => something is wrong
                logger.warn("Unable to find ditto policy for {}", thingId);
                return false;
            }
            // Fetch the policy instance
            final Optional<Policy> policyOptional = fetchPolicy(username, policyId.get());

            if (!policyOptional.isPresent()) {
                logger.warn("Unable to fetch policy with id {}", policyId);
                return false;
            }

            // Evaluate policy
            final Enforcer enforcer = PolicyEnforcers.defaultEvaluator(policyOptional.get());

            final boolean hasPermission = enforcer.hasPartialPermissions(ResourceKey.newInstance("thing", "/my/test/path"), AuthorizationContext.newInstance(AuthorizationSubject.newInstance("nginx:mqtt")), "HISTORY");

            return hasPermission;
        }
        return super.checkUserPrivileges(username, path, privilegeId);
    }

    public boolean hasPermission(String subject, String thingId, String feature, String path, String permission) {
        final Optional<Policy> policy = fetchPolicyForThing(subject, thingId).flatMap(
            policyId -> fetchPolicy(subject, policyId)
        );

        if (!policy.isPresent()) {
            return false;
        }

        final Enforcer enforcer = PolicyEnforcers.defaultEvaluator(policy.get());

        return enforcer.hasPartialPermissions(
            PoliciesResourceType.thingResource(String.format("/features/%s/properties/%s", feature, removeSlash(path))),
            AuthorizationContext.newInstance(AuthorizationSubject.newInstance(subject)),
            permission);
    }

    /**
     * Removes starting slash if existing.
     */
    private String removeSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    /**
     * Fetch name of policy that is applicable for thing.
     *
     * @param username Token
     * @param thingId Thing id with namespace
     * @return name of the policy (if found), empty optional otherwise
     */
    public Optional<String> fetchPolicyForThing(String username, String thingId) {
        try {
            HttpClient client = new HttpClient();
            final GetMethod getMethod = new GetMethod(new URI(dittoUrl).resolve(String.format("api/2/things/%s/policyId", thingId)).toString());
            getMethod.addRequestHeader("Authorization", "Bearer " + super.accessTokenMap.get(username));
            // getMethod.addRequestHeader("Authorization", "Basic bXF0dDptcXR0");
            client.executeMethod(getMethod);

            if (getMethod.getStatusCode() != 200) {
                logger.warn("Request returned HTTP Code {} for policy request for thing {}", getMethod.getStatusCode(), thingId);
                return Optional.empty();
            }
            final String policyId = getMethod.getResponseBodyAsString();
            final String unescaped = StringEscapeUtils.unescapeJava(policyId);
            return Optional.of(unescaped.substring(1, unescaped.length() - 1));
        } catch (IOException | URISyntaxException e) {
            logger.warn("Unable to get policy id for thing {}!", thingId, e);
            return Optional.empty();
        }
    }

    /**
     * Fetch policy with given ID
     *
     * @param username Token
     * @param policyId ID (with namespace) of the policy
     * @return policy (if found), empty optional otherwise
     */
    public Optional<Policy> fetchPolicy(String username, String policyId) {
        try {
            HttpClient client = new HttpClient();
            final GetMethod getMethod = new GetMethod(new URI(dittoUrl).resolve(String.format("api/2/policies/%s", policyId)).toString());
            getMethod.addRequestHeader("Authorization", "Bearer " + super.accessTokenMap.get(username));
            // getMethod.addRequestHeader("Authorization", "Basic bXF0dDptcXR0");
            client.executeMethod(getMethod);

            if (getMethod.getStatusCode() != 200) {
                logger.warn("Request returned HTTP Code {} for policy request for policy {}", getMethod.getStatusCode(), policyId);
                return Optional.empty();
            }
            final String policyContent = getMethod.getResponseBodyAsString();
            final Policy policy = PoliciesModelFactory.newPolicy(policyContent);
            return Optional.of(policy);
        } catch (IOException | URISyntaxException e) {
            logger.warn("Unable to get policy id for policy {}!", policyId, e);
            return Optional.empty();
        }
    }
}
