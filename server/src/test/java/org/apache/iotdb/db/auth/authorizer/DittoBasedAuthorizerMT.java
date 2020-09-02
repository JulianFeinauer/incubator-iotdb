/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.auth.authorizer;

import com.nimbusds.oauth2.sdk.ParseException;
import org.apache.iotdb.db.auth.AuthException;
import org.apache.iotdb.db.auth.entity.PrivilegeType;
import org.eclipse.ditto.model.base.auth.AuthorizationContext;
import org.eclipse.ditto.model.base.auth.AuthorizationSubject;
import org.eclipse.ditto.model.enforcers.Enforcer;
import org.eclipse.ditto.model.enforcers.PolicyEnforcers;
import org.eclipse.ditto.model.policies.Policy;
import org.eclipse.ditto.model.policies.ResourceKey;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Optional;

import static org.junit.Assert.*;

public class DittoBasedAuthorizerMT {

    @Test
    public void testCheckPrivileges() throws URISyntaxException, AuthException, ParseException, IOException {
        final DittoBasedAuthorizer dittoBasedAuthorizer = new DittoBasedAuthorizer("http://192.168.169.18:8088/auth/realms/IoTDB/", "https://twin.pragmaticindustries.de/");

        final Optional<String> policyForThing = dittoBasedAuthorizer.fetchPolicyForThing("", "org.apache.plc4x.examples", "0a7492e3a7ee4dae9377614d9c4c84bc");

        assertTrue(policyForThing.isPresent());
        assertEquals("org.apache.plc4x.examples:0a7492e3a7ee4dae9377614d9c4c84bc", policyForThing.get());
    }

    @Test
    public void testCheckPolicy() throws URISyntaxException, AuthException, ParseException, IOException {
        final DittoBasedAuthorizer dittoBasedAuthorizer = new DittoBasedAuthorizer("http://192.168.169.18:8088/auth/realms/IoTDB/", "https://twin.pragmaticindustries.de/");

        final Optional<Policy> policyForThing = dittoBasedAuthorizer.fetchPolicy("", "org.apache.plc4x.examples", "0a7492e3a7ee4dae9377614d9c4c84bc");

        assertTrue(policyForThing.isPresent());
        assertTrue(policyForThing.get().getEntityId().isPresent());
        assertEquals("org.apache.plc4x.examples:0a7492e3a7ee4dae9377614d9c4c84bc", policyForThing.flatMap(Policy::getEntityId).get().toString());

        final Enforcer enforcer = PolicyEnforcers.defaultEvaluator(policyForThing.get());

        final boolean hasPermission = enforcer.hasPartialPermissions(ResourceKey.newInstance("thing", "/my/test/path"), AuthorizationContext.newInstance(AuthorizationSubject.newInstance("nginx:mqtt")), "HISTORY");

        assertTrue(hasPermission);
    }
}