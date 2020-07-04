/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at    http://www.apache.org/licenses/LICENSE-2.0  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the License for the specific language governing permissions and limitations under the License.
 */

package org.apache.iotdb.db.auth.authorizer;

import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.RefreshTokenGrant;
import com.nimbusds.oauth2.sdk.ResourceOwnerPasswordCredentialsGrant;
import com.nimbusds.oauth2.sdk.TokenErrorResponse;
import com.nimbusds.oauth2.sdk.TokenRequest;
import com.nimbusds.oauth2.sdk.TokenResponse;
import com.nimbusds.oauth2.sdk.auth.ClientAuthentication;
import com.nimbusds.oauth2.sdk.auth.ClientSecretBasic;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderMetadata;
import net.minidev.json.JSONObject;
import org.apache.iotdb.db.auth.AuthException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Does full OAUtH 2 / OpenID Connect login
 */
public class OpenIdAuthorizer extends OpenIdTokenAuthorizer {

    private static final Logger logger = LoggerFactory.getLogger(OpenIdAuthorizer.class);

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    private final Map<String, AccessToken> accessTokenMap = new ConcurrentHashMap<>();

    private URI tokenEndpointURI;

    public OpenIdAuthorizer() throws AuthException, ParseException, IOException, URISyntaxException {
        super();
    }

    OpenIdAuthorizer(JSONObject jwk) throws AuthException {
        super(jwk);
    }

    OpenIdAuthorizer(String providerUrl) throws AuthException, URISyntaxException, ParseException, IOException {
        super(providerUrl);
        final OIDCProviderMetadata metadata = fetchMetadata(providerUrl);
        tokenEndpointURI = metadata.getTokenEndpointURI();
    }

    @Override
    public boolean login(String username, String password) throws AuthException {
        try {
            // Do the auth workflow...
            // Construct the code grant from the code obtained from the authz endpoint
            // and the original callback URI used at the authz endpoint
            final ResourceOwnerPasswordCredentialsGrant grant = new ResourceOwnerPasswordCredentialsGrant(username, new Secret(password));

            // The credentials to authenticate the client at the token endpoint
            ClientID clientID = new ClientID("iotdb-2");
            Secret clientSecret = new Secret("fdaae6f1-078e-4e4a-b192-576f4bf1694d");

            ClientAuthentication clientAuth = new ClientSecretBasic(clientID, clientSecret);

            // Make the token request
            TokenRequest request = new TokenRequest(tokenEndpointURI, clientAuth, grant);

            TokenResponse response = TokenResponse.parse(request.toHTTPRequest().send());

            if (!response.indicatesSuccess()) {
                // We got an error response...
                TokenErrorResponse errorResponse = response.toErrorResponse();
                throw new AuthException("Unable to get Token: " + errorResponse.getErrorObject().getDescription());
            }

            AccessTokenResponse successResponse = response.toSuccessResponse();

            // Get the access token, the server may also return a refresh token
            AccessToken accessToken = successResponse.getTokens().getAccessToken();
            this.accessTokenMap.put(username, accessToken);

            super.login(accessToken.getValue(), "");

            RefreshToken refreshToken = successResponse.getTokens().getRefreshToken();
            final long expiresIn = accessToken.getLifetime();


            logger.debug("User {} logged in, Token will expire in {}s", username, expiresIn);

            executorService.schedule(new RefreshTokenCommand(tokenEndpointURI, clientAuth, refreshToken, username), expiresIn - 10, TimeUnit.SECONDS);

            return true;
        } catch (IOException | ParseException e) {
            throw new AuthException("Unable to Login", e);
        }
    }

    private class RefreshTokenCommand implements Runnable {
        private final URI fixedEndpoint;
        private final ClientAuthentication clientAuth;
        private final RefreshToken refreshToken;
        private final String username;

        public RefreshTokenCommand(URI fixedEndpoint, ClientAuthentication clientAuth, RefreshToken refreshToken, String username) {
            this.fixedEndpoint = fixedEndpoint;
            this.clientAuth = clientAuth;
            this.refreshToken = refreshToken;
            this.username = username;
        }

        @Override
        public void run() {
            // Fetch a new Token

            OpenIdAuthorizer.logger.debug("Refreshing Auth Token for user {}", username);

            final TokenRequest request = new TokenRequest(fixedEndpoint, clientAuth, new RefreshTokenGrant(refreshToken));

            final TokenResponse response;
            try {
                response = TokenResponse.parse(request.toHTTPRequest().send());
            } catch (ParseException | IOException e) {
                // Simply do nothing???
                logger.error("Unable to Refresh Token for user {}", username, e);
                return;
            }

            if (!response.indicatesSuccess()) {
                // We got an error response...
                TokenErrorResponse errorResponse = response.toErrorResponse();
                logger.error("Unable to Refresh Token for user {}: {}", username, errorResponse.getErrorObject().getDescription());
            }

            // Get the access token, the server may also return a refresh token
            AccessToken accessToken = response.toSuccessResponse().getTokens().getAccessToken();
            OpenIdAuthorizer.this.accessTokenMap.put(username, accessToken);

            try {
                OpenIdAuthorizer.super.login(accessToken.getValue(), "");
            } catch (AuthException e) {
                logger.error("Unable to refresh login for user {}", username, e);
            }

            RefreshToken newRefreshToken = response.toSuccessResponse().getTokens().getRefreshToken();
            final long expiresIn = accessToken.getLifetime();

            // Reschedule itself
            OpenIdAuthorizer.this.executorService.schedule(new RefreshTokenCommand(fixedEndpoint, clientAuth, newRefreshToken, username), expiresIn - 10, TimeUnit.SECONDS);
        }
    }
}
