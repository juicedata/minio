/*
 * MinIO Cloud Storage, (C) 2016-2020 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd

import (
	"errors"
	"net/http"
	"strings"
	"time"

	jwtgo "github.com/golang-jwt/jwt/v4"
	jwtreq "github.com/golang-jwt/jwt/v4/request"
	xjwt "github.com/minio/minio/cmd/jwt"
	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/auth"
)

const (
	jwtAlgorithm = "Bearer"

	// Default JWT token for web handlers is one day.
	defaultJWTExpiry = 24 * time.Hour

	// Inter-node JWT token expiry is 15 minutes.
	defaultInterNodeJWTExpiry = 15 * time.Minute

	// URL JWT token expiry is one minute (might be exposed).
	defaultURLJWTExpiry = time.Minute
)

var (
	errInvalidAccessKeyID   = errors.New("The access key ID you provided does not exist in our records")
	errChangeCredNotAllowed = errors.New("Changing access key and secret key not allowed")
	errAuthentication       = errors.New("Authentication failed, check your access credentials")
	errNoAuthToken          = errors.New("JWT token missing")
	errIncorrectCreds       = errors.New("Current access key or secret key is incorrect")
	errPresignedNotAllowed  = errors.New("Unable to generate shareable URL due to lack of read permissions")
)

func authenticateJWTUsers(accessKey, secretKey string, expiry time.Duration) (string, error) {
	expiresAt := UTCNow().Add(expiry)

	cred, secret, err := authenticateUsersForJWT(accessKey, secretKey, expiresAt)
	if err != nil {
		return "", err
	}

	claims := xjwt.NewMapClaims()
	claims.SetExpiry(expiresAt)
	claims.SetAccessKey(cred.AccessKey)
	if cred.ParentUser != "" {
		claims.SetLDAPUser(cred.ParentUser)
	}

	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, claims)
	return jwt.SignedString([]byte(secret))
}

func authenticateUsersForJWT(accessKey, secretKey string, expiredAt time.Time) (auth.Credentials, string, error) {
	if globalIAMSys.usersSysType == LDAPUsersSysType {
		return authenticateLDAPUsersForJWT(accessKey, secretKey, expiredAt)
	}
	return authenticateMinIOUsersForJWT(accessKey, secretKey)
}

func authenticateLDAPUsersForJWT(username, password string, expiredAt time.Time) (auth.Credentials, string, error) {
	ldapUserDN, ldapGroups, err := globalLDAPConfig.Bind(username, password)
	if err != nil {
		return auth.Credentials{}, "", errAuthentication
	}

	// Check if this user or their groups have a policy applied.
	ldapPolicies, _ := globalIAMSys.PolicyDBGet(ldapUserDN, false, ldapGroups...)
	if len(ldapPolicies) == 0 {
		return auth.Credentials{}, "", errInvalidAccessKeyID
	}

	m := map[string]interface{}{
		expClaim: expiredAt.Unix(),
		ldapUser: ldapUserDN,
	}

	secret := globalActiveCred.SecretKey
	cred, err := auth.GetNewCredentialsWithMetadata(m, secret)
	if err != nil {
		return auth.Credentials{}, "", errAuthentication
	}

	cred.ParentUser = ldapUserDN
	cred.Groups = ldapGroups

	// Set the newly generated credentials, ensure that policies are fixed during the session.
	if err = globalIAMSys.SetTempUser(cred.AccessKey, cred, strings.Join(ldapPolicies, ",")); err != nil {
		return auth.Credentials{}, "", errAuthentication
	}

	// Notify all other MinIO peers to reload temp users
	for _, nerr := range globalNotificationSys.LoadUser(cred.AccessKey, true) {
		if nerr.Err != nil {
			return auth.Credentials{}, "", errAuthentication
		}
	}

	return cred, secret, nil
}

func authenticateMinIOUsersForJWT(accessKey, secretKey string) (auth.Credentials, string, error) {
	serverCred := globalActiveCred
	if serverCred.AccessKey != accessKey {
		var ok bool
		serverCred, ok = globalIAMSys.GetUser(accessKey)
		if !ok {
			return auth.Credentials{}, "", errInvalidAccessKeyID
		}
	}

	if serverCred.AccessKey != serverCred.AccessKey && serverCred.SecretKey != secretKey {
		return auth.Credentials{}, "", errAuthentication
	}
	return serverCred, serverCred.SecretKey, nil
}

func authenticateNode(accessKey, secretKey, audience string) (string, error) {
	claims := xjwt.NewStandardClaims()
	claims.SetExpiry(UTCNow().Add(defaultInterNodeJWTExpiry))
	claims.SetAccessKey(accessKey)
	claims.SetAudience(audience)

	jwt := jwtgo.NewWithClaims(jwtgo.SigningMethodHS512, claims)
	return jwt.SignedString([]byte(secretKey))
}

func authenticateWeb(accessKey, secretKey string) (string, error) {
	return authenticateJWTUsers(accessKey, secretKey, defaultJWTExpiry)
}

func authenticateURL(accessKey, secretKey string) (string, error) {
	return authenticateJWTUsers(accessKey, secretKey, defaultURLJWTExpiry)
}

// Callback function used for parsing
func webTokenCallback(claims *xjwt.MapClaims) ([]byte, error) {
	if claims.AccessKey == globalActiveCred.AccessKey {
		return []byte(globalActiveCred.SecretKey), nil
	}
	ok, _, err := globalIAMSys.IsTempUser(claims.AccessKey)
	if err != nil {
		if err == errNoSuchUser {
			return nil, errInvalidAccessKeyID
		}
		return nil, err
	}
	if ok {
		return []byte(globalActiveCred.SecretKey), nil
	}
	cred, ok := globalIAMSys.GetUser(claims.AccessKey)
	if !ok {
		return nil, errInvalidAccessKeyID
	}
	return []byte(cred.SecretKey), nil
}

func isAuthTokenValid(token string) bool {
	_, _, err := webTokenAuthenticate(token)
	return err == nil
}

func webTokenAuthenticate(token string) (*xjwt.MapClaims, bool, error) {
	if token == "" {
		return nil, false, errNoAuthToken
	}
	claims := xjwt.NewMapClaims()
	if err := xjwt.ParseWithClaims(token, claims, webTokenCallback); err != nil {
		return claims, false, errAuthentication
	}
	owner := claims.AccessKey == globalActiveCred.AccessKey
	return claims, owner, nil
}

// Check if the request is authenticated.
// Returns nil if the request is authenticated. errNoAuthToken if token missing.
// Returns errAuthentication for all other errors.
func webRequestAuthenticate(req *http.Request) (*xjwt.MapClaims, bool, error) {
	token, err := jwtreq.AuthorizationHeaderExtractor.ExtractToken(req)
	if err != nil {
		if err == jwtreq.ErrNoTokenInRequest {
			return nil, false, errNoAuthToken
		}
		return nil, false, err
	}
	claims := xjwt.NewMapClaims()
	if err := xjwt.ParseWithClaims(token, claims, webTokenCallback); err != nil {
		return claims, false, errAuthentication
	}
	owner := claims.AccessKey == globalActiveCred.AccessKey
	return claims, owner, nil
}

func newAuthToken(audience string) string {
	cred := globalActiveCred
	token, err := authenticateNode(cred.AccessKey, cred.SecretKey, audience)
	logger.CriticalIf(GlobalContext, err)
	return token
}
