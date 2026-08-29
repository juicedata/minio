/*
 * MinIO Cloud Storage, (C) 2026 MinIO, Inc.
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
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	xhttp "github.com/minio/minio/cmd/http"
	"github.com/minio/minio/pkg/bucket/versioning"
)

func TestCopyDstOptsPreservesBucketVersioningState(t *testing.T) {
	const bucket = "versioned-bucket"

	originalMetadataSys := globalBucketMetadataSys
	originalGatewaySSE := GlobalGatewaySSE
	originalIsGateway := globalIsGateway
	defer func() {
		globalBucketMetadataSys = originalMetadataSys
		GlobalGatewaySSE = originalGatewaySSE
		globalIsGateway = originalIsGateway
	}()

	globalIsGateway = false
	globalBucketMetadataSys = NewBucketMetadataSys()
	originalObjectLayer := newObjectLayerFn()
	objectLayer, root, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	setObjectLayer(objectLayer)
	defer func() {
		setObjectLayer(originalObjectLayer)
		objectLayer.Shutdown(context.Background())
		removeRoots([]string{root})
	}()

	sseS3Headers := http.Header{}
	sseS3Headers.Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionAES)
	sseCHeaders := http.Header{}
	sseCHeaders.Set(xhttp.AmzServerSideEncryptionCustomerAlgorithm, xhttp.AmzEncryptionAES)
	sseCHeaders.Set(xhttp.AmzServerSideEncryptionCustomerKey, "XAm0dRrJsEsyPb1UuFNezv1bl9hxuYsgUVC/MUctE2k=")
	sseCHeaders.Set(xhttp.AmzServerSideEncryptionCustomerKeyMD5, "bY4wkxQejw9mUJfo72k53A==")
	sseKMSHeaders := http.Header{}
	sseKMSHeaders.Set(xhttp.AmzServerSideEncryption, xhttp.AmzEncryptionKMS)

	testCases := []struct {
		name          string
		status        versioning.State
		gatewaySSE    gatewaySSE
		headers       http.Header
		wantVersioned bool
		wantSuspended bool
	}{
		{name: "default enabled", status: versioning.Enabled, wantVersioned: true},
		{name: "default suspended", status: versioning.Suspended, wantSuspended: true},
		{name: "SSE-S3 suspended", status: versioning.Suspended, gatewaySSE: gatewaySSE{gatewaySSES3}, headers: sseS3Headers, wantSuspended: true},
		{name: "SSE-C suspended", status: versioning.Suspended, gatewaySSE: gatewaySSE{gatewaySSEC}, headers: sseCHeaders, wantSuspended: true},
		{name: "SSE-KMS suspended", status: versioning.Suspended, headers: sseKMSHeaders, wantSuspended: true},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			metadata := NewBucketMetadata(bucket)
			metadata.versioningConfig = &versioning.Versioning{Status: testCase.status}
			globalBucketMetadataSys.Set(bucket, metadata)
			GlobalGatewaySSE = testCase.gatewaySSE
			storedVersioning, getErr := globalBucketMetadataSys.GetVersioningConfig(bucket)
			if getErr != nil {
				t.Fatal(getErr)
			}
			if storedVersioning.Status != testCase.status {
				t.Fatalf("stored versioning status: want %q, got %q", testCase.status, storedVersioning.Status)
			}

			req := httptest.NewRequest(http.MethodPut, "http://localhost/"+bucket+"/object", nil)
			req.Header = testCase.headers.Clone()
			opts, err := copyDstOpts(context.Background(), req, bucket, "object", nil)
			if err != nil {
				t.Fatal(err)
			}
			if opts.Versioned != testCase.wantVersioned {
				t.Fatalf("Versioned: want %v, got %v", testCase.wantVersioned, opts.Versioned)
			}
			if opts.VersionSuspended != testCase.wantSuspended {
				t.Fatalf("VersionSuspended: want %v, got %v", testCase.wantSuspended, opts.VersionSuspended)
			}
		})
	}
}
