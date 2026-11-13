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
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	xhttp "github.com/minio/minio/cmd/http"
)

// Record keys at the handler boundary so backend validation cannot mask a failure.
type pathRecordingObjectLayer struct {
	ObjectLayer
	bucket string
	keys   []string
}

func (o *pathRecordingObjectLayer) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header, lock LockType, opts ObjectOptions) (*GetObjectReader, error) {
	if bucket == o.bucket {
		o.keys = append(o.keys, object)
	}
	return o.ObjectLayer.GetObjectNInfo(ctx, bucket, object, rs, h, lock, opts)
}

func (o *pathRecordingObjectLayer) PutObject(ctx context.Context, bucket, object string, r *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
	if bucket == o.bucket {
		o.keys = append(o.keys, object)
	}
	return o.ObjectLayer.PutObject(ctx, bucket, object, r, opts)
}

func (o *pathRecordingObjectLayer) DeleteObjects(ctx context.Context, bucket string, objects []ObjectToDelete, opts ObjectOptions) ([]DeletedObject, []error) {
	if bucket == o.bucket {
		for _, object := range objects {
			o.keys = append(o.keys, object.ObjectName)
		}
	}
	return o.ObjectLayer.DeleteObjects(ctx, bucket, objects, opts)
}

func setupObjectPathTest(t *testing.T, endpoints ...string) (*pathRecordingObjectLayer, http.Handler) {
	t.Helper()
	obj, dir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		obj.Shutdown(context.Background())
		os.RemoveAll(dir)
		resetTestGlobals()
	})
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}
	recorder := &pathRecordingObjectLayer{ObjectLayer: obj}
	bucket, router, err := initAPIHandlerTest(recorder, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	recorder.bucket = bucket
	return recorder, router
}

func assertObjectPathError(t *testing.T, rec *httptest.ResponseRecorder, code APIErrorCode) {
	t.Helper()
	want := getAPIError(code)
	var response APIErrorResponse
	if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode error response: %v: %s", err, rec.Body)
	}
	if rec.Code != want.HTTPStatusCode || response.Code != want.Code {
		t.Errorf("HTTP %d, code %s; want HTTP %d, code %s", rec.Code, response.Code, want.HTTPStatusCode, want.Code)
	}
}

func TestCopySourcePathValidation(t *testing.T) {
	for _, endpoint := range []string{"CopyObject", "CopyObjectPart"} {
		t.Run(endpoint, func(t *testing.T) {
			obj, router := setupObjectPathTest(t, endpoint)
			ctx := context.Background()
			cred := globalActiveCred
			for _, tc := range []struct {
				source string
				key    string // Empty for invalid paths.
			}{
				{"workspaces/A/../B/secret", ""},
				{"workspaces/A/./file", ""},
				{"workspaces/A/..", ""},
				{"./secret", ""},
				{"workspaces/A/%2e%2e/B/secret", ""},
				{"workspaces/A/%2E%2E%2FB/secret", ""},
				{"workspaces/A/%2e/file", ""},
				{"workspaces/A/.hidden", "workspaces/A/.hidden"},
				{"workspaces/A/file..txt", "workspaces/A/file..txt"},
				{"workspaces/A/.../file", "workspaces/A/.../file"},
				{"workspaces/A/%252e%252e/file", "workspaces/A/%2e%2e/file"},
				{"workspaces/A/file?versionId=null", "workspaces/A/file"},
			} {
				t.Run(tc.source, func(t *testing.T) {
					put := func(key, data string) {
						t.Helper()
						if _, err := obj.ObjectLayer.PutObject(ctx, obj.bucket, key, mustGetPutObjReader(t, strings.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
							t.Fatal(err)
						}
					}
					put("target", "original")
					if tc.key != "" {
						put(tc.key, "source")
					}
					requestURL := "http://localhost/" + obj.bucket + "/target"
					var uploadID string
					if endpoint == "CopyObjectPart" {
						var err error
						uploadID, err = obj.NewMultipartUpload(ctx, obj.bucket, "target", ObjectOptions{})
						if err != nil {
							t.Fatal(err)
						}
						defer obj.AbortMultipartUpload(ctx, obj.bucket, "target", uploadID, ObjectOptions{})
						requestURL = getCopyObjectPartURL("http://localhost", obj.bucket, "target", uploadID, "1")
					}
					req, err := newTestSignedRequestV4(http.MethodPut, requestURL, 0, nil, cred.AccessKey, cred.SecretKey, map[string]string{xhttp.AmzCopySource: "/" + obj.bucket + "/" + tc.source})
					if err != nil {
						t.Fatal(err)
					}
					obj.keys = nil
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, req)
					if tc.key == "" {
						assertObjectPathError(t, rec, ErrInvalidResourceName)
						if len(obj.keys) != 0 {
							t.Errorf("invalid source reached backend: %v", obj.keys)
						}
					} else if rec.Code != http.StatusOK || !reflect.DeepEqual(obj.keys, []string{tc.key}) {
						t.Fatalf("valid source: HTTP %d, keys %v: %s", rec.Code, obj.keys, rec.Body)
					}
					want := "original"
					if uploadID != "" {
						parts, err := obj.ListObjectParts(ctx, obj.bucket, "target", uploadID, 0, 10, ObjectOptions{})
						wantParts := 0
						if tc.key != "" {
							wantParts = 1
						}
						if err != nil || len(parts.Parts) != wantParts {
							t.Fatalf("parts: %v, err: %v", parts.Parts, err)
						}
					} else if tc.key != "" {
						want = "source"
					}
					r, err := obj.ObjectLayer.GetObjectNInfo(ctx, obj.bucket, "target", nil, nil, readLock, ObjectOptions{})
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					data, err := io.ReadAll(r)
					if err != nil || string(data) != want {
						t.Errorf("target = %q, want %q; err: %v", data, want, err)
					}
				})
			}
		})
	}
}

func TestPostPolicyPathValidation(t *testing.T) {
	obj, router := setupObjectPathTest(t, "PostPolicy")
	cred := globalActiveCred
	now := UTCNow()
	policy := []byte(fmt.Sprintf(`{"expiration":%q,"conditions":[{"bucket":%q},["starts-with","$key",""],{"x-amz-algorithm":"AWS4-HMAC-SHA256"},{"x-amz-date":%q},{"x-amz-credential":%q},{"x-amz-meta-uuid":"1234"}]}`,
		now.Add(time.Hour).Format(iso8601TimeFormat), obj.bucket, now.Format(iso8601DateFormat), getCredentialString(cred.AccessKey, globalServerRegion, now)))
	for _, tc := range []struct {
		key      string
		filename string
		invalid  bool
	}{
		{"workspaces/A/../B/secret", "upload.txt", true},
		{"workspaces/A/./file", "upload.txt", true},
		{"workspaces/A/..", "upload.txt", true},
		{"workspaces/A/${filename}/file", "..", true},
		{"workspaces/A/${filename}/file", ".", true},
		{"workspaces/A/${filename}", "upload.txt", false},
		{"workspaces/A/.hidden", "upload.txt", false},
		{"workspaces/A/file..txt", "upload.txt", false},
		{"workspaces/A/%2e%2e/file", "upload.txt", false},
		{"workspaces/A/dir/", "upload.txt", false},
	} {
		t.Run(tc.key+"/"+tc.filename, func(t *testing.T) {
			req, err := newPostRequestV4Generic("http://localhost", obj.bucket, "", nil, cred.AccessKey, cred.SecretKey, globalServerRegion, now, policy, map[string]string{"key": tc.key}, false, false)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(req.Body)
			if err != nil {
				t.Fatal(err)
			}
			req.Body.Close()
			body = bytes.Replace(body, []byte(`filename="upload.txt"`), []byte(`filename="`+tc.filename+`"`), 1)
			req.Body = io.NopCloser(bytes.NewReader(body))
			req.ContentLength = int64(len(body))
			obj.keys = nil
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if tc.invalid {
				assertObjectPathError(t, rec, ErrInvalidResourceName)
				if len(obj.keys) != 0 {
					t.Errorf("invalid key reached backend: %v", obj.keys)
				}
			} else {
				key := strings.ReplaceAll(tc.key, "${filename}", tc.filename)
				if rec.Code != http.StatusNoContent || !reflect.DeepEqual(obj.keys, []string{key}) {
					t.Errorf("valid key: HTTP %d, keys %v: %s", rec.Code, obj.keys, rec.Body)
				}
			}
		})
	}
}

func TestDeleteObjectsPathValidation(t *testing.T) {
	obj, router := setupObjectPathTest(t, "DeleteMultipleObjects")
	cred := globalActiveCred
	for _, tc := range []struct {
		quiet       bool
		invalidOnly bool
	}{{false, false}, {true, false}, {false, true}, {true, true}} {
		t.Run(fmt.Sprintf("quiet=%t/invalidOnly=%t", tc.quiet, tc.invalidOnly), func(t *testing.T) {
			objects := []ObjectToDelete{
				{ObjectName: "workspaces/A/../B/secret", VersionID: NullVersionID},
				{ObjectName: "workspaces/A/./file"},
				{ObjectName: "workspaces/A/.hidden"},
				{ObjectName: "workspaces/A/file..txt"},
				{ObjectName: "workspaces/A/%2e%2e/file"},
				{ObjectName: "workspaces/A/dir/"},
			}
			if tc.invalidOnly {
				objects = objects[:2]
			}
			body, err := xml.Marshal(DeleteObjectsRequest{Quiet: tc.quiet, Objects: objects})
			if err != nil {
				t.Fatal(err)
			}
			for _, validSignature := range []bool{true, false} {
				req, err := newTestSignedRequestV4(http.MethodPost, getDeleteMultipleObjectsURL("http://localhost", obj.bucket), int64(len(body)), bytes.NewReader(body), cred.AccessKey, cred.SecretKey, nil)
				if err != nil {
					t.Fatal(err)
				}
				if !validSignature {
					req.Header.Set(xhttp.AmzDate, UTCNow().Add(-time.Minute).Format(iso8601Format))
				}
				obj.keys = nil
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				if !validSignature {
					assertObjectPathError(t, rec, ErrSignatureDoesNotMatch)
					if len(obj.keys) != 0 {
						t.Errorf("unauthenticated delete reached backend: %v", obj.keys)
					}
					continue
				}
				var response DeleteObjectsResponse
				if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if rec.Code != http.StatusOK || len(response.Errors) != 2 {
					t.Fatalf("unexpected delete response: HTTP %d: %s", rec.Code, rec.Body)
				}
				for i, e := range response.Errors {
					if e.Key != objects[i].ObjectName || e.VersionID != objects[i].VersionID || e.Code != getAPIError(ErrInvalidResourceName).Code {
						t.Errorf("unexpected error: %+v", e)
					}
				}
				var wantKeys []string
				for _, object := range objects[2:] {
					wantKeys = append(wantKeys, object.ObjectName)
				}
				sort.Strings(obj.keys)
				sort.Strings(wantKeys)
				if !reflect.DeepEqual(obj.keys, wantKeys) {
					t.Errorf("backend keys = %v, want %v", obj.keys, wantKeys)
				}
				var deletedKeys []string
				for _, object := range response.DeletedObjects {
					if object.ObjectName != "" {
						deletedKeys = append(deletedKeys, object.ObjectName)
					}
				}
				sort.Strings(deletedKeys)
				if tc.quiet {
					wantKeys = nil
				}
				if !reflect.DeepEqual(deletedKeys, wantKeys) {
					t.Errorf("deleted keys = %v, want %v", deletedKeys, wantKeys)
				}
			}
		})
	}
}
