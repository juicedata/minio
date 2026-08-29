/*
 * MinIO Cloud Storage, (C) 2018,2019 MinIO, Inc.
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
	"os"
	"testing"
	"time"
)

// Tests ToObjectInfo function.
func TestCacheMetadataObjInfo(t *testing.T) {
	m := cacheMeta{Meta: nil}
	objInfo := m.ToObjectInfo("testbucket", "testobject")
	if objInfo.Size != 0 {
		t.Fatal("Unexpected object info value for Size", objInfo.Size)
	}
	if !objInfo.ModTime.Equal(time.Time{}) {
		t.Fatal("Unexpected object info value for ModTime ", objInfo.ModTime)
	}
	if objInfo.IsDir {
		t.Fatal("Unexpected object info value for IsDir", objInfo.IsDir)
	}
	if !objInfo.Expires.IsZero() {
		t.Fatal("Unexpected object info value for Expires ", objInfo.Expires)
	}
}

// test wildcard patterns for excluding entries from cache
func TestCacheExclusion(t *testing.T) {
	cobjects := &cacheObjects{
		cache: nil,
	}

	testCases := []struct {
		bucketName     string
		objectName     string
		excludePattern string
		expectedResult bool
	}{
		{"testbucket", "testobjectmatch", "testbucket/testobj*", true},
		{"testbucket", "testobjectnomatch", "testbucet/testobject*", false},
		{"testbucket", "testobject/pref1/obj1", "*/*", true},
		{"testbucket", "testobject/pref1/obj1", "*/pref1/*", true},
		{"testbucket", "testobject/pref1/obj1", "testobject/*", false},
		{"photos", "image1.jpg", "*.jpg", true},
		{"photos", "europe/paris/seine.jpg", "seine.jpg", false},
		{"photos", "europe/paris/seine.jpg", "*/seine.jpg", true},
		{"phil", "z/likes/coffee", "*/likes/*", true},
		{"failbucket", "no/slash/prefixes", "/failbucket/no/", false},
		{"failbucket", "no/slash/prefixes", "/failbucket/no/*", false},
	}

	for i, testCase := range testCases {
		cobjects.exclude = []string{testCase.excludePattern}
		if cobjects.isCacheExclude(testCase.bucketName, testCase.objectName) != testCase.expectedResult {
			t.Fatal("Cache exclusion test failed for case ", i)
		}
	}
}

func TestCacheWriteBackRejectsConditionalWrites(t *testing.T) {
	backendCalls := 0
	cobjects := &cacheObjects{
		commitWriteback: true,
		InnerPutObjectFn: func(ctx context.Context, bucket, object string, data *PutObjReader, opts ObjectOptions) (ObjectInfo, error) {
			backendCalls++
			return ObjectInfo{}, nil
		},
		InnerCopyObjectFn: func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
			backendCalls++
			return ObjectInfo{}, nil
		},
	}

	data := []byte("conditional")
	_, err := cobjects.PutObject(context.Background(), "bucket", "object",
		mustGetPutObjReader(t, bytes.NewReader(data), int64(len(data)), "", ""),
		ObjectOptions{IfNoneMatch: true})
	if !backendDownError(err) {
		t.Fatalf("expected write-back conditional PUT to fail safely, got %v", err)
	}

	_, err = cobjects.CopyObject(context.Background(), "source-bucket", "source", "destination-bucket", "destination",
		ObjectInfo{}, ObjectOptions{}, ObjectOptions{IfNoneMatch: true})
	if !backendDownError(err) {
		t.Fatalf("expected write-back conditional COPY to fail safely, got %v", err)
	}
	if backendCalls != 0 {
		t.Fatalf("conditional writes reached backend %d times", backendCalls)
	}
}

func TestCacheCopyObjectInvalidatesDestinationAfterBackendSuccess(t *testing.T) {
	ctx := context.Background()
	const (
		destinationBucket = "destination-bucket"
		destinationObject = "destination"
	)

	cacheRoot := t.TempDir()
	cacheLock := NewNSLock(false)
	dcache := &diskCache{dir: cacheRoot, online: 1}
	dcache.NewNSLockFn = func(cachePath string) RWLocker {
		return cacheLock.NewNSLock(nil, cachePath, "")
	}
	if err := os.MkdirAll(getCacheSHADir(cacheRoot, destinationBucket, destinationObject), 0777); err != nil {
		t.Fatal(err)
	}

	backendCalls := 0
	cobjects := &cacheObjects{
		cache: []*diskCache{dcache},
		InnerCopyObjectFn: func(ctx context.Context, srcBucket, srcObject, dstBucket, dstObject string, srcInfo ObjectInfo, srcOpts, dstOpts ObjectOptions) (ObjectInfo, error) {
			backendCalls++
			return ObjectInfo{Bucket: dstBucket, Name: dstObject}, nil
		},
	}

	_, err := cobjects.CopyObject(ctx, "source-bucket", "source", destinationBucket, destinationObject,
		ObjectInfo{}, ObjectOptions{}, ObjectOptions{IfNoneMatch: true})
	if err != nil {
		t.Fatal(err)
	}
	if backendCalls != 1 {
		t.Fatalf("expected one backend copy, got %d", backendCalls)
	}
	if dcache.Exists(ctx, destinationBucket, destinationObject) {
		t.Fatal("expected successful backend copy to invalidate cached destination")
	}
}
