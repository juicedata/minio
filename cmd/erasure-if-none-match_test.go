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
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	humanize "github.com/dustin/go-humanize"
)

type blockingOneByteReader struct {
	started   chan struct{}
	release   chan struct{}
	delivered bool
}

func (r *blockingOneByteReader) Read(p []byte) (int, error) {
	if r.delivered {
		return 0, io.EOF
	}
	close(r.started)
	<-r.release
	r.delivered = true
	p[0] = 'x'
	return 1, nil
}

func TestWithNoLockPreservesObjectOptions(t *testing.T) {
	original := ObjectOptions{
		VersionSuspended: true,
		Versioned:        true,
		VersionID:        "version-id",
		IfNoneMatch:      true,
		UserDefined:      map[string]string{"key": "value"},
	}

	want := original
	want.NoLock = true
	if got := withNoLock(original); !reflect.DeepEqual(got, want) {
		t.Fatalf("withNoLock() changed options other than NoLock:\nwant: %#v\n got: %#v", want, got)
	}
	if original.NoLock {
		t.Fatal("withNoLock() mutated the input options")
	}
}

func TestRouteObjectOptionsPreservesVersioning(t *testing.T) {
	testCases := []ObjectOptions{
		{Versioned: true, VersionID: "version-id", NoLock: true},
		{VersionSuspended: true, VersionID: NullVersionID, NoLock: true},
	}
	for _, original := range testCases {
		want := original
		want.VersionID = ""
		want.NoLock = false
		if got := routeObjectOptions(original); !reflect.DeepEqual(got, want) {
			t.Fatalf("routeObjectOptions() lost versioning state:\nwant: %#v\n got: %#v", want, got)
		}
	}
}

func TestCrossPoolRouteRecheckPreservesVersioning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	z, roots := prepareTwoPoolErasure(t, ctx)
	defer z.Shutdown(context.Background())
	defer removeRoots(roots)

	const bucket = "route-versioning"
	if err := z.MakeBucketWithLocation(ctx, bucket, BucketOptions{}); err != nil {
		t.Fatal(err)
	}

	testCases := []struct {
		name string
		opts ObjectOptions
	}{
		{name: "versioned", opts: ObjectOptions{Versioned: true}},
		{name: "suspended", opts: ObjectOptions{VersionSuspended: true}},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			object := testCase.name + "-object"
			if _, err := z.serverPools[1].PutObject(ctx, bucket, object,
				mustGetPutObjReader(t, bytes.NewReader([]byte("data")), int64(len("data")), "", ""), testCase.opts); err != nil {
				t.Fatal(err)
			}

			disks := z.serverPools[1].sets[0].getDisks()
			metadataPath := pathJoin(object, xlStorageFormatFile)
			for _, disk := range disks[:3] {
				if err := disk.Delete(ctx, bucket, metadataPath, false); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := disks[3].ReadAll(ctx, bucket, metadataPath); err != nil {
				t.Fatalf("expected one metadata copy to remain before route recheck: %v", err)
			}

			targetPool := 0
			commitCtx := z.withCrossPoolCommit(ctx, bucket, object, &targetPool, testCase.opts)
			_, unlock, err := prepareForCommit(commitCtx)
			if err != nil {
				t.Fatalf("route recheck failed: %v", err)
			}
			unlock()

			if _, err := disks[3].ReadAll(ctx, bucket, metadataPath); err != nil {
				t.Fatalf("route recheck deleted dangling metadata for a %s bucket: %v", testCase.name, err)
			}
		})
	}
}

func TestCompleteMultipartIfNoneMatchFailurePreservesUpload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	obj, roots, err := prepareErasure16(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer obj.Shutdown(context.Background())
	defer removeRoots(roots)

	const (
		bucket = "multipart-condition"
		object = "object"
	)
	if err = obj.MakeBucketWithLocation(ctx, bucket, BucketOptions{}); err != nil {
		t.Fatal(err)
	}

	existing := []byte("existing")
	if _, err = obj.PutObject(ctx, bucket, object,
		mustGetPutObjReader(t, bytes.NewReader(existing), int64(len(existing)), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	existingInfo, err := obj.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	existingInfo.metadataOnly = true
	if _, err = obj.CopyObject(ctx, bucket, object, bucket, object, existingInfo,
		ObjectOptions{}, ObjectOptions{IfNoneMatch: true}); !isErrPreconditionFailed(err) {
		t.Fatalf("expected metadata-only copy precondition failure, got %v", err)
	}

	uploadID, err := obj.NewMultipartUpload(ctx, bucket, object, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	partOneData := bytes.Repeat([]byte("a"), 5*humanize.MiByte)
	partTwoData := []byte("second-part")
	partOne, err := obj.PutObjectPart(ctx, bucket, object, uploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(partOneData), int64(len(partOneData)), getMD5Hash(partOneData), ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	partTwo, err := obj.PutObjectPart(ctx, bucket, object, uploadID, 2,
		mustGetPutObjReader(t, bytes.NewReader(partTwoData), int64(len(partTwoData)), getMD5Hash(partTwoData), ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}

	_, err = obj.CompleteMultipartUpload(ctx, bucket, object, uploadID,
		[]CompletePart{{PartNumber: 2, ETag: partTwo.ETag}}, ObjectOptions{IfNoneMatch: true})
	if !isErrPreconditionFailed(err) {
		t.Fatalf("expected precondition failure, got %v", err)
	}

	if _, err = obj.DeleteObject(ctx, bucket, object, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	oi, err := obj.CompleteMultipartUpload(ctx, bucket, object, uploadID, []CompletePart{
		{PartNumber: 1, ETag: partOne.ETag},
		{PartNumber: 2, ETag: partTwo.ETag},
	}, ObjectOptions{})
	if err != nil {
		t.Fatalf("multipart upload was changed by the failed conditional completion: %v", err)
	}
	if want := int64(len(partOneData) + len(partTwoData)); oi.Size != want {
		t.Fatalf("unexpected completed object size: want %d, got %d", want, oi.Size)
	}
}

func TestServerPoolsIfNoneMatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	z, roots := prepareTwoPoolErasure(t, ctx)
	defer z.Shutdown(context.Background())
	defer removeRoots(roots)

	const bucket = "server-pool-condition"
	if err := z.MakeBucketWithLocation(ctx, bucket, BucketOptions{}); err != nil {
		t.Fatal(err)
	}

	const overwriteObject = "unconditional-multipart-overwrite"
	if _, err := z.serverPools[1].PutObject(ctx, bucket, overwriteObject,
		mustGetPutObjReader(t, bytes.NewReader([]byte("existing")), int64(len("existing")), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	overwriteUploadID, err := z.serverPools[0].NewMultipartUpload(ctx, bucket, overwriteObject, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	overwriteData := []byte("unconditional-multipart")
	overwritePart, err := z.serverPools[0].PutObjectPart(ctx, bucket, overwriteObject, overwriteUploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(overwriteData), int64(len(overwriteData)), getMD5Hash(overwriteData), ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = z.CompleteMultipartUpload(ctx, bucket, overwriteObject, overwriteUploadID,
		[]CompletePart{{PartNumber: 1, ETag: overwritePart.ETag}}, ObjectOptions{}); err != nil {
		t.Fatalf("unconditional multipart overwrite across pools failed: %v", err)
	}

	const existingObject = "existing-object"
	existing := []byte("stored-in-second-pool")
	if _, err := z.serverPools[1].PutObject(ctx, bucket, existingObject,
		mustGetPutObjReader(t, bytes.NewReader(existing), int64(len(existing)), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	_, err = z.PutObject(ctx, bucket, existingObject,
		mustGetPutObjReader(t, bytes.NewReader([]byte("replacement")), int64(len("replacement")), "", ""),
		ObjectOptions{IfNoneMatch: true})
	if !isErrPreconditionFailed(err) {
		t.Fatalf("expected object in another pool to fail the precondition, got %v", err)
	}

	uploadID, err := z.serverPools[0].NewMultipartUpload(ctx, bucket, existingObject, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	multipartData := []byte("multipart-data")
	part, err := z.serverPools[0].PutObjectPart(ctx, bucket, existingObject, uploadID, 1,
		mustGetPutObjReader(t, bytes.NewReader(multipartData), int64(len(multipartData)), getMD5Hash(multipartData), ""), ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	completeParts := []CompletePart{{PartNumber: 1, ETag: part.ETag}}
	if _, err = z.CompleteMultipartUpload(ctx, bucket, existingObject, uploadID, completeParts,
		ObjectOptions{IfNoneMatch: true}); !isErrPreconditionFailed(err) {
		t.Fatalf("expected multipart completion to find object in another pool, got %v", err)
	}
	if _, err = z.DeleteObject(ctx, bucket, existingObject, ObjectOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err = z.CompleteMultipartUpload(ctx, bucket, existingObject, uploadID, completeParts, ObjectOptions{}); err != nil {
		t.Fatalf("multipart upload was changed by cross-pool precondition failure: %v", err)
	}

	const concurrentObject = "concurrent-object"
	type putRequest struct {
		reader *PutObjReader
		etag   string
	}
	requests := make([]putRequest, 8)
	for i := range requests {
		payload := []byte{byte('a' + i)}
		requests[i] = putRequest{
			reader: mustGetPutObjReader(t, bytes.NewReader(payload), int64(len(payload)), "", ""),
			etag:   getMD5Hash(payload),
		}
	}

	type putResult struct {
		etag string
		err  error
	}
	results := make(chan putResult, len(requests))
	var wg sync.WaitGroup
	for i := range requests {
		wg.Add(1)
		go func(request putRequest) {
			defer wg.Done()
			_, putErr := z.PutObject(ctx, bucket, concurrentObject, request.reader, ObjectOptions{IfNoneMatch: true})
			results <- putResult{etag: request.etag, err: putErr}
		}(requests[i])
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("conditional puts did not complete; possible namespace-lock deadlock")
	}
	close(results)

	successes := 0
	winningETag := ""
	for result := range results {
		if result.err == nil {
			successes++
			winningETag = result.etag
			continue
		}
		if !isErrPreconditionFailed(result.err) {
			t.Fatalf("unexpected conditional put error: %v", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("expected exactly one successful conditional put, got %d", successes)
	}

	oi, err := z.GetObjectInfo(ctx, bucket, concurrentObject, ObjectOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if oi.ETag != winningETag {
		t.Fatalf("stored object ETag %q does not match successful request ETag %q", oi.ETag, winningETag)
	}

	const mixedObject = "mixed-conditional-object"
	mixedReaders := []*PutObjReader{
		mustGetPutObjReader(t, bytes.NewReader([]byte("conditional")), int64(len("conditional")), "", ""),
		mustGetPutObjReader(t, bytes.NewReader([]byte("unconditional")), int64(len("unconditional")), "", ""),
	}
	mixedResults := make(chan putResult, 2)
	start := make(chan struct{})
	go func() {
		<-start
		_, putErr := z.PutObject(ctx, bucket, mixedObject, mixedReaders[0], ObjectOptions{IfNoneMatch: true})
		mixedResults <- putResult{err: putErr}
	}()
	go func() {
		<-start
		_, putErr := z.PutObject(ctx, bucket, mixedObject, mixedReaders[1], ObjectOptions{})
		mixedResults <- putResult{etag: "unconditional", err: putErr}
	}()
	close(start)

	for i := 0; i < 2; i++ {
		result := <-mixedResults
		if result.etag != "unconditional" {
			if result.err != nil && !isErrPreconditionFailed(result.err) {
				t.Fatalf("unexpected conditional conflict error: %v", result.err)
			}
			continue
		}
		if result.err == nil {
			continue
		}
		if _, ok := result.err.(OperationTimedOut); !ok {
			t.Fatalf("unexpected unconditional conflict error: %v", result.err)
		}
		retryData := []byte("unconditional-retry")
		if _, err = z.PutObject(ctx, bucket, mixedObject,
			mustGetPutObjReader(t, bytes.NewReader(retryData), int64(len(retryData)), "", ""), ObjectOptions{}); err != nil {
			t.Fatalf("unconditional conflict retry failed: %v", err)
		}
	}

	storedPools := 0
	for _, pool := range z.serverPools {
		_, poolErr := pool.GetObjectInfo(ctx, bucket, mixedObject, ObjectOptions{})
		if poolErr == nil {
			storedPools++
			continue
		}
		if !isErrObjectNotFound(poolErr) {
			t.Fatal(poolErr)
		}
	}
	if storedPools != 1 {
		t.Fatalf("mixed conditional writes created object histories in %d pools", storedPools)
	}
}

func TestServerPoolsPutStagesBeforeCommitLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	z, roots := prepareTwoPoolErasure(t, ctx)
	defer z.Shutdown(context.Background())
	defer removeRoots(roots)

	const (
		bucket = "server-pool-staging"
		object = "object"
	)
	if err := z.MakeBucketWithLocation(ctx, bucket, BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	existing := []byte("existing")
	if _, err := z.serverPools[1].PutObject(ctx, bucket, object,
		mustGetPutObjReader(t, bytes.NewReader(existing), int64(len(existing)), "", ""), ObjectOptions{}); err != nil {
		t.Fatal(err)
	}

	reader := &blockingOneByteReader{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	putReader := mustGetPutObjReader(t, reader, 1, "", "")
	putDone := make(chan error, 1)
	go func() {
		_, err := z.PutObject(ctx, bucket, object, putReader, ObjectOptions{})
		putDone <- err
	}()

	select {
	case <-reader.started:
	case <-time.After(5 * time.Second):
		t.Fatal("put did not begin reading the request body")
	}

	getDone := make(chan error, 1)
	go func() {
		_, err := z.GetObjectInfo(ctx, bucket, object, ObjectOptions{})
		getDone <- err
	}()
	select {
	case err := <-getDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("request-body staging held the destination namespace lock")
	}

	destinationLock := z.serverPools[1].NewNSLock(bucket, object)
	if _, err := destinationLock.GetLock(ctx, globalOperationTimeout); err != nil {
		t.Fatal(err)
	}
	close(reader.release)
	select {
	case err := <-putDone:
		destinationLock.Unlock()
		t.Fatalf("put bypassed the destination pool lock: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	destinationLock.Unlock()

	select {
	case err := <-putDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("put did not finish after releasing the request body")
	}

	crossPoolLock := z.NewNSLock(MinioMetaBucket, pathJoin(crossPoolCommitLockPrefix, bucket, object))
	if _, err := crossPoolLock.GetLock(ctx, globalOperationTimeout); err != nil {
		t.Fatal(err)
	}
	commitReader := mustGetPutObjReader(t, bytes.NewReader([]byte("y")), 1, "", "")
	commitDone := make(chan error, 1)
	go func() {
		_, err := z.PutObject(ctx, bucket, object, commitReader, ObjectOptions{})
		commitDone <- err
	}()
	select {
	case err := <-commitDone:
		crossPoolLock.Unlock()
		t.Fatalf("unconditional put bypassed the cross-pool commit lock: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	crossPoolLock.Unlock()
	select {
	case err := <-commitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("unconditional put did not finish after releasing the cross-pool commit lock")
	}
}

func prepareTwoPoolErasure(t *testing.T, ctx context.Context) (*erasureServerPools, []string) {
	t.Helper()

	roots, err := getRandomDisks(8)
	if err != nil {
		t.Fatal(err)
	}
	endpointServerPools := EndpointServerPools{
		{
			SetCount:     1,
			DrivesPerSet: 4,
			Endpoints:    mustGetNewEndpoints(roots[:4]...),
		},
		{
			SetCount:     1,
			DrivesPerSet: 4,
			Endpoints:    mustGetNewEndpoints(roots[4:]...),
		},
	}
	obj, err := newTestObjectLayer(ctx, endpointServerPools)
	if err != nil {
		removeRoots(roots)
		t.Fatal(err)
	}
	return obj.(*erasureServerPools), roots
}
