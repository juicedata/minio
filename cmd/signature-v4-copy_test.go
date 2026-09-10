package cmd

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7/pkg/signer"
	xhttp "github.com/minio/minio/cmd/http"
)

func TestExtractSignedCopyHeaders(t *testing.T) {
	for _, header := range []string{
		xhttp.AmzCopySource,
		xhttp.AmzCopySourceRange,
		xhttp.AmzCopySourceIfMatch,
		xhttp.AmzCopySourceIfNoneMatch,
		xhttp.AmzCopySourceIfModifiedSince,
		xhttp.AmzCopySourceIfUnmodifiedSince,
		"X-Amz-Copy-Source-Server-Side-Encryption-Customer-Key",
	} {
		t.Run(header, func(t *testing.T) {
			for _, value := range []string{"/source/secret", ""} {
				req := httptest.NewRequest(http.MethodPut, "http://localhost/destination/object", nil)
				req.Header.Set(header, value)
				if _, code := extractSignedHeaders([]string{"host"}, req); code != ErrUnsignedHeaders {
					t.Errorf("unsigned %s = %q: got %v, want ErrUnsignedHeaders", header, value, code)
				}
				if _, code := extractSignedHeaders([]string{"host", strings.ToLower(header)}, req); code != ErrNone {
					t.Errorf("signed %s = %q: got %v, want ErrNone", header, value, code)
				}
			}
		})
	}
	req := httptest.NewRequest(http.MethodPut, "http://localhost/destination/object", nil)
	req.Header.Set(xhttp.AmzCopySource, "/source/secret")
	req.Header.Set(xhttp.AmzCopySourceRange, "bytes=0-1")
	if _, code := extractSignedHeaders([]string{"host", "x-amz-copy-source"}, req); code != ErrUnsignedHeaders {
		t.Errorf("unsigned range on signed copy: got %v, want ErrUnsignedHeaders", code)
	}
}

func TestStreamingSignedCopyHeaders(t *testing.T) {
	cred := globalActiveCred
	req, err := newTestStreamingSignedRequest(http.MethodPut, "http://localhost/destination/object", 4, 4, strings.NewReader("data"), cred.AccessKey, cred.SecretKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, code := calculateSeedSignature(req); code != ErrNone {
		t.Fatalf("normal streaming signature: %v", code)
	}
	req.Header.Set(xhttp.AmzCopySource, "/source/secret")
	if _, _, _, _, code := calculateSeedSignature(req); code != ErrUnsignedHeaders {
		t.Errorf("unsigned copy header on streaming request: got %v, want ErrUnsignedHeaders", code)
	}
}

// Exercise real PUT/copy routing with the existing filesystem backend and SDK signer.
func TestSigV4CopyHeaderAuthorization(t *testing.T) {
	resetTestGlobals()
	defer resetTestGlobals()
	ctx := context.Background()
	obj, dir, err := prepareFS()
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	defer obj.Shutdown(ctx)
	if err = newTestConfig(globalMinioDefaultRegion, obj); err != nil {
		t.Fatal(err)
	}
	bucket, _, err := initAPIHandlerTest(obj, []string{"CopyObject"})
	if err != nil {
		t.Fatal(err)
	}
	router := initTestAPIEndPoints(obj, nil)
	otherBucket := getRandomBucketName()
	if err = obj.MakeBucketWithLocation(ctx, otherBucket, BucketOptions{}); err != nil {
		t.Fatal(err)
	}
	const original = "original upload"
	const secret = "source secret"
	put := func(t *testing.T, bucket, key, data string) {
		t.Helper()
		if _, err := obj.PutObject(ctx, bucket, key, mustGetPutObjReader(t, strings.NewReader(data), int64(len(data)), "", ""), ObjectOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	for _, sourceBucket := range []string{bucket, otherBucket} {
		put(t, sourceBucket, "secret", secret)
		for _, presigned := range []bool{false, true} {
			for _, multipart := range []bool{false, true} {
				for _, scenario := range []string{"upload", "unsigned-copy", "signed-copy", "tampered-copy"} {
					name := sourceBucket + "/authorization/"
					if presigned {
						name = sourceBucket + "/presigned/"
					}
					if multipart {
						name += "part/"
					}
					name += scenario
					t.Run(name, func(t *testing.T) {
						const target = "target"
						put(t, bucket, target, original)
						requestURL := "http://localhost/" + bucket + "/" + target
						uploadID := ""
						if multipart {
							uploadID, err = obj.NewMultipartUpload(ctx, bucket, target, ObjectOptions{})
							if err != nil {
								t.Fatal(err)
							}
							defer obj.AbortMultipartUpload(ctx, bucket, target, uploadID, ObjectOptions{})
							if _, err := obj.PutObjectPart(ctx, bucket, target, uploadID, 1, mustGetPutObjReader(t, strings.NewReader(original), int64(len(original)), "", ""), ObjectOptions{}); err != nil {
								t.Fatal(err)
							}
							requestURL = getPutObjectPartURL("http://localhost", bucket, target, uploadID, "1")
						}
						body := ""
						if scenario == "upload" {
							body = "new upload"
						}
						req, err := http.NewRequest(http.MethodPut, requestURL, strings.NewReader(body))
						if err != nil {
							t.Fatal(err)
						}
						if scenario == "signed-copy" || scenario == "tampered-copy" {
							req.Header.Set(xhttp.AmzCopySource, "/"+sourceBucket+"/secret")
						}
						cred := globalActiveCred
						if presigned {
							req = signer.PreSignV4(*req, cred.AccessKey, cred.SecretKey, "", globalServerRegion, 60)
						} else {
							req.Header.Set(xhttp.AmzContentSha256, unsignedPayload)
							if err = signRequestV4(req, cred.AccessKey, cred.SecretKey); err != nil {
								t.Fatal(err)
							}
						}
						wantCode := ErrNone
						wantData := body
						switch scenario {
						case "unsigned-copy":
							req.Header.Set(xhttp.AmzCopySource, "/"+sourceBucket+"/secret")
							wantCode, wantData = ErrUnsignedHeaders, original
						case "tampered-copy":
							req.Header.Set(xhttp.AmzCopySource, "/"+sourceBucket+"/target")
							wantCode, wantData = ErrSignatureDoesNotMatch, original
						case "signed-copy":
							wantData = secret
						}
						rec := httptest.NewRecorder()
						req.RequestURI = req.URL.RequestURI()
						router.ServeHTTP(rec, req)
						wantStatus := http.StatusOK
						if wantCode != ErrNone {
							wantStatus = getAPIError(wantCode).HTTPStatusCode
							var response APIErrorResponse
							if err := xml.Unmarshal(rec.Body.Bytes(), &response); err != nil {
								t.Errorf("decode rejection: %v; body: %s", err, rec.Body)
							} else if response.Code != getAPIError(wantCode).Code {
								t.Errorf("error code = %s, want %s", response.Code, getAPIError(wantCode).Code)
							}
						}
						if rec.Code != wantStatus {
							t.Errorf("HTTP %d, want %d: %s", rec.Code, wantStatus, rec.Body)
						}
						if multipart {
							parts, err := obj.ListObjectParts(ctx, bucket, target, uploadID, 0, 10, ObjectOptions{})
							if err != nil || len(parts.Parts) != 1 {
								t.Fatalf("list parts: %v, parts: %v", err, parts.Parts)
							}
							if _, err = obj.CompleteMultipartUpload(ctx, bucket, target, uploadID, []CompletePart{{PartNumber: 1, ETag: parts.Parts[0].ETag}}, ObjectOptions{}); err != nil {
								t.Fatal(err)
							}
						}
						reader, err := obj.GetObjectNInfo(ctx, bucket, target, nil, nil, readLock, ObjectOptions{})
						if err != nil {
							t.Fatal(err)
						}
						defer reader.Close()
						data, err := io.ReadAll(reader)
						if err != nil || string(data) != wantData {
							t.Errorf("target = %q, want %q; error: %v", data, wantData, err)
						}
					})
				}
			}
		}
	}
}
