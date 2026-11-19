package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/progrium/keepsync/internal/config"
)

// s3Bucket is a minimal path-style S3 client for the storage bucket, enough
// to list it (usage) and to check it at init. ZeroFS does the real I/O.
type s3Bucket struct {
	hc       *http.Client
	endpoint string // scheme://host, no trailing slash
	region   string
	bucket   string
	prefix   string // "" or ending in "/"
	creds    aws.Credentials
}

// newS3Bucket returns a client for an s3:// storage URL.
func newS3Bucket(hc *http.Client, st config.Storage) (*s3Bucket, error) {
	u, err := url.Parse(st.URL)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "s3" {
		return nil, fmt.Errorf("%s is not an s3:// URL", st.URL)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("%s has no bucket name", st.URL)
	}
	prefix := strings.TrimPrefix(u.Path, "/")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	region := st.Region
	if region == "" || region == "auto" && st.Endpoint == "" {
		region = "us-east-1"
	}
	endpoint := strings.TrimRight(st.Endpoint, "/")
	if endpoint == "" {
		endpoint = "https://s3." + region + ".amazonaws.com"
	}
	return &s3Bucket{
		hc: hc, endpoint: endpoint, region: region, bucket: u.Host, prefix: prefix,
		creds: aws.Credentials{AccessKeyID: st.AccessKeyID, SecretAccessKey: st.SecretAccessKey},
	}, nil
}

// do sends a signed request for key (relative to the bucket, not the
// prefix; "" is the bucket itself) and returns the status and body.
func (b *s3Bucket) do(ctx context.Context, method, key string, query url.Values, body []byte) (int, []byte, error) {
	u := b.endpoint + "/" + b.bucket
	if key != "" {
		u += "/" + (&url.URL{Path: key}).EscapedPath()
	}
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	req.ContentLength = int64(len(body))
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	req.Header.Set("X-Amz-Content-Sha256", hash)
	if b.creds.AccessKeyID != "" {
		if err := v4.NewSigner().SignHTTP(ctx, b.creds, req, hash, "s3", b.region, time.Now()); err != nil {
			return 0, nil, err
		}
	}
	resp, err := b.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	return resp.StatusCode, data, err
}

// s3Error describes a failed S3 response: the error code and message S3
// sends in its XML body, else the HTTP status.
func s3Error(status int, body []byte) error {
	var e struct {
		Code    string `xml:"Code"`
		Message string `xml:"Message"`
	}
	if xml.Unmarshal(body, &e) == nil && e.Code != "" {
		if e.Message != "" {
			return fmt.Errorf("%s: %s (HTTP %d)", e.Code, e.Message, status)
		}
		return fmt.Errorf("%s (HTTP %d)", e.Code, status)
	}
	return fmt.Errorf("HTTP %d %s", status, http.StatusText(status))
}

// CheckBucket checks that the storage bucket exists and that the
// credentials can write, read, and delete under its prefix, by doing each
// once with a small object. Other schemes (file:// is for testing), and
// s3:// without keys (credentials ZeroFS finds in its environment), are not
// checked.
func CheckBucket(ctx context.Context, hc *http.Client, st config.Storage) error {
	u, err := url.Parse(st.URL)
	if err != nil {
		return fmt.Errorf("bucket %s: %w", st.URL, err)
	}
	if u.Scheme != "s3" {
		return nil
	}
	if strings.HasPrefix(st.Endpoint, "http://") && !st.AllowHTTP {
		return fmt.Errorf("bucket endpoint %s is plain http: pass --allow-http to use it", st.Endpoint)
	}
	if st.AccessKeyID == "" || st.SecretAccessKey == "" {
		return nil
	}
	b, err := newS3Bucket(hc, st)
	if err != nil {
		return fmt.Errorf("bucket: %w", err)
	}
	var nonce [8]byte
	rand.Read(nonce[:])
	key := b.prefix + ".keepsync-init-check-" + hex.EncodeToString(nonce[:])
	where := "s3://" + b.bucket + "/" + key
	payload := []byte("keepsync init bucket check\n")
	fail := func(op string, err error) error {
		return fmt.Errorf("bucket check: %s %s: %w", op, where, err)
	}
	status, body, err := b.do(ctx, http.MethodPut, key, nil, payload)
	if err != nil {
		return fail("write", err)
	}
	if status/100 != 2 {
		return fail("write", s3Error(status, body))
	}
	status, body, err = b.do(ctx, http.MethodGet, key, nil, nil)
	if err == nil && status/100 != 2 {
		err = s3Error(status, body)
	}
	if err == nil && !bytes.Equal(body, payload) {
		err = fmt.Errorf("read back %d bytes that differ from the %d written", len(body), len(payload))
	}
	readErr := err
	status, body, err = b.do(ctx, http.MethodDelete, key, nil, nil)
	if err == nil && status/100 != 2 {
		err = s3Error(status, body)
	}
	if readErr != nil {
		return fail("read", readErr)
	}
	if err != nil {
		return fail("delete", err)
	}
	return nil
}
