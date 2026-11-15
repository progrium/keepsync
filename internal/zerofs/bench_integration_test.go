package zerofs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
	"perkeep.org/pkg/blobserver/diskpacked"
	"perkeep.org/pkg/blobserver/files"
	"perkeep.org/pkg/blobserver/localdisk"
)

// TestBlobStoreBench compares the ways a Perkeep blob store can sit on real
// zerofs: diskpacked through the FUSE mount (what cache mode used first),
// one file per blob through the FUSE mount, and one file per blob over 9P
// with no mount. For each it writes the same blobs, flushes, reads them
// back warm, then restarts zerofs with an empty cache and reads them back
// cold, from the bucket. It needs the TestRealZeroFS variables plus:
//
//	KEEPSYNC_ZEROFS_BENCH  comma-separated cases, or "all":
//	                       diskpacked-fuse, files-fuse, files-9p
//	KEEPSYNC_BENCH_BLOBS   blob count (default 2000)
//
// It logs one line per case; the bucket prefix of each is logged too, for
// counting objects.
func TestBlobStoreBench(t *testing.T) {
	cases := os.Getenv("KEEPSYNC_ZEROFS_BENCH")
	bin := os.Getenv("KEEPSYNC_ZEROFS_BIN")
	endpoint := os.Getenv("KEEPSYNC_TEST_S3_ENDPOINT")
	bucket := os.Getenv("KEEPSYNC_TEST_S3_BUCKET")
	if cases == "" || bin == "" || endpoint == "" || bucket == "" {
		t.Skip("set KEEPSYNC_ZEROFS_BENCH, KEEPSYNC_ZEROFS_BIN and KEEPSYNC_TEST_S3_*")
	}
	if cases == "all" {
		cases = "diskpacked-fuse,files-fuse,files-9p"
	}
	n := 2000
	if s := os.Getenv("KEEPSYNC_BENCH_BLOBS"); s != "" {
		n, _ = strconv.Atoi(s)
	}
	blobs := benchBlobs(n)
	var total int64
	for _, b := range blobs {
		total += int64(len(b))
	}
	t.Logf("%d blobs, %.1f MiB", len(blobs), float64(total)/(1<<20))

	for _, c := range strings.Split(cases, ",") {
		t.Run(c, func(t *testing.T) {
			layout, access, _ := strings.Cut(c, "-")
			mount := MountFUSE
			if access == "9p" {
				mount = MountNineP
			}
			pw, _ := GeneratePassword()
			dir, err := os.MkdirTemp("", "zfsb")
			if err != nil {
				t.Fatal(err)
			}
			defer os.RemoveAll(dir)
			prefix := "bench-" + c + "-" + time.Now().Format("150405")
			cfg := Config{
				Binary:  bin,
				DataDir: filepath.Join(dir, "data"),
				Storage: Storage{
					URL:             "s3://" + bucket + "/" + prefix,
					Endpoint:        endpoint,
					Region:          "us-east-1",
					AccessKeyID:     os.Getenv("KEEPSYNC_TEST_S3_KEY"),
					SecretAccessKey: os.Getenv("KEEPSYNC_TEST_S3_SECRET"),
					AllowHTTP:       true,
				},
				Password:     pw,
				Mount:        mount,
				CacheSizeGB:  DefaultCacheSizeGB,
				StartTimeout: time.Minute,
				Output:       io.Discard,
			}
			ctx := context.Background()
			start := func() (*Supervisor, blobserver.Storage) {
				s, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				if err := s.Start(ctx); err != nil {
					t.Fatal(err)
				}
				var sto blobserver.Storage
				switch {
				case layout == "diskpacked":
					sto, err = diskpacked.New(s.Dir())
				case mount == MountNineP:
					fs := s.FS()
					err = fs.MkdirAll("blobs", 0o700)
					sto = files.NewStorage(fs, "/blobs")
				default:
					os.MkdirAll(filepath.Join(s.Dir(), "blobs"), 0o700)
					sto, err = localdisk.New(filepath.Join(s.Dir(), "blobs"))
				}
				if err != nil {
					s.Stop(ctx)
					t.Fatal(err)
				}
				return s, sto
			}

			s, sto := start()
			t0 := time.Now()
			parallel(blobs, func(b []byte) error {
				_, err := blobserver.Receive(ctx, sto, blob.RefFromBytes(b), bytes.NewReader(b))
				return err
			}, t)
			write := time.Since(t0)
			t0 = time.Now()
			if err := s.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			flush := time.Since(t0)
			t0 = time.Now()
			readAll(ctx, sto, blobs, t)
			warm := time.Since(t0)
			if c, ok := sto.(io.Closer); ok {
				c.Close()
			}
			if err := s.Stop(ctx); err != nil {
				t.Fatal(err)
			}

			// Cold: a new cache, so every read goes to the bucket.
			os.RemoveAll(filepath.Join(cfg.DataDir, "cache"))
			t0 = time.Now()
			s, sto = start()
			open := time.Since(t0)
			t0 = time.Now()
			readAll(ctx, sto, blobs, t)
			cold := time.Since(t0)
			if c, ok := sto.(io.Closer); ok {
				c.Close()
			}
			s.Stop(ctx)

			mibs := func(d time.Duration) float64 { return float64(total) / (1 << 20) / d.Seconds() }
			t.Logf("RESULT %s: write %v (%.1f MiB/s, %.2f ms/blob), flush %v, warm read %v (%.1f MiB/s), start+open %v, cold read %v (%.1f MiB/s); prefix %s",
				c, write.Round(time.Millisecond), mibs(write), write.Seconds()*1000/float64(len(blobs)),
				flush.Round(time.Millisecond), warm.Round(time.Millisecond), mibs(warm),
				open.Round(time.Millisecond), cold.Round(time.Millisecond), mibs(cold), prefix)
		})
	}
}

// benchBlobs returns n random blobs shaped like a keepsync upload: one in
// ten a large 256 KiB chunk, the rest schema blobs and small chunks of
// 512 B to 64 KiB. Random bytes keep zerofs's compression out of it.
func benchBlobs(n int) [][]byte {
	r := rand.New(rand.NewPCG(1, 2))
	out := make([][]byte, n)
	for i := range out {
		size := 512 + r.IntN(64<<10)
		if i%10 == 0 {
			size = 256 << 10
		}
		b := make([]byte, size)
		for j := range b {
			b[j] = byte(r.Uint32())
		}
		out[i] = b
	}
	return out
}

func readAll(ctx context.Context, sto blobserver.Storage, blobs [][]byte, t *testing.T) {
	parallel(blobs, func(b []byte) error {
		rc, _, err := sto.Fetch(ctx, blob.RefFromBytes(b))
		if err != nil {
			return err
		}
		defer rc.Close()
		got, err := io.ReadAll(rc)
		if err == nil && !bytes.Equal(got, b) {
			err = fmt.Errorf("blob %d bytes read back wrong", len(b))
		}
		return err
	}, t)
}

// parallel runs fn on every blob with 8 workers, as perkeepd's uploads and
// syncs do.
func parallel(blobs [][]byte, fn func([]byte) error, t *testing.T) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	ch := make(chan []byte)
	for range 8 {
		wg.Go(func() {
			for b := range ch {
				if err := fn(b); err != nil {
					mu.Lock()
					if first == nil {
						first = err
					}
					mu.Unlock()
				}
			}
		})
	}
	for _, b := range blobs {
		ch <- b
	}
	close(ch)
	wg.Wait()
	if first != nil {
		t.Fatal(first)
	}
}
