package main

import (
	"fmt"
	"time"

	"github.com/progrium/keepsync/internal/config"
	"github.com/progrium/keepsync/internal/registry"
	"github.com/progrium/keepsync/internal/server"
)

// humanBytes prints n in decimal units, as disks and buckets are sold.
func humanBytes[T int64 | uint64](n T) string {
	v := float64(n)
	for _, unit := range []string{"B", "kB", "MB", "GB", "TB"} {
		if v < 1000 || unit == "TB" {
			if unit == "B" {
				return fmt.Sprintf("%d B", int64(v))
			}
			return fmt.Sprintf("%.1f %s", v, unit)
		}
		v /= 1000
	}
	return ""
}

func ago(t time.Time) string {
	return time.Since(t).Round(time.Second).String() + " ago"
}

func flushLine(last time.Time, errText string) string {
	switch {
	case errText != "":
		return "FAILING: " + errText
	case !last.IsZero():
		return "last " + ago(last)
	default:
		return "none yet"
	}
}

func bucketLine(bytes int64, at time.Time, errText string) string {
	switch {
	case errText != "":
		return "unknown (" + errText + ")"
	case at.IsZero():
		return "not measured yet"
	default:
		return fmt.Sprintf("%s (listed %s)", humanBytes(bytes), ago(at))
	}
}

// localLine is the local disk use for the server's mode.
func localLine(mode string, blobs, index, cache, cacheLimit int64) string {
	if mode == config.ModeCache {
		return fmt.Sprintf("cache %s of %s, index %s", humanBytes(cache), humanBytes(cacheLimit), humanBytes(index))
	}
	return fmt.Sprintf("blobs %s, index %s, cache %s", humanBytes(blobs), humanBytes(index), humanBytes(cache))
}

// printServerSummary prints the server lines of a node's status.
func printServerSummary(e *env, s *registry.ServerSummary, errText string) {
	if s == nil {
		if errText != "" {
			fmt.Fprintf(e.stdout, "  summary unavailable: %s\n", errText)
		}
		return
	}
	health := "healthy"
	if s.FlushErr != "" {
		health = "DEGRADED (flushes failing, so nothing new becomes durable)"
	}
	fmt.Fprintf(e.stdout, "  %s, %s mode, %d of %d nodes online\n", health, s.Mode, s.Online, s.Nodes)
	fmt.Fprintf(e.stdout, "  flush:  %s\n", flushLine(s.LastFlush, s.FlushErr))
	fmt.Fprintf(e.stdout, "  disk:   %s, %s free\n", localLine(s.Mode, s.BlobsBytes, s.IndexBytes, s.CacheBytes, s.CacheLimitBytes), humanBytes(s.VolumeFree))
	fmt.Fprintf(e.stdout, "  bucket: %s\n", bucketLine(s.BucketBytes, s.BucketAt, s.BucketErr))
}

// printDisk prints the disk section of server status.
func printDisk(e *env, d server.DiskUsage) {
	fmt.Fprintf(e.stdout, "mode:    %s\n", d.Mode)
	if d.MeasuredAt.IsZero() {
		fmt.Fprintln(e.stdout, "disk:    not measured yet")
	} else {
		if d.Mode == config.ModeStore {
			fmt.Fprintf(e.stdout, "disk:    blobs %s (local store), index %s, zerofs cache %s (limit %s)\n",
				humanBytes(d.BlobsBytes), humanBytes(d.IndexBytes), humanBytes(d.CacheBytes), humanBytes(d.CacheLimitBytes))
		} else {
			fmt.Fprintf(e.stdout, "disk:    zerofs cache %s of %s, index %s (no local blob store)\n",
				humanBytes(d.CacheBytes), humanBytes(d.CacheLimitBytes), humanBytes(d.IndexBytes))
		}
		fmt.Fprintf(e.stdout, "         %s in the server dir; volume %s free of %s\n",
			humanBytes(d.TotalBytes), humanBytes(d.VolumeFree), humanBytes(d.VolumeSize))
		if d.Err != "" {
			fmt.Fprintf(e.stdout, "         (measuring: %s)\n", d.Err)
		}
	}
	fmt.Fprintf(e.stdout, "bucket:  %s", bucketLine(d.Bucket.Bytes, d.Bucket.MeasuredAt, d.Bucket.Err))
	if d.Bucket.Err == "" && !d.Bucket.MeasuredAt.IsZero() {
		fmt.Fprintf(e.stdout, ", %d objects", d.Bucket.Objects)
	}
	fmt.Fprintln(e.stdout)
}
