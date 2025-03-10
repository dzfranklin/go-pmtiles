package pmtiles

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"

	// "github.com/dustin/go-humanize"
	"io"
	"log"
	"os"
)

// Show prints detailed information about an archive.
func Show(_ *log.Logger, output io.Writer, bucketURL string, key string, showHeaderJsonOnly bool, showMetadataOnly bool, showTilejson bool, showSizes bool, publicURL string, showTile bool, z int, x int, y int) error {
	ctx := context.Background()

	bucketURL, key, err := NormalizeBucketKey(bucketURL, "", key)

	if err != nil {
		return err
	}

	bucket, err := OpenBucket(ctx, bucketURL, "")

	if err != nil {
		return fmt.Errorf("Failed to open bucket for %s, %w", bucketURL, err)
	}
	defer bucket.Close()

	r, err := bucket.NewRangeReader(ctx, key, 0, 16384)

	if err != nil {
		return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return fmt.Errorf("Failed to read %s, %w", key, err)
	}
	r.Close()

	header, err := deserializeHeader(b[0:HeaderV3LenBytes])
	if err != nil {
		// check to see if it's a V2 file
		if string(b[0:2]) == "PM" {
			specVersion := b[2]
			return fmt.Errorf("PMTiles version %d detected; please use 'pmtiles convert' to upgrade to version 3", specVersion)
		}

		return fmt.Errorf("Failed to read %s, %w", key, err)
	}

	if !showTile {
		var tileType string
		switch header.TileType {
		case Mvt:
			tileType = "Vector Protobuf (MVT)"
		case Png:
			tileType = "Raster PNG"
		case Jpeg:
			tileType = "Raster Jpeg"
		case Webp:
			tileType = "Raster WebP"
		case Avif:
			tileType = "Raster AVIF"
		default:
			tileType = "Unknown"
		}

		metadataReader, err := bucket.NewRangeReader(ctx, key, int64(header.MetadataOffset), int64(header.MetadataLength))
		if err != nil {
			return fmt.Errorf("Failed to create range reader for %s, %w", key, err)
		}

		var metadataBytes []byte
		if header.InternalCompression == Gzip {
			r, _ := gzip.NewReader(metadataReader)
			metadataBytes, err = io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("Failed to read %s, %w", key, err)
			}
		} else {
			metadataBytes, err = io.ReadAll(metadataReader)
			if err != nil {
				return fmt.Errorf("Failed to read %s, %w", key, err)
			}
		}
		metadataReader.Close()

		if showMetadataOnly && showTilejson {
			return fmt.Errorf("cannot use more than one of --header-json, --metadata, and --tilejson together")
		}

		if showHeaderJsonOnly {
			fmt.Fprintln(output, headerToStringifiedJson(header))
		} else if showMetadataOnly {
			fmt.Fprintln(output, string(metadataBytes))
		} else if showTilejson {
			if publicURL == "" {
				// Using Fprintf instead of logger here, as this message should be written to Stderr in case
				// Stdout is being redirected.
				fmt.Fprintln(os.Stderr, "no --public-url specified; using placeholder tiles URL")
			}
			tilejsonBytes, err := CreateTileJSON(header, metadataBytes, publicURL)
			if err != nil {
				return fmt.Errorf("Failed to create tilejson for %s, %w", key, err)
			}
			fmt.Fprintln(output, string(tilejsonBytes))
		} else if showSizes {
			if err := doShowSizes(ctx, bucket, key, header); err != nil {
				return err
			}
		} else {
			fmt.Printf("pmtiles spec version: %d\n", header.SpecVersion)
			// fmt.Printf("total size: %s\n", humanize.Bytes(uint64(r.Size())))
			fmt.Printf("tile type: %s\n", tileType)
			fmt.Printf("bounds: (long: %f, lat: %f) (long: %f, lat: %f)\n", float64(header.MinLonE7)/10000000, float64(header.MinLatE7)/10000000, float64(header.MaxLonE7)/10000000, float64(header.MaxLatE7)/10000000)
			fmt.Printf("min zoom: %d\n", header.MinZoom)
			fmt.Printf("max zoom: %d\n", header.MaxZoom)
			fmt.Printf("center: (long: %f, lat: %f)\n", float64(header.CenterLonE7)/10000000, float64(header.CenterLatE7)/10000000)
			fmt.Printf("center zoom: %d\n", header.CenterZoom)
			fmt.Printf("addressed tiles count: %d\n", header.AddressedTilesCount)
			fmt.Printf("tile entries count: %d\n", header.TileEntriesCount)
			fmt.Printf("tile contents count: %d\n", header.TileContentsCount)
			fmt.Printf("clustered: %t\n", header.Clustered)
			fmt.Printf("internal compression: %d\n", header.InternalCompression)
			fmt.Printf("tile compression: %d\n", header.TileCompression)

			var metadataMap map[string]interface{}
			json.Unmarshal(metadataBytes, &metadataMap)
			for k, v := range metadataMap {
				switch v := v.(type) {
				case string:
					fmt.Println(k, v)
				default:
					fmt.Println(k, "<object...>")
				}
			}
		}
	} else {
		// write the tile to stdout

		tileID := ZxyToID(uint8(z), uint32(x), uint32(y))

		dirOffset := header.RootOffset
		dirLength := header.RootLength

		for depth := 0; depth <= 3; depth++ {
			r, err := bucket.NewRangeReader(ctx, key, int64(dirOffset), int64(dirLength))
			if err != nil {
				return fmt.Errorf("Network error")
			}
			defer r.Close()
			b, err := io.ReadAll(r)
			if err != nil {
				return fmt.Errorf("I/O Error")
			}
			directory := deserializeEntries(bytes.NewBuffer(b))
			entry, ok := findTile(directory, tileID)
			if ok {
				if entry.RunLength > 0 {
					tileReader, err := bucket.NewRangeReader(ctx, key, int64(header.TileDataOffset+entry.Offset), int64(entry.Length))
					if err != nil {
						return fmt.Errorf("Network error")
					}
					defer tileReader.Close()
					tileBytes, err := io.ReadAll(tileReader)
					if err != nil {
						return fmt.Errorf("I/O Error")
					}
					output.Write(tileBytes)
					break
				}
				dirOffset = header.LeafDirectoryOffset + entry.Offset
				dirLength = uint64(entry.Length)
			} else {
				fmt.Println("Tile not found in archive.")
				return nil
			}
		}
	}
	return nil
}

func doShowSizes(ctx context.Context, bucket Bucket, key string, header HeaderV3) error {
	entries, err := listAllEntries(ctx, bucket, key, header, header.RootOffset, header.RootLength)
	if err != nil {
		return err
	}

	byZoom := make([][]EntryV3, header.MaxZoom+1)
	for _, entry := range entries {
		z, _, _ := IDToZxy(entry.TileID)
		byZoom[z] = append(byZoom[z], entry)
	}

	for z := header.MinZoom; z <= header.MaxZoom; z++ {
		fmt.Printf("* Zoom %d\n\n", z)
		doShowEntries(byZoom[z])
		fmt.Print("\n\n")
	}

	fmt.Print("* All zoom levels\n\n")
	doShowEntries(entries)

	return nil
}

func doShowEntries(entries []EntryV3) {
	// Sort by size, descending

	slices.SortFunc(entries, func(a, b EntryV3) int {
		return int(b.Length) - int(a.Length)
	})

	// Compute stats

	histogramSizeList := make([]float64, 0, len(entries))
	totalSize := int64(0)
	for _, entry := range entries {
		totalSize += int64(entry.Length)
		histogramSizeList = append(histogramSizeList, float64(entry.Length))
	}

	averageSize := int64(math.Round(float64(totalSize) / float64(len(entries))))

	var medianSize int64
	if len(entries) == 0 {
		medianSize = 0
	} else if len(entries)%2 == 0 {
		medianSize = int64(math.Round(float64(entries[len(entries)/2-1].Length+entries[len(entries)/2].Length) / 2))
	} else {
		medianSize = int64(math.Round(float64(entries[len(entries)/2].Length)))
	}

	// Display

	fmt.Printf("median %s / average %s / count %s\n",
		formatByteCountIEC(medianSize), formatByteCountIEC(averageSize), formatCommas(int64(len(entries))))

	fmt.Println()
	hist := newHistogram(9, histogramSizeList)
	_ = histogramFPrintf(os.Stdout, hist, linearHistogramScale(10), func(v float64) string {
		return formatByteCountIEC(int64(math.Round(v)))
	})

	printLargestCount := min(len(entries), 5)
	fmt.Printf("\n%d largest tiles:\n", printLargestCount)
	for i, entry := range entries {
		if i > printLargestCount-1 {
			break
		}

		z, x, y := IDToZxy(entry.TileID)
		name := fmt.Sprintf("%d/%d/%d", z, x, y)

		size := formatByteCountIEC(int64(entry.Length))
		if entry.RunLength > 1 {
			size += fmt.Sprintf(" (run of %d)", entry.RunLength)
		}

		percentage := fmt.Sprintf("%.8g%%", float64(entry.RunLength)/float64(totalSize)*100)

		fmt.Println(name, size, percentage)
	}
}

func listAllEntries(ctx context.Context, bucket Bucket, key string, header HeaderV3, dirOffset, dirLength uint64) ([]EntryV3, error) {
	r, err := bucket.NewRangeReader(ctx, key, int64(dirOffset), int64(dirLength))
	if err != nil {
		return nil, fmt.Errorf("Network error")
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("I/O Error")
	}
	directory := deserializeEntries(bytes.NewBuffer(b))

	out := make([]EntryV3, 0, len(directory))
	for _, entry := range directory {
		if entry.RunLength > 0 {
			out = append(out, entry)
		} else {
			children, err := listAllEntries(ctx, bucket, key, header, header.LeafDirectoryOffset+entry.Offset, uint64(entry.Length))
			if err != nil {
				return nil, err
			}
			out = append(out, children...)
		}
	}
	return out, nil
}

func formatByteCountIEC(b int64) string {
	// From <https://yourbasic.org/golang/formatting-byte-size-to-human-readable-format/> (CC BY 3.0 license)
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB",
		float64(b)/float64(div), "KMGTPE"[exp])
}

func formatCommas(v int64) string {
	// From <https://github.com/dustin/go-humanize/blob/master/comma.go> (MIT license)
	sign := ""

	// Min int can't be negated to a usable value, so it has to be special cased.
	if v == math.MinInt64 {
		return "-9,223,372,036,854,775,808"
	}

	if v < 0 {
		sign = "-"
		v = 0 - v
	}

	parts := []string{"", "", "", "", "", "", ""}
	j := len(parts) - 1

	for v > 999 {
		parts[j] = strconv.FormatInt(v%1000, 10)
		switch len(parts[j]) {
		case 2:
			parts[j] = "0" + parts[j]
		case 1:
			parts[j] = "00" + parts[j]
		}
		v = v / 1000
		j--
	}
	parts[j] = strconv.Itoa(int(v))
	return sign + strings.Join(parts[j:], ",")
}

// Histogram code from <https://github.com/aybabtme/uniplot> (MIT license)

type histogram struct {
	// Min is the size of the smallest bucket.
	Min int
	// Max is the size of the biggest bucket.
	Max int
	// Count is the total size of all buckets.
	Count int
	// Buckets over which values are partitioned.
	Buckets []histogramBucket
}

// Bucket counts a partion of values.
type histogramBucket struct {
	// Count is the number of values represented in the bucket.
	Count int
	// Min is the low, inclusive bound of the bucket.
	Min float64
	// Max is the high, exclusive bound of the bucket. If
	// this bucket is the last bucket, the bound is inclusive
	// and contains the max value of the histogram.
	Max float64
}

func newHistogram(bins int, input []float64) histogram {
	if len(input) == 0 || bins == 0 {
		return histogram{}
	}

	minSize, maxSize := input[0], input[0]
	for _, val := range input {
		minSize = math.Min(minSize, val)
		maxSize = math.Max(maxSize, val)
	}

	if minSize == maxSize {
		return histogram{
			Min:     len(input),
			Max:     len(input),
			Count:   len(input),
			Buckets: []histogramBucket{{Count: len(input), Min: minSize, Max: maxSize}},
		}
	}

	scale := (maxSize - minSize) / float64(bins)
	buckets := make([]histogramBucket, bins)
	for i := range buckets {
		bmin, bmax := float64(i)*scale+minSize, float64(i+1)*scale+minSize
		buckets[i] = histogramBucket{Min: bmin, Max: bmax}
	}

	minC, maxC := 0, 0
	for _, val := range input {
		minx := float64(minSize)
		xdiff := val - minx
		bi := min(int(xdiff/scale), len(buckets)-1)
		if bi < 0 || bi >= len(buckets) {
			log.Panicf("bi=%d\tval=%f\txdiff=%f\tscale=%f\tlen(buckets)=%d", bi, val, xdiff, scale, len(buckets))
		}
		buckets[bi].Count++
		minC = min(minC, buckets[bi].Count)
		maxC = min(maxC, buckets[bi].Count)
	}

	return histogram{
		Min:     minC,
		Max:     maxC,
		Count:   len(input),
		Buckets: buckets,
	}
}

// Scale gives the scaled count of the bucket at idx, using the
// provided scale func.
func (h histogram) scale(s histogramScaleFunc, idx int) float64 {
	bkt := h.Buckets[idx]
	scale := s(h.Min, h.Max, bkt.Count)
	return scale
}

// histogramScaleFunc is the type to implement to scale an histogram.
type histogramScaleFunc func(min, max, value int) float64

var histogramBlocks = []string{
	"▏", "▎", "▍", "▌", "▋", "▊", "▉", "█",
}

var histogramBarstring = func(v float64) string {
	decimalf := (v - math.Floor(v)) * 10.0
	decimali := math.Floor(decimalf)
	charIdx := int(decimali / 10.0 * 8.0)
	return strings.Repeat("█", int(v)) + histogramBlocks[charIdx]
}

// linearHistogramScale builds a histogramScaleFunc that will linearly scale the values of
// an histogram so that they do not exceed width.
func linearHistogramScale(width int) histogramScaleFunc {
	return func(min, max, value int) float64 {
		if min == max {
			return 1
		}
		return float64(value-min) / float64(max-min) * float64(width)
	}
}

// histogramFormatFunc formats a float into the proper string form. Used to
// print meaningful axe labels.
type histogramFormatFunc func(v float64) string

func histogramFPrintf(w io.Writer, h histogram, s histogramScaleFunc, f histogramFormatFunc) error {
	tabw := tabwriter.NewWriter(w, 2, 2, 2, byte(' '), 0)

	yfmt := func(y int) string {
		if y > 0 {
			return strconv.Itoa(y)
		}
		return ""
	}

	for i, bkt := range h.Buckets {
		sz := h.scale(s, i)
		fmt.Fprintf(tabw, "%s - %s\t%.3g%%\t%s\n",
			f(bkt.Min), f(bkt.Max),
			float64(bkt.Count)*100.0/float64(h.Count),
			histogramBarstring(sz)+"\t"+yfmt(bkt.Count),
		)
	}

	return tabw.Flush()
}
