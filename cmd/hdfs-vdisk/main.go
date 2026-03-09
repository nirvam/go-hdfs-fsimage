package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/nirvam/go-hdfs-fsimage/pkg/exporter"
	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage"
	"github.com/schollz/progressbar/v3"
)

func main() {
	fsimagePath := flag.String("i", "", "Input FSImage file path")
	outputType := flag.String("t", "csv", "Output type: csv or duckdb")
	outputPath := flag.String("o", "", "Output file path")
	cpuprofile := flag.String("cpuprofile", "", "Write cpu profile to `file`")
	memprofile := flag.String("memprofile", "", "Write memory profile to `file`")
	showStats := flag.Bool("stats", false, "Show detailed statistics after completion")

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for brevity
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	if *fsimagePath == "" {
		fmt.Println("Usage: hdfs-vdisk -i <fsimage> -t <csv|duckdb> -o <output>")
		flag.PrintDefaults()
		os.Exit(1)
	}

	info, err := os.Stat(*fsimagePath)
	if err != nil {
		log.Fatalf("Failed to stat FSImage: %v", err)
	}

	if *outputPath == "" {
		ext := "csv"
		if *outputType == "duckdb" {
			ext = "duckdb"
		}
		*outputPath = filepath.Base(*fsimagePath) + "." + ext
	}

	start := time.Now()

	// 1. New FSImage
	img, err := fsimage.NewFSImage(*fsimagePath)
	if err != nil {
		log.Fatalf("Failed to open FSImage: %v", err)
	}
	defer img.Close()

	log.Printf("FSImage loaded (LayoutVersion: %d, OnDiskVersion: %d)", img.Summary.GetLayoutVersion(), img.Summary.GetOndiskVersion())

	// 2. Pass 1
	pass1Start := time.Now()
	ctx := fsimage.NewPass1Context()
	bar1 := progressbar.NewOptions64(
		info.Size(),
		progressbar.OptionSetDescription("Pass 1 (Indexing)"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowBytes(true),
		progressbar.OptionSetWidth(15),
		progressbar.OptionThrottle(100*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionClearOnFinish(),
	)

	if err := img.RunPass1(ctx, bar1); err != nil {
		log.Fatalf("Pass 1 failed: %v", err)
	}
	pass1Duration := time.Since(pass1Start)
	fmt.Println() // New line after progress bar
	log.Printf("Pass 1 completed. Found %d strings, %d directory names, %d references, %d child->parent mappings.",
		len(ctx.StringTable), len(ctx.IDToName), len(ctx.RefList), len(ctx.ChildToParent))

	// 3. Setup Exporter
	var exp exporter.Exporter
	switch *outputType {
	case "csv":
		exp, err = exporter.NewCSVExporter(*outputPath)
	case "duckdb":
		exp, err = exporter.NewDuckDBExporter(*outputPath)
	default:
		log.Fatalf("Unsupported output type: %s", *outputType)
	}
	if err != nil {
		log.Fatalf("Failed to create exporter: %v", err)
	}
	defer exp.Close()

	if err := exp.ExportStringTable(ctx.StringTable); err != nil {
		log.Fatalf("Failed to export string table: %v", err)
	}

	// 4. Pass 2
	pass2Start := time.Now()
	bar2 := progressbar.NewOptions(-1,
		progressbar.OptionSetDescription("Pass 2 (Processing)"),
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionShowIts(),
		progressbar.OptionSetWidth(15),
		progressbar.OptionThrottle(500*time.Millisecond),
		progressbar.OptionClearOnFinish(),
	)

	if err := img.RunPass2(ctx, exp, bar2); err != nil {
		log.Fatalf("Pass 2 failed: %v", err)
	}
	pass2Duration := time.Since(pass2Start)
	fmt.Println()

	log.Printf("All tasks completed in %v. Output saved to: %s", time.Since(start), *outputPath)

	if *showStats {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		fmt.Printf("\n--- Runtime Statistics ---\n")
		fmt.Printf("Timing:\n")
		fmt.Printf("  Pass 1 (Index):  %v\n", pass1Duration)
		fmt.Printf("  Pass 2 (Export): %v\n", pass2Duration)
		fmt.Printf("  Total Elapsed:   %v\n", time.Since(start))

		fmt.Printf("\nThroughput:\n")
		numINodes := uint64(len(ctx.ChildToParent)) // Approximation of total INodes
		if numINodes > 0 && pass2Duration.Seconds() > 0 {
			fmt.Printf("  Processing Rate: %.2f INodes/sec\n", float64(numINodes)/pass2Duration.Seconds())
		}

		fmt.Printf("\nMemory & GC:\n")
		fmt.Printf("  Total Alloc:     %v MiB (Cumulative)\n", m.TotalAlloc/1024/1024)
		fmt.Printf("  Heap Alloc:      %v MiB (Current)\n", m.HeapAlloc/1024/1024)
		fmt.Printf("  Heap Objects:    %v\n", m.HeapObjects)
		fmt.Printf("  GC Cycles:       %v\n", m.NumGC)
		fmt.Printf("  Last GC Pause:   %v\n", time.Duration(m.PauseNs[(m.NumGC+255)%256]))
	}

	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}
