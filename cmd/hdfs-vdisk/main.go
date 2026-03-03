package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/nirvam/go-hdfs-fsimage/pkg/exporter"
	"github.com/nirvam/go-hdfs-fsimage/pkg/fsimage"
)

func main() {
	fsimagePath := flag.String("i", "", "Input FSImage file path")
	outputType := flag.String("t", "csv", "Output type: csv or duckdb")
	outputPath := flag.String("o", "", "Output file path")

	flag.Parse()

	if *fsimagePath == "" {
		fmt.Println("Usage: hdfs-vdisk -i <fsimage> -t <csv|duckdb> -o <output>")
		flag.PrintDefaults()
		os.Exit(1)
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
	ctx := fsimage.NewPass1Context()
	log.Println("Starting Pass 1 (Indexing)...")
	if err := img.RunPass1(ctx); err != nil {
		log.Fatalf("Pass 1 failed: %v", err)
	}
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

	// 4. Pass 2
	log.Println("Starting Pass 2 (Processing)...")
	if err := img.RunPass2(ctx, exp); err != nil {
		log.Fatalf("Pass 2 failed: %v", err)
	}

	log.Printf("All tasks completed in %v. Output saved to: %s", time.Since(start), *outputPath)
}
