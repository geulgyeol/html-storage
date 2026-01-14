package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/akamensky/argparse"
	"github.com/cockroachdb/pebble"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/gozstd"
)

var (
	filePushTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "html_storage_file_push_total",
		Help: "The total number of files pushed to the storage",
	})
)

var (
	fileWriteDuration = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "html_storage_file_write_duration_seconds",
		Help:       "Duration of file write operations",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
)

var (
	fileQueuingDuration = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "html_storage_file_queuing_duration_seconds",
		Help:       "Duration of file queuing operations",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
)

var (
	fileCompressionDuration = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "html_storage_file_compression_duration_seconds",
		Help:       "Duration of file compression operations",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
)

var (
	fileAddToPebbleDuration = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "html_storage_file_add_to_pebble_duration_seconds",
		Help:       "Duration of adding file metadata to Pebble DB",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
)

var (
	fileSaveDuration = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "html_storage_file_save_duration_seconds",
		Help:       "Duration of file save operations",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
)

var cdict *gozstd.CDict
var ddict *gozstd.DDict

func compressHTML(html string) []byte {
	//var buf bytes.Buffer
	//gz := gzip.NewWriter(&buf)
	//_, _ = gz.Write([]byte(html))
	//_ = gz.Close()

	start := time.Now()
	defer func() {
		fileCompressionDuration.Observe(time.Since(start).Seconds())
	}()

	compressedData := gozstd.CompressDict(nil, []byte(html), cdict)

	return compressedData
}

func getDir(dataPath string, timestamp int64) string {
	t := time.Unix(timestamp, 0)
	year, month, day := t.Date()

	return filepath.Join(dataPath, fmt.Sprintf("%d", year), fmt.Sprintf("%02d", month), fmt.Sprintf("%02d", day))
}

func getFilename(url string, blog string) string {
	// strip protocol
	url = strings.ReplaceAll(url, "http://", "")
	url = strings.ReplaceAll(url, "https://", "")

	// canonicalize url
	url = strings.ReplaceAll(url, "/", "_")

	// canonicalize query parameters
	url = strings.ReplaceAll(url, "?", "_")
	url = strings.ReplaceAll(url, "&", "_")
	url = strings.ReplaceAll(url, "=", "_")

	return fmt.Sprintf("%s_%s.html.zst", blog, url)
}

// dirCache caches known directories to avoid repeated MkdirAll calls
var dirCache sync.Map

func saveHTML(dir string, path string, compressedHTML []byte) error {
	start := time.Now()
	defer func() {
		fileSaveDuration.Observe(time.Since(start).Seconds())
	}()

	// Check cache before calling MkdirAll
	if _, exists := dirCache.Load(dir); !exists {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
		dirCache.Store(dir, struct{}{})
	}

	fullPath := filepath.Join(dir, path)
	file, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error closing file: %v\n", err)
		}
	}(file)

	// Use buffered writer for better write performance
	writer := bufio.NewWriterSize(file, 16*1024) // 16KB buffer
	_, err = writer.Write(compressedHTML)
	if err != nil {
		return err
	}

	return writer.Flush()
}

type FileInfo struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	CreatedAt string `json:"createdAt"`
}

type Job struct {
	ID              string
	Body            string
	Blog            string
	Timestamp       int64
	IsPrecompressed bool
	EnqueuedAt      time.Time
	ResultChan      chan error
}

var jobQueue chan Job

// atomic total - use atomic operations for lock-free updates
var estimatedTotal int64

// listFiles returns a paginated list of files in the data directory using cursor-based pagination
// cursor is the last key from the previous page (empty string for the first page)
func listFiles(db *pebble.DB, cursor string, pageSize int) ([]FileInfo, string, int64, error) {
	var files []FileInfo

	opts := &pebble.IterOptions{}
	if cursor != "" {
		// Set lower bound to cursor (exclusive) by appending a null byte
		opts.LowerBound = append([]byte(cursor), 0)
	}

	iter, err := db.NewIter(opts)
	if err != nil {
		return nil, "", 0, err
	}

	defer func(iter *pebble.Iterator) {
		err := iter.Close()
		if err != nil {
			fmt.Printf("Error closing iterator: %v\n", err)
		}
	}(iter)

	count := 0
	var nextCursor string
	for iter.First(); iter.Valid(); iter.Next() {
		if count >= pageSize {
			// Set the next cursor to the current key
			nextCursor = string(iter.Key())
			break
		}

		var dbValue FileMetadata
		value, err := iter.ValueAndErr()
		if err != nil {
			return nil, "", 0, err
		}

		err = json.Unmarshal(value, &dbValue)
		if err != nil {
			return nil, "", 0, err
		}
		files = append(files, FileInfo{
			Name:      dbValue.Name,
			Path:      dbValue.Path,
			Size:      dbValue.Size,
			CreatedAt: time.Unix(dbValue.Timestamp, 0).Format(time.RFC3339),
		})
		count++
	}

	return files, nextCursor, atomic.LoadInt64(&estimatedTotal), nil
}

// readFile reads a raw compressed HTML file
func readFile(dataPath, year, month, day, filename string) ([]byte, error) {
	filePath := filepath.Join(dataPath, year, month, day, filename)

	// Validate path to prevent directory traversal
	absDataPath, err := filepath.Abs(dataPath)
	if err != nil {
		return nil, err
	}
	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, err
	}
	// Clean paths and add separator suffix to ensure proper prefix matching
	cleanDataPath := filepath.Clean(absDataPath) + string(filepath.Separator)
	cleanFilePath := filepath.Clean(absFilePath)
	if !strings.HasPrefix(cleanFilePath, cleanDataPath) {
		return nil, fmt.Errorf("invalid file path")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error closing file: %v\n", err)
		}
	}(file)

	compressedData, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	return compressedData, nil
}

func decompressHTML(filename string, compressedData []byte) (string, error) {
	if strings.HasSuffix(filename, ".zst") {
		decompressedData, err := gozstd.DecompressDict(nil, compressedData, ddict)
		if err != nil {
			return "", err
		}
		return string(decompressedData), nil
	} else if strings.HasSuffix(filename, ".html.gz") {
		gz, err := gzip.NewReader(bytes.NewReader(compressedData))
		if err != nil {
			return "", err
		}
		defer func(gz *gzip.Reader) {
			err := gz.Close()
			if err != nil {
				fmt.Printf("Error closing gzip reader: %v\n", err)
			}
		}(gz)

		content, err := io.ReadAll(gz)
		if err != nil {
			return "", err
		}

		return string(content), nil
	}

	return "", fmt.Errorf("unsupported file format")
}

type FileMetadata struct {
	Name       string `json:"name"`
	Path       string `json:"path"`
	Timestamp  int64  `json:"timestamp"`
	Size       int64  `json:"size"`
	IsArchived bool   `json:"is_archived"`
}

func addFileToPebble(db *pebble.DB, name, filePath string, timestamp, size int64) error {
	start := time.Now()
	defer func() {
		fileAddToPebbleDuration.Observe(time.Since(start).Seconds())
	}()

	dbKey := []byte(filePath)
	dbValue := FileMetadata{
		Name:       name,
		Path:       filePath,
		Timestamp:  timestamp,
		Size:       size,
		IsArchived: false,
	}

	valueBytes, err := json.Marshal(dbValue)
	if err != nil {
		return err
	}

	// Use NoSync for better write throughput - Pebble will batch syncs
	return db.Set(dbKey, valueBytes, pebble.NoSync)
}

func getFileFromPebble(db *pebble.DB, filePath string) (FileMetadata, error) {
	// fmt.Printf("%s", filePath)
	dbKey := []byte(filePath)
	valueBytes, closer, err := db.Get(dbKey)
	if err != nil {
		return FileMetadata{}, err
	}
	defer func(closer io.Closer) {
		err := closer.Close()
		if err != nil {
			fmt.Printf("Error closing pebble value: %v\n", err)
		}
	}(closer)

	var dbValue FileMetadata
	err = json.Unmarshal(valueBytes, &dbValue)
	if err != nil {
		return FileMetadata{}, err
	}
	return dbValue, nil
}

func startWorkerPool(numWorkers int, db *pebble.DB, dataPath string) {
	jobQueue = make(chan Job, numWorkers*2)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for job := range jobQueue {
				start := time.Now()
				fileQueuingDuration.Observe(time.Since(job.EnqueuedAt).Seconds())

				var data []byte
				var err error

				if job.IsPrecompressed {
					data, err = base64.StdEncoding.DecodeString(job.Body)
					if err != nil {
						job.ResultChan <- fmt.Errorf("error decoding base64: %w", err)
						continue
					}
				} else {
					data = compressHTML(job.Body)
				}

				dir := getDir(dataPath, job.Timestamp)
				path := getFilename(job.ID, job.Blog)

				err = saveHTML(dir, path, data)
				if err != nil {
					job.ResultChan <- fmt.Errorf("error saving HTML: %w", err)
					continue
				}

				filePushTotal.Inc()
				atomic.AddInt64(&estimatedTotal, 1)

				err = addFileToPebble(db, path, filepath.Join(dir, path), job.Timestamp, int64(len(data)))
				if err != nil {
					job.ResultChan <- fmt.Errorf("error adding file metadata to Pebble: %w", err)
					// We continue even if pebble update fails, consistent with batch logic, or return error?
					// Original simple handler returned 500. Batch handler logged.
					// Let's rely on caller to decide.
					continue
				}

				fileWriteDuration.Observe(time.Since(start).Seconds())
				job.ResultChan <- nil
			}
		}()
	}
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	parser := argparse.NewParser("geulgyeol-html-storage", "A HTML storage server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	dataPathArg := parser.String("d", "data-path", &argparse.Options{Default: "/data", Help: "Path to store HTML files"})
	zstdDictionaryPath := parser.String("z", "zstd-dictionary", &argparse.Options{Default: "./zstd_dict", Help: "Path to Zstd dictionary file"})
	doZstdMigration := parser.Flag("", "do-zstd-migration", &argparse.Options{Help: "Perform background migration from Gzip to Zstd compression", Default: false})
	doPebbleMigration := parser.Flag("", "do-pebble-migration", &argparse.Options{Help: "Perform background migration to Pebble DB storage", Default: false})
	cleanArchivedFiles := parser.Flag("", "clean-archived-files", &argparse.Options{Help: "Clean up archived files to DB", Default: false})
	poolSize := parser.Int("", "worker-pool-size", &argparse.Options{Default: 32, Help: "Number of workers in the worker pool"})

	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	dataPath, err := filepath.Abs(*dataPathArg)
	if err != nil {
		panic(fmt.Sprintf("Failed to get absolute path of data directory: %v", err))
	}

	// Load Zstd dictionary
	dictData, err := os.ReadFile(*zstdDictionaryPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to read Zstd dictionary: %v", err))
	}

	cdict, err = gozstd.NewCDictLevel(dictData, 5)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Zstd dictionary: %v", err))
	}
	ddict, err = gozstd.NewDDict(dictData)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Zstd dictionary: %v", err))
	}

	// load pebble db for metadata
	pebblePath := filepath.Join(dataPath, "pebble_db")
	err = os.MkdirAll(pebblePath, os.ModePerm)
	if err != nil {
		panic(err)
	}
	db, err := pebble.Open(pebblePath, &pebble.Options{})
	if err != nil {
		panic(err)
	}

	defer func(db *pebble.DB) {
		err := db.Close()
		if err != nil {
			fmt.Printf("Error closing database: %v\n", err)
		}
	}(db)

	startWorkerPool(*poolSize, db, dataPath)

	// In background, find .gz files under dataPath and ungzip, then recompress with zstd and save
	if *doZstdMigration {
		go func() {
			err := filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}
				if !info.IsDir() && strings.HasSuffix(info.Name(), ".html.gz") {
					newPath := strings.TrimSuffix(path, ".gz") + ".zst"
					// read and ungzip
					file, err := os.Open(path)
					if err != nil {
						fmt.Printf("File open error during background recompression for %s: %v\n", path, err)
						return nil
					}
					gz, err := gzip.NewReader(file)
					if err != nil {
						_ = file.Close()
						fmt.Printf("Gzip reader open error during background recompression for %s: %v\n", path, err)
						return nil
					}
					content, err := io.ReadAll(gz)
					_ = gz.Close()
					_ = file.Close()
					if err != nil {
						fmt.Printf("Read error during background recompression for %s: %v\n", path, err)
						return nil
					}

					// recompress with zstd
					compressedData := gozstd.CompressDict(nil, content, cdict)

					// overwrite file with zstd compressed data
					err = os.WriteFile(newPath, compressedData, info.Mode())
					if err != nil {
						fmt.Printf("File write error during background recompression for %s: %v\n", path, err)
						return nil
					}
					if chtimesErr := os.Chtimes(newPath, info.ModTime(), info.ModTime()); chtimesErr != nil {
						fmt.Printf("File time preserve error during background recompression for %s: %v\n", path, chtimesErr)
					}
					// remove old .gz file
					err = os.Remove(path)
					if err != nil {
						fmt.Printf("File remove error during background recompression for %s: %v\n", path, err)
						return nil
					}
					fmt.Printf("Recompressed %s to %s\n", path, newPath)
				}
				return nil
			})
			if err != nil {
				fmt.Printf("Error during background recompression: %v\n", err)
			}
		}()
	}

	if *doPebbleMigration {
		go func() {
			err := filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if !info.IsDir() && (strings.HasSuffix(info.Name(), ".html.gz") || strings.HasSuffix(info.Name(), ".html.zst")) {
					// Use absolute path as key (path from Walk is already absolute)
					_, getErr := getFileFromPebble(db, path)
					if getErr != nil {
						// not found in pebble, add it
						addErr := addFileToPebble(db, info.Name(), path, info.ModTime().Unix(), info.Size())
						atomic.AddInt64(&estimatedTotal, 1)
						if addErr != nil {
							fmt.Printf("Error adding file to Pebble during migration for %s: %v\n", path, addErr)
						} else {
							fmt.Printf("Added file to Pebble during migration: %s\n", path)
						}
					}
				}
				return nil
			})
			if err != nil {
				fmt.Printf("Error during Pebble migration: %v\n", err)
			}
		}()
	}

	if *cleanArchivedFiles {
		go func() {
			// iter db
			iter, err := db.NewIter(&pebble.IterOptions{})
			if err != nil {
				fmt.Printf("Error creating iterator for cleaning archived files: %v\n", err)
				return
			}
			for iter.First(); iter.Valid(); iter.Next() {
				var dbValue FileMetadata
				value, err := iter.ValueAndErr()
				if err != nil {
					fmt.Printf("Error getting value during cleaning archived files: %v\n", err)
					continue
				}
				err = json.Unmarshal(value, &dbValue)
				if err != nil {
					fmt.Printf("Error unmarshaling value during cleaning archived files: %v\n", err)
					continue
				}
				if dbValue.IsArchived {
					// delete metadata entry
					err = db.Delete([]byte(dbValue.Path), pebble.Sync)
					if err != nil {
						fmt.Printf("Error deleting metadata during cleaning archived files for %s: %v\n", dbValue.Path, err)
						continue
					}
					fmt.Printf("Deleted archived file metadata from DB: %s\n", dbValue.Path)
				}
			}
			err = iter.Close()
		}()
	}

	// every 10 minutes, update estimated total from pebble db
	go func() {
		for {
			var count int64 = 0
			iter, err := db.NewIter(&pebble.IterOptions{})
			if err != nil {
				fmt.Printf("Error creating iterator for total count update: %v\n", err)
				continue
			}
			for iter.First(); iter.Valid(); iter.Next() {
				count++
			}
			err = iter.Close()
			if err != nil {
				fmt.Printf("Error closing iterator for total count update: %v\n", err)
			}
			atomic.StoreInt64(&estimatedTotal, count)

			time.Sleep(10 * time.Minute)
		}
	}()

	r := gin.Default()

	//r.Use(ginGzip.Gzip(ginGzip.DefaultCompression))

	// Prometheus metrics endpoint
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// List files endpoint with cursor-based pagination
	r.GET("/files", func(c *gin.Context) {
		cursor := c.DefaultQuery("cursor", "")

		pageSize, err := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
		if err != nil || pageSize < 1 {
			pageSize = 20
		}
		if pageSize > 100_000 {
			pageSize = 100_000
		}

		files, nextCursor, total, err := listFiles(db, cursor, pageSize)
		if err != nil {
			fmt.Printf("Error listing files: %v\n", err)
			c.JSON(500, gin.H{"error": "Failed to list files"})
			return
		}

		c.JSON(200, gin.H{
			"files":      files,
			"total":      total,
			"nextCursor": nextCursor,
			"pageSize":   pageSize,
		})
	})

	// Read file endpoint
	r.GET("/files/:year/:month/:day/:filename", func(c *gin.Context) {
		year := c.Param("year")
		month := c.Param("month")
		day := c.Param("day")
		filename := c.Param("filename")

		// check is archived from pebble db (use absolute path as key)
		filePath := filepath.Join(dataPath, year, month, day, filename)
		meta, err := getFileFromPebble(db, filePath)
		if err == nil {
			if meta.IsArchived {
				c.JSON(410, gin.H{"error": "File is archived"})
				return
			}
		} else {
			c.JSON(404, gin.H{"error": "File metadata not found"})
			return
		}

		content, err := readFile(dataPath, year, month, day, filename)
		if err != nil {
			if os.IsNotExist(err) {
				c.JSON(404, gin.H{"error": "File not found"})
				return
			}
			fmt.Printf("Error reading file: %v\n", err)
			c.JSON(500, gin.H{"error": "Failed to read file"})
			return
		}

		doDecompress := c.DefaultQuery("decompress", "true")
		if doDecompress == "true" {
			decompressedContent, err := decompressHTML(filename, content)
			if err != nil {
				fmt.Printf("Error decompressing file: %v\n", err)
				c.JSON(500, gin.H{"error": "Failed to decompress file"})
				return
			}
			c.JSON(200, gin.H{
				"content": decompressedContent,
				"path":    filepath.Join(year, month, day, filename),
			})
			return
		}

		// return raw compressed content as base64
		encodedContent := base64.StdEncoding.EncodeToString(content)
		c.JSON(200, gin.H{
			"content": encodedContent,
			"path":    filepath.Join(year, month, day, filename),
		})
	})

	r.POST("/files/:year/:month/:day/:filename/archive", func(c *gin.Context) {
		year := c.Param("year")
		month := c.Param("month")
		day := c.Param("day")
		filename := c.Param("filename")

		filePath := filepath.Join(dataPath, year, month, day, filename)
		meta, err := getFileFromPebble(db, filePath)
		if err != nil {
			c.JSON(404, gin.H{"error": "File not found"})
			return
		}

		meta.IsArchived = true
		valueBytes, err := json.Marshal(meta)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to archive file"})
			return
		}

		err = db.Set([]byte(filePath), valueBytes, pebble.Sync)
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to archive file"})
			return
		}

		// delete the actual file
		err = os.Remove(filePath)
		if err != nil {
			fmt.Printf("Error deleting archived file: %v\n", err)
		}

		c.JSON(200, gin.H{"status": "file archived"})
	})

	r.POST("/batch-get", func(c *gin.Context) {
		var req struct {
			Paths []string `json:"paths"`
		}

		if err := c.BindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": "Invalid JSON"})
			return
		}

		doDecompress := c.DefaultQuery("decompress", "true") == "true"
		results := make(map[string]interface{})
		var mu sync.Mutex
		var wg sync.WaitGroup

		// Limit concurrency to avoid overwhelming the system
		sem := make(chan struct{}, 16)

		for _, relPath := range req.Paths {
			wg.Add(1)
			go func(relPath string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				parts := strings.Split(relPath, "/")
				if len(parts) != 4 {
					mu.Lock()
					results[relPath] = gin.H{"error": "Invalid path format"}
					mu.Unlock()
					return
				}
				year, month, day, filename := parts[0], parts[1], parts[2], parts[3]

				filePath := filepath.Join(dataPath, year, month, day, filename)
				meta, err := getFileFromPebble(db, filePath)
				if err == nil {
					if meta.IsArchived {
						mu.Lock()
						results[relPath] = gin.H{"error": "File is archived", "code": 410}
						mu.Unlock()
						return
					}
				} else {
					mu.Lock()
					results[relPath] = gin.H{"error": "File metadata not found", "code": 404}
					mu.Unlock()
					return
				}

				content, err := readFile(dataPath, year, month, day, filename)
				if err != nil {
					mu.Lock()
					if os.IsNotExist(err) {
						results[relPath] = gin.H{"error": "File not found", "code": 404}
					} else {
						results[relPath] = gin.H{"error": fmt.Sprintf("Failed to read file: %v", err), "code": 500}
					}
					mu.Unlock()
					return
				}

				if doDecompress {
					decompressedContent, err := decompressHTML(filename, content)
					if err != nil {
						mu.Lock()
						results[relPath] = gin.H{"error": fmt.Sprintf("Failed to decompress file: %v", err), "code": 500}
						mu.Unlock()
						return
					}
					mu.Lock()
					results[relPath] = gin.H{
						"content": decompressedContent,
						"path":    relPath,
					}
					mu.Unlock()
				} else {
					encodedContent := base64.StdEncoding.EncodeToString(content)
					mu.Lock()
					results[relPath] = gin.H{
						"content": encodedContent,
						"path":    relPath,
					}
					mu.Unlock()
				}
			}(relPath)
		}

		wg.Wait()
		c.JSON(200, gin.H{"results": results})
	})

	r.POST("/:id", func(c *gin.Context) {
		var body struct {
			Body      string `json:"body"`
			Blog      string `json:"blog"`
			Timestamp int64  `json:"timestamp"`
		}

		if err := c.BindJSON(&body); err != nil {
			c.JSON(400, gin.H{"error": "Invalid JSON"})
			return
		}

		isPrecompressed := c.DefaultQuery("is_precompressed", "false") == "true"
		resultChan := make(chan error, 1)

		jobQueue <- Job{
			ID:              c.Param("id"),
			Body:            body.Body,
			Blog:            body.Blog,
			Timestamp:       body.Timestamp,
			IsPrecompressed: isPrecompressed,
			EnqueuedAt:      time.Now(),
			ResultChan:      resultChan,
		}

		err := <-resultChan
		if err != nil {
			fmt.Printf("%v\n", err)
			c.JSON(500, gin.H{"error": "Failed to save HTML"})
			return
		}

		c.JSON(200, gin.H{"status": "success"})
	})

	r.POST("/batch", func(c *gin.Context) {
		var body map[string]struct {
			Body      string `json:"body"`
			Blog      string `json:"blog"`
			Timestamp int64  `json:"timestamp"`
		}

		if err := c.BindJSON(&body); err != nil {
			c.JSON(400, gin.H{"error": "Invalid JSON"})
			return
		}

		isPrecompressed := c.DefaultQuery("is_precompressed", "false") == "true"

		resultChans := make([]chan error, 0, len(body))

		for id, fileData := range body {
			rc := make(chan error, 1)
			jobQueue <- Job{
				ID:              id,
				Body:            fileData.Body,
				Blog:            fileData.Blog,
				Timestamp:       fileData.Timestamp,
				IsPrecompressed: isPrecompressed,
				EnqueuedAt:      time.Now(),
				ResultChan:      rc,
			}
			resultChans = append(resultChans, rc)
		}

		// Wait for all workers to finish
		for _, rc := range resultChans {
			err := <-rc
			if err != nil {
				fmt.Printf("Error processing batch item: %v\n", err)
			}
		}

		c.JSON(200, gin.H{"status": "success"})
	})

	fmt.Printf("Starting server on port %d\n", *port)

	// run the server
	_ = r.Run(fmt.Sprintf(":%d", *port))
}
