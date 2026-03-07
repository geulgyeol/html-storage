package main

import (
	"context"
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
	"github.com/geulgyeol/html-storage/db"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
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
	fileAddToDBDuration = promauto.NewSummary(prometheus.SummaryOpts{
		Name:       "html_storage_file_add_to_db_duration_seconds",
		Help:       "Duration of adding file metadata to DB",
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

func compressHTML(html string) []byte {
	start := time.Now()
	defer func() {
		fileCompressionDuration.Observe(time.Since(start).Seconds())
	}()

	compressedData := gozstd.CompressDict(nil, []byte(html), cdict)

	return compressedData
}

func getFilename(url string, blog string) string {
	url = strings.ReplaceAll(url, "http://", "")
	url = strings.ReplaceAll(url, "https://", "")

	url = strings.ReplaceAll(url, "/", "_")

	url = strings.ReplaceAll(url, "?", "_")
	url = strings.ReplaceAll(url, "&", "_")
	url = strings.ReplaceAll(url, "=", "_")

	return fmt.Sprintf("%s_%s.html.zst", blog, url)
}

// dirCache caches known directories to avoid repeated MkdirAll calls
var dirCache sync.Map

type BundleEntry struct {
	Name    string `json:"name"`
	Size    int    `json:"size"`
	Blog    string `json:"blog"`
	PostURL string `json:"post_url"`
	TS      int64  `json:"ts"`
}

var bundleMutexes sync.Map

func getBundleMutex(baseBundlePath string) *sync.Mutex {
	val, _ := bundleMutexes.LoadOrStore(baseBundlePath, &sync.Mutex{})
	return val.(*sync.Mutex)
}

func getBundlePath(dataPath string, timestamp int64) string {
	t := time.Unix(timestamp, 0)
	year, month, day := t.Date()
	hour := t.Hour()
	dir := filepath.Join(dataPath, fmt.Sprintf("%d", year), fmt.Sprintf("%02d", month), fmt.Sprintf("%02d", day))
	return filepath.Join(dir, fmt.Sprintf("%02d.bundle", hour))
}

func getActiveBundlePath(baseBundlePath string, maxBundleSize int64) string {
	info, err := os.Stat(baseBundlePath)
	if err != nil || info.Size() < maxBundleSize {
		return baseBundlePath
	}

	ext := filepath.Ext(baseBundlePath)
	base := strings.TrimSuffix(baseBundlePath, ext)
	for i := 1; ; i++ {
		segPath := fmt.Sprintf("%s_%d%s", base, i, ext)
		info, err := os.Stat(segPath)
		if err != nil || info.Size() < maxBundleSize {
			return segPath
		}
	}
}

func appendToBundle(bundlePath string, entry BundleEntry, data []byte, maxBundleSize int64) (actualBundlePath string, offset int64, length int64, err error) {
	actualBundlePath = getActiveBundlePath(bundlePath, maxBundleSize)

	dir := filepath.Dir(actualBundlePath)
	if _, exists := dirCache.Load(dir); !exists {
		if mkErr := os.MkdirAll(dir, os.ModePerm); mkErr != nil {
			return "", 0, 0, mkErr
		}
		dirCache.Store(dir, struct{}{})
	}

	f, err := os.OpenFile(actualBundlePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return "", 0, 0, err
	}
	defer f.Close()

	headerStart, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return "", 0, 0, err
	}

	entry.Size = len(data)
	headerBytes, err := json.Marshal(entry)
	if err != nil {
		return "", 0, 0, err
	}
	headerBytes = append(headerBytes, '\n')

	if _, err := f.Write(headerBytes); err != nil {
		return "", 0, 0, err
	}

	offset = headerStart + int64(len(headerBytes))
	length = int64(len(data))

	if _, err := f.Write(data); err != nil {
		return "", 0, 0, err
	}

	return actualBundlePath, offset, length, nil
}

func makeBundleDBPath(bundlePath string, offset, length int64) string {
	return fmt.Sprintf("%s?offset=%d&length=%d", bundlePath, offset, length)
}

func validatePathInDataRoot(dataPath, targetPath string) error {
	absDataPath, err := filepath.Abs(dataPath)
	if err != nil {
		return fmt.Errorf("failed to resolve data path: %w", err)
	}
	absTargetPath, err := filepath.Abs(targetPath)
	if err != nil {
		return fmt.Errorf("failed to resolve target path: %w", err)
	}
	if !strings.HasPrefix(filepath.Clean(absTargetPath), filepath.Clean(absDataPath)+string(filepath.Separator)) {
		return fmt.Errorf("invalid path")
	}
	return nil
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

var estimatedTotal int64

func listFiles(queries *db.Queries, cursor string, pageSize int) ([]FileInfo, string, int64, error) {
	params := db.ListBlogPostsParams{Limit: int32(pageSize + 1)}
	if cursor != "" {
		params.Cursor = pgtype.Text{String: cursor, Valid: true}
	}

	posts, err := queries.ListBlogPosts(context.Background(), params)
	if err != nil {
		return nil, "", 0, err
	}

	var nextCursor string
	if len(posts) > pageSize {
		nextCursor = posts[pageSize].Path
		posts = posts[:pageSize]
	}

	total, err := queries.CountBlogPosts(context.Background())
	if err != nil {
		return nil, "", 0, err
	}

	files := make([]FileInfo, 0, len(posts))
	for _, post := range posts {
		createdAt := ""
		if post.CreatedAt.Valid {
			createdAt = post.CreatedAt.Time.Format(time.RFC3339)
		}
		files = append(files, FileInfo{
			Name:      filepath.Base(post.Path),
			Path:      post.Path,
			Size:      0,
			CreatedAt: createdAt,
		})
	}

	return files, nextCursor, total, nil
}

func addFileToDB(queries *db.Queries, blog, postURL, filePath string, timestamp int64) error {
	start := time.Now()
	defer func() {
		fileAddToDBDuration.Observe(time.Since(start).Seconds())
	}()

	return queries.UpsertBlogPost(context.Background(), db.UpsertBlogPostParams{
		BlogPlatform: blog,
		PostUrl:      postURL,
		Path:         filePath,
		PublishedAt:  pgtype.Timestamp{Time: time.Unix(timestamp, 0), Valid: true},
	})
}

func startWorkerPool(numWorkers int, queries *db.Queries, dataPath string, maxBundleSize int64) {
	jobQueue = make(chan Job, numWorkers*2)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for job := range jobQueue {
				start := time.Now()
				fileQueuingDuration.Observe(time.Since(job.EnqueuedAt).Seconds())

				_, dbErr := queries.GetBlogPostByPostUrl(context.Background(), job.ID)
				if dbErr == nil {
					job.ResultChan <- nil
					continue
				}

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

				bundlePath := getBundlePath(dataPath, job.Timestamp)
				entry := BundleEntry{
					Name:    getFilename(job.ID, job.Blog),
					Blog:    job.Blog,
					PostURL: job.ID,
					TS:      job.Timestamp,
				}

				appendStart := time.Now()
				mu := getBundleMutex(bundlePath)
				mu.Lock()
				actualBundlePath, offset, length, appendErr := appendToBundle(bundlePath, entry, data, maxBundleSize)
				mu.Unlock()
				fileSaveDuration.Observe(time.Since(appendStart).Seconds())
				if appendErr != nil {
					job.ResultChan <- fmt.Errorf("error appending to bundle: %w", appendErr)
					continue
				}

				filePushTotal.Inc()
				atomic.AddInt64(&estimatedTotal, 1)

				dbPath := makeBundleDBPath(actualBundlePath, offset, length)
				err = addFileToDB(queries, job.Blog, job.ID, dbPath, job.Timestamp)
				if err != nil {
					job.ResultChan <- fmt.Errorf("error adding file metadata to DB: %w", err)
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
	postgresURL := parser.String("", "conn-string", &argparse.Options{Required: true, Help: "PostgreSQL connection URL"})
	zstdDictionaryPath := parser.String("z", "zstd-dictionary", &argparse.Options{Default: "./zstd_dict_v2", Help: "Path to Zstd dictionary file"})
	poolSize := parser.Int("", "worker-pool-size", &argparse.Options{Default: 32, Help: "Number of workers in the worker pool"})
	maxBundleSize := parser.Int("", "max-bundle-size", &argparse.Options{Default: 4294967296, Help: "Maximum bundle file size in bytes before creating a new segment"})

	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	dataPath, err := filepath.Abs(*dataPathArg)
	if err != nil {
		panic(fmt.Sprintf("Failed to get absolute path of data directory: %v", err))
	}

	dictData, err := os.ReadFile(*zstdDictionaryPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to read Zstd dictionary: %v", err))
	}

	cdict, err = gozstd.NewCDictLevel(dictData, 5)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Zstd dictionary: %v", err))
	}

	pool, err := pgxpool.New(context.Background(), *postgresURL)
	if err != nil {
		panic(err)
	}
	defer pool.Close()

	queries := db.New(pool)

	startWorkerPool(*poolSize, queries, dataPath, int64(*maxBundleSize))

	go func() {
		for {
			count, err := queries.CountBlogPosts(context.Background())
			if err != nil {
				fmt.Printf("Error getting total count: %v\n", err)
			} else {
				atomic.StoreInt64(&estimatedTotal, count)
			}

			time.Sleep(10 * time.Minute)
		}
	}()

	r := gin.Default()

	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	r.GET("/files", func(c *gin.Context) {
		cursor := c.DefaultQuery("cursor", "")

		pageSize, err := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
		if err != nil || pageSize < 1 {
			pageSize = 20
		}
		if pageSize > 100_000 {
			pageSize = 100_000
		}

		files, nextCursor, total, err := listFiles(queries, cursor, pageSize)
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

	r.GET("/bundles/:year/:month/:day/:filename", func(c *gin.Context) {
		year := c.Param("year")
		month := c.Param("month")
		day := c.Param("day")
		filename := c.Param("filename")

		bundlePath := filepath.Join(dataPath, year, month, day, filename)
		if err := validatePathInDataRoot(dataPath, bundlePath); err != nil {
			c.JSON(400, gin.H{"error": "Invalid path"})
			return
		}

		if _, err := os.Stat(bundlePath); os.IsNotExist(err) {
			c.JSON(404, gin.H{"error": "Bundle not found"})
			return
		}

		c.File(bundlePath)
	})

	r.DELETE("/bundles/:year/:month/:day/:filename", func(c *gin.Context) {
		year := c.Param("year")
		month := c.Param("month")
		day := c.Param("day")
		filename := c.Param("filename")

		bundlePath := filepath.Join(dataPath, year, month, day, filename)
		if err := validatePathInDataRoot(dataPath, bundlePath); err != nil {
			c.JSON(400, gin.H{"error": "Invalid path"})
			return
		}

		result, err := queries.DeleteBlogPostsByBundlePath(context.Background(), pgtype.Text{String: bundlePath, Valid: true})
		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to delete DB entries"})
			return
		}

		rowsDeleted := result.RowsAffected()

		err = os.Remove(bundlePath)
		if err != nil && !os.IsNotExist(err) {
			fmt.Printf("Error deleting bundle file %s: %v\n", bundlePath, err)
		}

		c.JSON(200, gin.H{
			"status":          "archived",
			"deleted_entries": rowsDeleted,
		})
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

		for _, rc := range resultChans {
			err := <-rc
			if err != nil {
				fmt.Printf("Error processing batch item: %v\n", err)
			}
		}

		c.JSON(200, gin.H{"status": "success"})
	})

	fmt.Printf("Starting server on port %d\n", *port)

	_ = r.Run(fmt.Sprintf(":%d", *port))
}
