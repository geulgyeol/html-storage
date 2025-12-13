package main

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

var cdict *gozstd.CDict
var ddict *gozstd.DDict

func compressHTML(html string) []byte {
	//var buf bytes.Buffer
	//gz := gzip.NewWriter(&buf)
	//_, _ = gz.Write([]byte(html))
	//_ = gz.Close()

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

func saveHTML(dir string, path string, compressedHTML []byte) error {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return err
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

	_, err = file.Write(compressedHTML)
	if err != nil {
		return err
	}

	return nil
}

type FileInfo struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	CreatedAt string `json:"createdAt"`
}

// listFiles returns a paginated list of files in the data directory
func listFiles(db *pebble.DB, page, pageSize int) ([]FileInfo, int64, error) {
	var files []FileInfo
	iter, err := db.NewIter(&pebble.IterOptions{})
	if err != nil {
		return nil, 0, err
	}

	defer func(iter *pebble.Iterator) {
		err := iter.Close()
		if err != nil {
			fmt.Printf("Error closing iterator: %v\n", err)
		}
	}(iter)

	total := db.Metrics().Compact.Count

	skipped := (page - 1) * pageSize
	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		if skipped > 0 {
			skipped--
			continue
		}
		if count >= pageSize {
			break
		}
		var dbValue FileMetadata
		err := json.Unmarshal(iter.Value(), &dbValue)
		if err != nil {
			return nil, 0, err
		}
		files = append(files, FileInfo{
			Name:      dbValue.Name,
			Path:      dbValue.Path,
			Size:      dbValue.Size,
			CreatedAt: time.Unix(dbValue.Timestamp, 0).Format(time.RFC3339),
		})
		count++
	}

	return files, total, nil
}

// readFile reads and decompresses a stored HTML file
func readFile(dataPath, year, month, day, filename string) (string, error) {
	filePath := filepath.Join(dataPath, year, month, day, filename)

	// Validate path to prevent directory traversal
	absDataPath, err := filepath.Abs(dataPath)
	if err != nil {
		return "", err
	}
	absFilePath, err := filepath.Abs(filePath)
	if err != nil {
		return "", err
	}
	// Clean paths and add separator suffix to ensure proper prefix matching
	cleanDataPath := filepath.Clean(absDataPath) + string(filepath.Separator)
	cleanFilePath := filepath.Clean(absFilePath)
	if !strings.HasPrefix(cleanFilePath, cleanDataPath) {
		return "", fmt.Errorf("invalid file path")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			fmt.Printf("Error closing file: %v\n", err)
		}
	}(file)

	if strings.HasSuffix(filename, ".zst") {
		compressedData, err := io.ReadAll(file)
		if err != nil {
			return "", err
		}
		decompressedData, err := gozstd.DecompressDict(nil, compressedData, ddict)
		if err != nil {
			return "", err
		}
		return string(decompressedData), nil
	} else if strings.HasSuffix(filename, ".html.gz") {
		gz, err := gzip.NewReader(file)
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

	return db.Set(dbKey, valueBytes, pebble.Sync)
}

func getFileFromPebble(db *pebble.DB, filePath string) (FileMetadata, error) {
	dbKey := []byte(filePath)
	valueBytes, closer, err := db.Get(dbKey)
	if err != nil {
		return FileMetadata{}, err
	}
	_ = closer.Close()

	var dbValue FileMetadata
	err = json.Unmarshal(valueBytes, &dbValue)
	if err != nil {
		return FileMetadata{}, err
	}
	return dbValue, nil
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	parser := argparse.NewParser("geulgyeol-html-storage", "A HTML storage server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	dataPath := parser.String("d", "data-path", &argparse.Options{Default: "/data", Help: "Path to store HTML files"})
	zstdDictionaryPath := parser.String("z", "zstd-dictionary", &argparse.Options{Default: "./zstd_dict", Help: "Path to Zstd dictionary file"})
	doZstdMigration := parser.Flag("", "do-zstd-migration", &argparse.Options{Help: "Perform background migration from Gzip to Zstd compression", Default: false})
	doPebbleMigration := parser.Flag("", "do-pebble-migration", &argparse.Options{Help: "Perform background migration to Pebble DB storage", Default: false})

	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	// Load Zstd dictionary
	dictData, err := os.ReadFile(*zstdDictionaryPath)
	if err != nil {
		panic(fmt.Sprintf("Failed to read Zstd dictionary: %v", err))
	}

	cdict, err = gozstd.NewCDictLevel(dictData, 9)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Zstd dictionary: %v", err))
	}
	ddict, err = gozstd.NewDDict(dictData)
	if err != nil {
		panic(fmt.Sprintf("Failed to create Zstd dictionary: %v", err))
	}

	// load pebble db for metadata
	pebblePath := filepath.Join(*dataPath, "pebble_db")
	os.MkdirAll(pebblePath, os.ModePerm)
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

	// In background, find .gz files under dataPath and ungzip, then recompress with zstd and save
	if *doZstdMigration {
		go func() {
			err := filepath.Walk(*dataPath, func(path string, info os.FileInfo, err error) error {
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
			err := filepath.Walk(*dataPath, func(path string, info os.FileInfo, err error) error {
				if err != nil {
					return err
				}

				if !info.IsDir() && (strings.HasSuffix(info.Name(), ".html.gz") || strings.HasSuffix(info.Name(), ".html.zst")) {
					relPath, relErr := filepath.Rel(*dataPath, path)
					if relErr != nil {
						return nil
					}
					_, getErr := getFileFromPebble(db, relPath)
					if getErr != nil {
						// not found in pebble, add it
						addErr := addFileToPebble(db, info.Name(), relPath, info.ModTime().Unix(), info.Size())
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

	r := gin.Default()

	//r.Use(ginGzip.Gzip(ginGzip.DefaultCompression))

	// Prometheus metrics endpoint
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok"})
	})

	// List files endpoint with pagination
	r.GET("/files", func(c *gin.Context) {
		page, err := strconv.Atoi(c.DefaultQuery("page", "1"))
		if err != nil || page < 1 {
			page = 1
		}

		pageSize, err := strconv.Atoi(c.DefaultQuery("pageSize", "20"))
		if err != nil || pageSize < 1 {
			pageSize = 20
		}
		if pageSize > 100_000 {
			pageSize = 100_000
		}

		files, total, err := listFiles(db, page, pageSize)
		if err != nil {
			fmt.Printf("Error listing files: %v\n", err)
			c.JSON(500, gin.H{"error": "Failed to list files"})
			return
		}

		c.JSON(200, gin.H{
			"files":    files,
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
		})
	})

	// Read file endpoint
	r.GET("/files/:year/:month/:day/:filename", func(c *gin.Context) {
		year := c.Param("year")
		month := c.Param("month")
		day := c.Param("day")
		filename := c.Param("filename")

		// check is archived from pebble db
		relPath := filepath.Join(year, month, day, filename)
		meta, err := getFileFromPebble(db, relPath)
		if err == nil {
			if meta.IsArchived {
				c.JSON(410, gin.H{"error": "File is archived"})
				return
			}
		}

		content, err := readFile(*dataPath, year, month, day, filename)
		if err != nil {
			if os.IsNotExist(err) {
				c.JSON(404, gin.H{"error": "File not found"})
				return
			}
			fmt.Printf("Error reading file: %v\n", err)
			c.JSON(500, gin.H{"error": "Failed to read file"})
			return
		}

		c.JSON(200, gin.H{
			"content": content,
			"path":    filepath.Join(year, month, day, filename),
		})
	})

	r.POST("/:id", func(c *gin.Context) {
		var json struct {
			Body      string `json:"body"`
			Blog      string `json:"blog"`
			Timestamp int64  `json:"timestamp"`
		}

		if err := c.BindJSON(&json); err != nil {
			c.JSON(400, gin.H{"error": "Invalid JSON"})
			return
		}

		go func() {
			compressedHTML := compressHTML(json.Body)
			dir := getDir(*dataPath, json.Timestamp)
			path := getFilename(c.Param("id"), json.Blog)

			err := saveHTML(dir, path, compressedHTML)
			if err != nil {
				fmt.Printf("Error saving HTML: %v\n", err)
			} else {
				filePushTotal.Inc()

				err = addFileToPebble(db, path, filepath.Join(filepath.Base(dir), path), json.Timestamp, int64(len(compressedHTML)))
				if err != nil {
					fmt.Printf("Error adding file metadata to Pebble after failed save: %v\n", err)
				}
			}
		}()

		c.JSON(200, gin.H{"status": "success"})
	})

	fmt.Printf("Starting server on port %d\n", *port)

	// run the server
	_ = r.Run(fmt.Sprintf(":%d", *port))
}
