package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/akamensky/argparse"
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

// FileInfo represents metadata about a stored HTML file
type FileInfo struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	Size      int64  `json:"size"`
	CreatedAt string `json:"createdAt"`
}

var files []FileInfo
var fileLastWalked time.Time = time.Time{}
var fileWalkMutex = sync.RWMutex{}

const cacheDuration = 10 * time.Minute

func paginateFiles(files []FileInfo, page, pageSize int) []FileInfo {
	start := (page - 1) * pageSize
	if start >= len(files) {
		return []FileInfo{}
	}

	end := start + pageSize
	if end > len(files) {
		end = len(files)
	}

	return files[start:end]
}

// listFiles returns a paginated list of files in the data directory
func listFiles(dataPath string, page, pageSize int) ([]FileInfo, int, error) {
	fileWalkMutex.RLock()
	cachedFiles := files
	lastWalked := fileLastWalked

	if cachedFiles != nil && time.Since(lastWalked) <= cacheDuration {
		defer fileWalkMutex.RUnlock()
		// Use cached data for pagination
		total := len(cachedFiles)
		return paginateFiles(cachedFiles, page, pageSize), total, nil
	}

	fileWalkMutex.RUnlock()
	fileWalkMutex.Lock()
	defer fileWalkMutex.Unlock()

	// re-check after acquiring write lock
	if files != nil && time.Since(fileLastWalked) <= cacheDuration {
		total := len(files)
		return paginateFiles(files, page, pageSize), total, nil
	}

	files = []FileInfo{}

	// Go walk is deterministic, so files are in a consistent order
	err := filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".html.gz") {
			relPath, relErr := filepath.Rel(dataPath, path)
			if relErr != nil {
				return relErr
			}
			files = append(files, FileInfo{
				Name:      info.Name(),
				Path:      relPath,
				Size:      info.Size(),
				CreatedAt: info.ModTime().Format(time.RFC3339),
			})
		}
		return nil
	})

	if err != nil {
		return nil, 0, err
	}

	fileLastWalked = time.Now()

	total := len(files)

	// Apply pagination
	start := (page - 1) * pageSize
	if start >= total {
		return []FileInfo{}, total, nil
	}

	end := start + pageSize
	if end > total {
		end = total
	}

	return files[start:end], total, nil
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
	} else if !strings.HasSuffix(filename, ".html.gz") {
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

func main() {
	gin.SetMode(gin.ReleaseMode)

	parser := argparse.NewParser("geulgyeol-html-storage", "A HTML storage server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	dataPath := parser.String("d", "data-path", &argparse.Options{Default: "/data", Help: "Path to store HTML files"})
	zstdDictionaryPath := parser.String("z", "zstd-dictionary", &argparse.Options{Default: "./zstd_dict", Help: "Path to Zstd dictionary file"})

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

	// In background, find .gz files under dataPath and ungzip, then recompress with zstd and save
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

		files, total, err := listFiles(*dataPath, page, pageSize)
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

	r.POST("/files/batch", func(c *gin.Context) {
		// expecting an array of path strings
		var paths []string
		if err := c.BindJSON(&paths); err != nil {
			c.JSON(400, gin.H{"error": "Invalid JSON"})
			return
		}

		type FileContent struct {
			Path    string `json:"path"`
			Content string `json:"content"`
		}

		var results []FileContent

		for _, path := range paths {
			parts := strings.SplitN(path, "/", 4)
			if len(parts) != 4 {
				continue
			}
			year, month, day, filename := parts[0], parts[1], parts[2], parts[3]
			content, err := readFile(*dataPath, year, month, day, filename)
			if err != nil {
				continue
			}
			results = append(results, FileContent{
				path,
				content,
			})
		}

		c.JSON(200, gin.H{
			"files": results,
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
			}
		}()

		c.JSON(200, gin.H{"status": "success"})
	})

	fmt.Printf("Starting server on port %d\n", *port)

	// run the server
	_ = r.Run(fmt.Sprintf(":%d", *port))
}
