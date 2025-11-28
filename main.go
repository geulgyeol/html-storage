package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/akamensky/argparse"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func compressHTML(html string) string {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(html))
	_ = gz.Close()

	return buf.String()
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

	return fmt.Sprintf("%s_%s.html.gz", blog, url)
}

func saveHTML(dir string, path string, compressedHTML string) error {
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

	_, err = file.Write([]byte(compressedHTML))
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

// listFiles returns a paginated list of files in the data directory
func listFiles(dataPath string, page, pageSize int) ([]FileInfo, int, error) {
	var files []FileInfo

	err := filepath.Walk(dataPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".html.gz") {
			relPath, _ := filepath.Rel(dataPath, path)
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

	// Sort files by path for consistent pagination
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})

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
	if !strings.HasPrefix(absFilePath, absDataPath) {
		return "", fmt.Errorf("invalid file path")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	gz, err := gzip.NewReader(file)
	if err != nil {
		return "", err
	}
	defer gz.Close()

	content, err := io.ReadAll(gz)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	parser := argparse.NewParser("geulgyeol-html-storage", "A HTML storage server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	dataPath := parser.String("d", "data-path", &argparse.Options{Default: "/data", Help: "Path to store HTML files"})

	err := parser.Parse(os.Args)
	if err != nil {
		panic(err)
	}

	r := gin.Default()

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
		if pageSize > 100 {
			pageSize = 100
		}

		files, total, err := listFiles(*dataPath, page, pageSize)
		if err != nil {
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

		compressedHTML := compressHTML(json.Body)
		dir := getDir(*dataPath, json.Timestamp)
		path := getFilename(c.Param("id"), json.Blog)
		err := saveHTML(dir, path, compressedHTML)

		if err != nil {
			fmt.Printf("Error saving HTML: %v\n", err)
			c.JSON(500, gin.H{"error": "Failed to save HTML"})
			return
		}

		c.JSON(200, gin.H{"status": "success"})
	})

	fmt.Printf("Starting server on port %d\n", *port)

	// run the server
	_ = r.Run(fmt.Sprintf(":%d", *port))
}
