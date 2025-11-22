package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"strings"
	"time"

	"github.com/akamensky/argparse"
	"github.com/gin-gonic/gin"
)

func compressHTML(html string) string {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, _ = gz.Write([]byte(html))
	_ = gz.Close()

	return buf.String()
}

func getPath(dataPath string, url string, blog string, timestamp int64) string {
	t := time.Unix(timestamp, 0)
	year, month, day := t.Date()

	// strip protocol
	url = strings.ReplaceAll(url, "http://", "")
	url = strings.ReplaceAll(url, "https://", "")

	// canonicalize url
	url = strings.ReplaceAll(url, "/", "_")

	// canonicalize query parameters
	url = strings.ReplaceAll(url, "?", "_")
	url = strings.ReplaceAll(url, "&", "_")
	url = strings.ReplaceAll(url, "=", "_")

	return fmt.Sprintf("%s/%d/%02d/%02d/%s_%s.html.gz", dataPath, year, month, day, blog, url)
}

func saveHTML(path string, compressedHTML string) error {
	// save compressedHTML to path
	// create directories if not exist
	return nil
}

func main() {
	gin.SetMode(gin.ReleaseMode)
	
	parser := argparse.NewParser("geulgyeol-html-storage", "A HTML storage server for Geulgyeol.")

	port := parser.Int("p", "port", &argparse.Options{Default: 8080, Help: "Port to run the server on"})
	err := parser.Parse(nil)
	if err != nil {
		panic(err)
	}

	dataPath := parser.String("d", "data-path", &argparse.Options{Default: "/data", Help: "Path to store HTML files"})
	err = parser.Parse(nil)
	if err != nil {
		panic(err)
	}

	r := gin.Default()

	r.POST("/{id}", func(c *gin.Context) {
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
		path := getPath(*dataPath, c.Param("id"), json.Blog, json.Timestamp)
		err := saveHTML(path, compressedHTML)

		if err != nil {
			c.JSON(500, gin.H{"error": "Failed to save HTML"})
			return
		}

		c.JSON(200, gin.H{"status": "success"})
	})

	// run the server
	_ = r.Run(fmt.Sprintf(":%d", *port))
}
