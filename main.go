package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
	"golang.org/x/net/proxy"
)

type AO3Work struct {
	ID       string            `json:"id"`
	Title    string            `json:"title"`
	Metadata map[string]string `json:"metadata"`
	Text     string            `json:"text"`
}

// StatsMetadata contains parsed statistics from the work
type StatsMetadata struct {
	Published string `json:"published"`
	Completed string `json:"completed"`
	Words     string `json:"words"`
	Chapters  string `json:"chapters"`
}

// ProxyInfo stores proxy connection details
type ProxyInfo struct {
	Host     string
	Port     string
	Username string
	Password string
}

// ProxyManager handles proxy rotation and tracking
type ProxyManager struct {
	proxies      []ProxyInfo
	currentIndex int
	mu           *sync.Mutex
	badProxies   map[int]time.Time // Track rate-limited proxies with cooldown time
}

type Configuration struct {
	StartID            int
	EndID              int
	BatchSize          int
	ConcurrentRequests int
	ProxyFile          string
	OutputDir          string
	UseProxies         bool
	RetryAttempts      int
	Timeout            time.Duration
	BaseURL            string
	UserAgent          string
}

func main() {
	// Define command-line flags
	config := parseFlags()

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(config.OutputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags)

	// Print startup information
	logger.Printf("Configuration: StartID=%d, EndID=%d, BatchSize=%d, Concurrent=%d",
		config.StartID, config.EndID, config.BatchSize, config.ConcurrentRequests)

	var proxyManager *ProxyManager
	if config.UseProxies {
		// Load proxies from file
		proxies, err := loadProxies(config.ProxyFile)
		if err != nil {
			logger.Fatalf("Error loading proxies: %v", err)
		}

		if len(proxies) == 0 && config.UseProxies {
			logger.Fatalf("No proxies found in %s but --no-proxy flag was not used", config.ProxyFile)
		}

		// Log proxy information
		logger.Printf("Loaded %d proxies", len(proxies))

		// Create a proxy manager for rotation
		proxyManager = &ProxyManager{
			proxies:      proxies,
			currentIndex: 0,
			mu:           &sync.Mutex{},
			badProxies:   make(map[int]time.Time),
		}
	} else {
		logger.Printf("Running without proxies (direct connection)")
		proxyManager = &ProxyManager{
			proxies:      []ProxyInfo{},
			currentIndex: 0,
			mu:           &sync.Mutex{},
			badProxies:   make(map[int]time.Time),
		}
	}

	// Create a file mutex to protect file writes
	var fileMutex sync.Mutex

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create a semaphore to limit concurrent requests
	semaphore := make(chan struct{}, config.ConcurrentRequests)

	// Start time to calculate processing rate
	startTime := time.Now()
	processedCount := 0
	var processedMutex sync.Mutex

	// Process all IDs
	totalIDs := config.EndID - config.StartID + 1

	for id := config.StartID; id <= config.EndID; id++ {
		wg.Add(1)

		// Acquire a semaphore slot
		semaphore <- struct{}{}

		go func(workID int) {
			defer wg.Done()
			defer func() { <-semaphore }()

			batchStart := ((workID-config.StartID)/config.BatchSize)*config.BatchSize + config.StartID
			batchEnd := batchStart + config.BatchSize - 1
			if batchEnd > config.EndID {
				batchEnd = config.EndID
			}

			outputFile := filepath.Join(config.OutputDir, fmt.Sprintf("ao3_works_%d-%d.jsonl", batchStart, batchEnd))

			url := fmt.Sprintf("%s/%d/a.html", config.BaseURL, workID)

			// Process the work with thread-safe approach and proxy rotation
			processWorkWithProxies(url, strconv.Itoa(workID), outputFile, proxyManager, &fileMutex, config, logger)

			// Always increment the processed count regardless of success
			processedMutex.Lock()
			processedCount++
			current := processedCount
			processedMutex.Unlock()

			if current%100 == 0 {
				elapsedSeconds := time.Since(startTime).Seconds()
				rate := float64(current) / elapsedSeconds
				estimatedTotal := float64(totalIDs) / rate
				estimatedRemaining := estimatedTotal - elapsedSeconds

				logger.Printf("Progress: %d/%d works processed (%.2f%%), %.2f works/sec, est. remaining: %.1f minutes",
					current, totalIDs, float64(current)/float64(totalIDs)*100, rate,
					estimatedRemaining/60)
			}
		}(id)
	}

	wg.Wait()

	duration := time.Since(startTime)
	logger.Printf("Processing complete! Processed %d works in %v (%.2f works/sec)",
		processedCount, duration.Round(time.Second), float64(processedCount)/duration.Seconds())
}

func parseFlags() *Configuration {
	config := &Configuration{}

	flag.IntVar(&config.StartID, "start-id", 1, "Starting ID")
	flag.IntVar(&config.EndID, "end-id", 100000, "Ending ID")
	flag.IntVar(&config.BatchSize, "batch-size", 10000, "Number of IDs per output file")
	flag.IntVar(&config.ConcurrentRequests, "concurrent", 4, "Maximum number of concurrent requests")
	flag.IntVar(&config.RetryAttempts, "retries", 5, "Number of retry attempts per work")
	flag.StringVar(&config.ProxyFile, "proxy-file", "proxy.txt", "File containing proxies")
	flag.StringVar(&config.OutputDir, "output", "output", "Directory for output files")
	flag.BoolVar(&config.UseProxies, "use-proxies", false, "Use proxies for connections")
	flag.DurationVar(&config.Timeout, "timeout", 60*time.Second, "Timeout for HTTP requests")
	flag.StringVar(&config.BaseURL, "base-url", "https://download.archiveofourown.org/downloads", "otwarchive downloads base URL")
	flag.StringVar(&config.UserAgent, "user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36", "User agent string for requests")

	flag.Parse()

	// Validate flags
	if config.StartID <= 0 {
		log.Fatalf("start-id must be greater than 0")
	}

	if config.EndID < config.StartID {
		log.Fatalf("end-id must be greater than or equal to start-id")
	}

	if config.ConcurrentRequests <= 0 {
		log.Fatalf("concurrent must be greater than 0")
	}

	return config
}

// GetNextClient returns the next available HTTP client (with proxy if available)
func (pm *ProxyManager) GetNextClient(config *Configuration) (*http.Client, int, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// If no proxies or direct connection requested, return a direct client
	if len(pm.proxies) == 0 || !config.UseProxies {
		transport := &http.Transport{}
		client := &http.Client{
			Transport: transport,
			Timeout:   config.Timeout,
		}
		return client, -1, nil
	}

	// Try proxies until we find a good one
	proxyIndex := pm.currentIndex
	proxyInfo := pm.proxies[proxyIndex]

	// Create auth for the proxy
	auth := &proxy.Auth{
		User:     proxyInfo.Username,
		Password: proxyInfo.Password,
	}

	// Create a SOCKS5 dialer
	dialer, err := proxy.SOCKS5("tcp", fmt.Sprintf("%s:%s", proxyInfo.Host, proxyInfo.Port), auth, proxy.Direct)
	if err != nil {
		// Update the current index for next time and try again
		pm.currentIndex = (proxyIndex + 1) % len(pm.proxies)
		return pm.GetNextClient(config)
	}

	// Create the transport with the SOCKS5 dialer
	transport := &http.Transport{
		Dial: dialer.Dial,
	}

	// Create a client with the SOCKS5 transport and timeout
	client := &http.Client{
		Transport: transport,
		Timeout:   config.Timeout,
	}

	pm.currentIndex = (proxyIndex + 1) % len(pm.proxies)

	return client, proxyIndex, nil
}

// MarkProxyBad marks a proxy as rate-limited for a cooldown period
func (pm *ProxyManager) MarkProxyBad(index int) {
	if index == -1 {
		return // Don't mark direct connections
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()

	pm.badProxies[index] = time.Now().Add(1 * time.Minute)
}

// Load proxies from the file
func loadProxies(filename string) ([]ProxyInfo, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var proxies []ProxyInfo
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.Split(line, ":")
		if len(parts) == 4 {
			proxies = append(proxies, ProxyInfo{
				Host:     parts[0],
				Port:     parts[1],
				Username: parts[2],
				Password: parts[3],
			})
		} else if len(parts) == 2 {
			// Handle format without auth
			proxies = append(proxies, ProxyInfo{
				Host:     parts[0],
				Port:     parts[1],
				Username: "",
				Password: "",
			})
		} else {
			return nil, fmt.Errorf("invalid proxy format at line: %s (expected host:port:username:password or host:port)", line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return proxies, nil
}

// Process work using a proxy with retries
func processWorkWithProxies(url, id, outputFile string, proxyManager *ProxyManager, fileMutex *sync.Mutex, config *Configuration, logger *log.Logger) bool {
	for retry := 0; retry < config.RetryAttempts; retry++ {
		client, proxyIndex, err := proxyManager.GetNextClient(config)
		if err != nil {
			logger.Printf("ID: %s - Error getting client: %v - Retrying with next proxy", id, err)
			time.Sleep(2 * time.Second)
			continue
		}

		// First check HEAD request before proceeding with GET
		validResource, statusCode, err := checkResourceExists(url, client, config.UserAgent)

		if err != nil {
			if isTimeoutError(err) {
				logger.Printf("ID: %s - Timeout error with proxy - Retrying with another proxy", id)
				time.Sleep(2 * time.Second)
				continue // Try with another proxy
			}
		}

		// Handle rate limiting
		if statusCode == http.StatusTooManyRequests {
			logger.Printf("ID: %s - Rate limited (429) - Switching to next proxy and retrying", id)
			proxyManager.MarkProxyBad(proxyIndex)
			time.Sleep(2 * time.Second)
			continue
		}

		if statusCode == http.StatusServiceUnavailable {
			logger.Printf("ID: %s - Service unavailable (503) - Retrying with next proxy", id)
			time.Sleep(3 * time.Second)
			continue
		}

		// Handle connection issues
		if statusCode == 0 {
			logger.Printf("ID: %s - Connection issue (status code 0) - Retrying with another proxy", id)
			time.Sleep(2 * time.Second)
			continue
		}

		if err != nil || !validResource {
			logger.Printf("ID: %s - HEAD Check: Resource not available - Status: %d", id, statusCode)
			return false
		}

		// Fetch HTML content
		htmlContent, statusCode, err := fetchHTML(url, client, config.UserAgent)

		if err != nil {
			if isTimeoutError(err) {
				logger.Printf("ID: %s - Timeout error during GET - Retrying with another proxy", id)
				time.Sleep(2 * time.Second)
				continue // Try with another proxy
			}
		}

		// Handle rate limiting for GET requests too
		if statusCode == http.StatusTooManyRequests {
			logger.Printf("ID: %s - Rate limited (429) during GET - Switching to next proxy and retrying", id)
			proxyManager.MarkProxyBad(proxyIndex)
			time.Sleep(2 * time.Second)
			continue
		}

		// Handle connection issues during GET request
		if statusCode == 0 {
			logger.Printf("ID: %s - Connection issue during GET (status code 0) - Retrying with another proxy", id)
			time.Sleep(2 * time.Second)
			continue
		}

		// Always print status code
		logger.Printf("ID: %s - Status: %d", id, statusCode)

		// Only proceed if request was successful
		if err != nil || statusCode != http.StatusOK {
			time.Sleep(1 * time.Second)
			continue
		}

		// Extract filename from content-disposition header
		filename, err := getFilenameFromURL(url, client, config.UserAgent)
		if err != nil {
			if isTimeoutError(err) {
				logger.Printf("ID: %s - Timeout error getting filename - Retrying with another proxy", id)
				time.Sleep(2 * time.Second)
				continue
			}
			logger.Printf("Error getting filename for ID %s: %v", id, err)
			return false
		}

		// Process the filename to get title
		title := strings.TrimSuffix(filename, filepath.Ext(filename))
		title = strings.ReplaceAll(title, "_", " ")

		// Parse metadata from HTML
		metadata, storyText, err := extractMetadata(htmlContent)
		if err != nil {
			logger.Printf("Error extracting metadata for ID %s: %v", id, err)
			return false
		}

		// Remove duplicate title from metadata since it's already in the top level
		delete(metadata, "title")

		// Parse stats field if it exists
		if stats, ok := metadata["Stats"]; ok {
			parsedStats := parseStats(stats)
			metadata["published"] = parsedStats.Published
			metadata["completed"] = parsedStats.Completed
			metadata["words"] = parsedStats.Words
			metadata["chapters"] = parsedStats.Chapters
			delete(metadata, "Stats")
		}

		work := AO3Work{
			ID:       id,
			Title:    title,
			Metadata: metadata,
			Text:     storyText,
		}

		// Save to JSONL file with mutex protection
		err = saveToJSONL(work, outputFile, fileMutex)
		if err != nil {
			logger.Printf("Error saving ID %s to JSONL: %v", id, err)
			return false
		}

		return true
	}

	logger.Printf("ID: %s - Failed after %d retries", id, config.RetryAttempts)
	return false
}

// Check if resource exists by making a HEAD request first
func checkResourceExists(url string, client *http.Client, userAgent string) (bool, int, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return false, 0, err
	}

	// Set user agent
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == http.StatusOK, resp.StatusCode, nil
}

// Helper function to check if error is a timeout error
func isTimeoutError(err error) bool {
	if err, ok := err.(net.Error); ok && err.Timeout() {
		return true
	}
	return strings.Contains(err.Error(), "timeout")
}

// Parse the stats string into structured data
func parseStats(stats string) StatsMetadata {
	result := StatsMetadata{}

	// Clean up whitespace
	cleanStats := cleanStats(stats)

	// Extract published date
	pubRegex := regexp.MustCompile(`Published:\s*(\d{4}-\d{2}-\d{2})`)
	if pubMatches := pubRegex.FindStringSubmatch(cleanStats); len(pubMatches) > 1 {
		result.Published = pubMatches[1]
	}

	// Extract completed date
	compRegex := regexp.MustCompile(`Completed:\s*(\d{4}-\d{2}-\d{2})`)
	if compMatches := compRegex.FindStringSubmatch(cleanStats); len(compMatches) > 1 {
		result.Completed = compMatches[1]
	}

	// Extract word count
	wordsRegex := regexp.MustCompile(`Words:\s*([\d,]+)`) // Handle commas in numbers
	if wordsMatches := wordsRegex.FindStringSubmatch(cleanStats); len(wordsMatches) > 1 {
		result.Words = wordsMatches[1]
	}

	// Extract chapters
	chaptersRegex := regexp.MustCompile(`Chapters:\s*(\d+/\?|\d+/\d+)`) // Handle "Chapters: 4/?" case
	if chaptersMatches := chaptersRegex.FindStringSubmatch(cleanStats); len(chaptersMatches) > 1 {
		result.Chapters = chaptersMatches[1]
	}

	return result
}

// Clean up whitespace in stats string
func cleanStats(stats string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(stats, " ")
}

func fetchHTML(url string, client *http.Client, userAgent string) (string, int, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", 0, err
	}

	// Set user agent
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return "", 0, err
	}
	defer resp.Body.Close()

	// Return status code even if it's not 200
	if resp.StatusCode != http.StatusOK {
		return "", resp.StatusCode, fmt.Errorf("HTTP status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", resp.StatusCode, err
	}

	return string(body), resp.StatusCode, nil
}

func getFilenameFromURL(url string, client *http.Client, userAgent string) (string, error) {
	// First make a HEAD request to get the content-disposition header
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return "", err
	}

	// Set user agent
	req.Header.Set("User-Agent", userAgent)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Extract filename from content-disposition header
	contentDisposition := resp.Header.Get("Content-Disposition")
	if contentDisposition == "" {
		return "unknown.html", nil
	}

	re := regexp.MustCompile(`filename\*?=['"]?(?:UTF-8['']?)?([^;'"]*)['"]?`)
	matches := re.FindStringSubmatch(contentDisposition)
	if len(matches) > 1 {
		return matches[1], nil
	}

	// Fallback to using the URL
	parts := strings.Split(url, "/")
	return parts[len(parts)-1], nil
}

func extractMetadata(htmlContent string) (map[string]string, string, error) {
	metadata := make(map[string]string)
	var storyText string

	doc, err := html.Parse(strings.NewReader(htmlContent))
	if err != nil {
		return nil, "", err
	}

	// Reset userstuffContents and processedUserStuff
	var userstuffContents []string
	processedUserStuff := make(map[string]bool)

	// Extract metadata (title, author, tags, etc.)
	extractMetadataFromNode(doc, metadata)

	// Find chapters section and collect story text
	chaptersNode := findNodeByID(doc, "chapters")
	if chaptersNode != nil {
		for c := chaptersNode.FirstChild; c != nil; c = c.NextSibling {
			if c.Type == html.ElementNode && c.Data == "div" && hasClass(c, "userstuff") {
				content := extractTextContent(c)
				if content != "" && !processedUserStuff[content] {
					userstuffContents = append(userstuffContents, content)
					processedUserStuff[content] = true
				}
			}
		}
	}

	storyText = strings.Join(userstuffContents, "\n\n----- CHAPTER BREAK -----\n\n")

	return metadata, storyText, nil
}

// Helper function to find a node by ID
func findNodeByID(n *html.Node, targetID string) *html.Node {
	if n.Type == html.ElementNode {
		for _, a := range n.Attr {
			if a.Key == "id" && a.Val == targetID {
				return n
			}
		}
	}
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		if found := findNodeByID(c, targetID); found != nil {
			return found
		}
	}
	return nil
}

// Extract metadata from HTML nodes
func extractMetadataFromNode(n *html.Node, metadata map[string]string) {
	if n.Type == html.ElementNode && n.Data == "dl" && hasClass(n, "tags") {
		processTagsElement(n, metadata)
	}

	// Extract title
	if n.Type == html.ElementNode && n.Data == "h1" {
		metadata["title"] = extractTextContent(n)
	}

	// Extract author
	if n.Type == html.ElementNode && n.Data == "div" && hasClass(n, "byline") {
		metadata["author"] = extractTextContent(n)
	}

	// Traverse children recursively
	for c := n.FirstChild; c != nil; c = c.NextSibling {
		extractMetadataFromNode(c, metadata)
	}
}

func processTagsElement(n *html.Node, metadata map[string]string) {
	var currentTag string
	var tagValues []string

	var traverseChildren func(*html.Node)
	traverseChildren = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "dt" {
			currentTag = strings.TrimSuffix(extractTextContent(node), ":")
			tagValues = []string{}
		} else if node.Type == html.ElementNode && node.Data == "dd" && currentTag != "" {
			// Extract all <a> tags or text directly if no <a> tags
			links := extractLinks(node)
			if len(links) > 0 {
				tagValues = append(tagValues, links...)
			} else {
				tagText := extractTextContent(node)
				if tagText != "" {
					tagValues = append(tagValues, tagText)
				}
			}
			// Store joined values in metadata
			metadata[currentTag] = strings.Join(tagValues, ", ")
		}

		for c := node.FirstChild; c != nil; c = c.NextSibling {
			traverseChildren(c)
		}
	}

	traverseChildren(n)
}

func extractLinks(n *html.Node) []string {
	var links []string

	var traverse func(*html.Node)
	traverse = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "a" {
			linkText := extractTextContent(node)
			if linkText != "" {
				links = append(links, linkText)
			}
		}

		for c := node.FirstChild; c != nil; c = c.NextSibling {
			traverse(c)
		}
	}

	traverse(n)
	return links
}

func hasClass(n *html.Node, className string) bool {
	for _, attr := range n.Attr {
		if attr.Key == "class" {
			classes := strings.Fields(attr.Val)
			return slices.Contains(classes, className)
		}
	}
	return false
}

func extractTextContent(n *html.Node) string {
	var text string
	var extractText func(*html.Node)
	extractText = func(node *html.Node) {
		if node.Type == html.TextNode {
			text += node.Data
		}
		for c := node.FirstChild; c != nil; c = c.NextSibling {
			extractText(c)
		}
	}
	extractText(n)
	return strings.TrimSpace(text)
}

func saveToJSONL(work AO3Work, filename string, fileMutex *sync.Mutex) error {
	// Protect file access with mutex
	fileMutex.Lock()
	defer fileMutex.Unlock()

	// Create or open the file
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Marshal to JSON
	jsonData, err := json.Marshal(work)
	if err != nil {
		return err
	}

	// Write to file
	_, err = file.WriteString(string(jsonData) + "\n")
	return err
}
