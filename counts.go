package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oleiade/lane"
)

var counterMap map[string]*WikiPageCounter
var lineCount int

// WikiPageCounter is a counter for the views for each wiki page for a given language.
type WikiPageCounter struct {
	language string
	counter  map[string]uint32
	db       *DB
	mu       sync.Mutex
}

type dbPage struct {
	pageTitle string
	key       string
	pageViews uint32
}

// Exists reports whether the named file or directory exists.
func Exists(name string) bool {
	if _, err := os.Stat(name); err != nil {
		if os.IsNotExist(err) {
			return false
		}
	}
	return true
}

// DownloadFiles downloads and stores the gzips for a given file if it does not exist.
func DownloadFiles(baseURL string, hour int, date time.Time) (string, error) {
	file := date.Format("2006/2006-01/pagecounts-20060102")
	file += fmt.Sprintf("-%02d0000.gz", hour)
	filePath := fmt.Sprintf("cached/%s", file)
	if !Exists(filePath) {
		fileURL := baseURL + file
		fmt.Printf("Downloading %s to %s", fileURL, "to", filePath)
		file, err := os.Create("filePath")
		defer file.Close()
		if err != nil {
			panic(err)
		}

		resp, err := http.Get(fileURL)
		defer resp.Body.Close()
		if err != nil {
			return "", fmt.Errorf("Error while downloading", fileURL, "-", err)
		}
		if resp.StatusCode != http.StatusOK {

			return "", fmt.Errorf("Server return non-200 status: %v\n", resp.Status)
		}
		source := resp.Body
		n, err := io.Copy(file, source)
		if err != nil {
			return "", fmt.Errorf("Error creating file: ", err)
		}
		fmt.Println(n, " bytes downloaded.")
	}
	return filePath, nil
}

// Reafile reads and processes a view count GZIP.
func readFile(filePath string) {
	if !Exists(filePath) {
		return
	}
	fi, err := os.Open(filePath)
	defer fi.Close()
	if err != nil {
		panic(err)
	}

	r, err := gzip.NewReader(fi)
	defer r.Close()
	if err != nil {
		fmt.Println("Can not decode: ", err)
		return
	}
	scanner := bufio.NewScanner(r)

	for scanner.Scan() {
		line := scanner.Text()
		tokens := strings.Fields(line)
		language := strings.ToLower(tokens[0])
		counter := counterMap[language]
		if counter != nil {
			counter.ProcessLine(tokens, language)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error while decoding", filePath, "-", err)
		return
	}
}

// ProcessLine parses and saves a line to the db
func (counter *WikiPageCounter) ProcessLine(tokens []string, language string) {

	if len(tokens) == 4 && strings.Index(tokens[1], ":") == -1 && strings.Index(tokens[1], "?") == -1 {
		pageTitle, _ := url.QueryUnescape(tokens[1])
		if len(pageTitle) == 0 {
			return
		}
		pageViews, _ := strconv.ParseInt(tokens[2], 10, 64)

		counter.mu.Lock()
		counter.counter[pageTitle] += uint32(pageViews)
		lineCount++
		counter.mu.Unlock()

	}
}

// CreateCounter creates a counter or returns the existing one.
func CreateCounter(language string) *WikiPageCounter {

	if counterMap[language] != nil {
		return counterMap[language]
	}
	db := NewDB()
	if err := db.Open(fmt.Sprintf("db/wiki_counts_%s", language)); err != nil {
		log.Fatal(err)
	}
	db.Update(func(tx *Tx) error {
		_, _ = tx.CreateBucketIfNotExists([]byte("pages"))
		return nil
	})
	counterMap[language] = &WikiPageCounter{
		language: language,
		counter:  make(map[string]uint32),
		db:       db,
	}

	return counterMap[language]
}

// WriteToDB writes all values of a counter to its DB
func WriteToDB(wpc *WikiPageCounter, date string) {
	counter := wpc.counter
	deque := lane.NewCappedDeque(5000)
	for title, views := range counter {
		lineCount++
		deque.Append(&dbPage{
			pageTitle: title,
			pageViews: views,
			key:       date,
		})
		delete(counter, title)
		if deque.Full() {
			writeFromDeque(deque, wpc)
		}

	}
	//Write remaining items at end of write cycle
	writeFromDeque(deque, wpc)
}

func writeFromDeque(dq *lane.Deque, wpc *WikiPageCounter) {

	wpc.db.Update(func(tx *Tx) error {
		for !dq.Empty() {
			update := dq.Pop().(*dbPage)
			titleBucket := tx.Bucket([]byte("pages"))
			timeSeries := make(map[string]uint32)
			title := titleBucket.Get([]byte(update.pageTitle))
			if title != nil {
				json.Unmarshal(title, &timeSeries)
			}
			timeSeries[update.key] = update.pageViews
			newVal, _ := json.Marshal(timeSeries)
			titleBucket.Put([]byte(update.pageTitle), newVal)

		}
		return nil
	})

}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	counterMap = make(map[string]*WikiPageCounter)

	os.Mkdir("db", 0777)
	for _, wikiType := range WikiTypes {
		CreateCounter(wikiType)
		// Sleep so inserts are staggered to mitigate disk thrashing
		time.Sleep(50)
	}
	i := 0
	go func() {
		for {
			time.Sleep(1 * time.Second)
			i++
			fmt.Println("Average lines/s: ", lineCount/i, ", Total lines: ", lineCount)
		}
	}()

	baseURL := "http://dumps.wikimedia.org/other/pagecounts-raw/"
	// t := time.Now().Format("2006/2006-01/pagecounts-20060102")
	// file := fmt.Sprintf("%s", t)
	date, _ := time.Parse("2006-01-02", "2015-05-24")
	concurrency := 2
	sem := make(chan bool, concurrency)
	filePaths := make([]string, 24)
	for i := 0; i < 24; i++ {
		sem <- true
		go func(i int) {
			path, err := DownloadFiles(baseURL, i, date)
			if err != nil {
				fmt.Println(err)
			} else {
				filePaths[i] = path
			}
			<-sem
		}(i)
	}
	// Defer until semaphore is completely flushed
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	// Construct counters for each language based on files
	var wg sync.WaitGroup
	for _, file := range filePaths {
		wg.Add(1)
		go func(file string) {
			defer wg.Done()
			if file != "" {
				readFile(file)
			}
		}(file)
	}
	wg.Wait()
	lineCount = 0
	i = 0

	// Loop through counters and sequentially write each one to to file.
	for _, counter := range counterMap {
		WriteToDB(counter, "2015-05-24")
	}
	wg.Wait()
}
