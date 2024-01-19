package main

import (
	"crypto/md5"
	crand "crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const fsFactor int = 32
const clusterSize int = 512
const maxFileSizeDefault int = 5000
const numFilesDefault int = 500
const numDirsDefault int = 5
const nworkers int = 10

type SafeNumber struct {
	amount int
	mu     *sync.RWMutex
}

func (n *SafeNumber) Get() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.amount
}

func (n *SafeNumber) Decr(v int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.amount -= v
}

func (n *SafeNumber) Incr(v int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.amount += v
}

func CreateSafeNumber(n int) *SafeNumber {
	var mu sync.RWMutex
	return &SafeNumber{n, &mu}
}

// TODO: round up to allocation unit size
func main() {
	path, amount, numFiles, numDirs, maxFileSize, err := parseArgs(os.Args[1:])
	if err != nil {
		printHelp()
		panic(errors.New("invalid args"))
	}
	fmt.Printf("Starting with path \"%s\", size=%d, numFiles=%d, numDirs=%d, maxFileSize=%d\n", path, amount, numFiles, numDirs, maxFileSize)
	total := amount
	limiter := CreateSafeNumber(amount)
	dirCount := CreateSafeNumber(0)
	fileCount := CreateSafeNumber(0)

	//write print loop
	go func(total int) {
		for {
			perc := 100 * (float32(total) - float32(limiter.Get())) / float32(total)
			if perc > 100 {
				perc = 100
			}
			fmt.Printf("Processing %3.2f%%...\n", perc)
			time.Sleep(time.Second * 3)

		}
	}(total)

	//write log loop
	outputchan := make(chan string, 10000000)
	defer close(outputchan)
	f, err := os.Create(fmt.Sprintf("Run-%d.txt", time.Now().UnixNano()))
	if err != nil {
		panic(fmt.Errorf("log file couldnt be created: %s", err))
	}
	defer f.Close()
	go func() {
		for x := range outputchan {
			_, _ = f.WriteString(x)
		}
	}()

	// write worker loop
	var wg sync.WaitGroup
	pathqueue := make(chan string, 1000000000)
	defer close(pathqueue)
	for i := 0; i < nworkers; i++ {
		go func(wg *sync.WaitGroup) {
			for path := range pathqueue {
				if CreateRandomFile(path, numFiles, maxFileSize, limiter, fileCount, outputchan) {
					CreateRandomDir(path, numDirs, limiter, dirCount, wg, pathqueue)
				}
				wg.Done()
			}
		}(&wg)
	}
	//seed queue with first path
	wg.Add(1)
	pathqueue <- path
	wg.Wait()
	fmt.Printf("DONE; Created %d files and %d directories.\n", fileCount.Get(), dirCount.Get())
}

func CreateRandomFile(
	path string,
	numFiles int,
	maxFileSize int,
	limiter *SafeNumber,
	fileCount *SafeNumber,
	outputchan chan<- string) bool {
	for i := 0; i < numFiles; i++ {
		size := rand.Intn(maxFileSize)
		rsize := clusterSize * int(math.Ceil(float64(size)/float64(clusterSize)))
		if limiter.Get()-rsize < 0 {
			return false
		}
		limiter.Decr(rsize)
		f, err := os.CreateTemp(path, "*")
		if err != nil {
			continue
		}
		token := make([]byte, size)
		crand.Read(token)
		_, err = f.Write(token)
		if err != nil {
			f.Close()
			continue
		}
		h := md5.New()
		h.Write(token)
		outputchan <- fmt.Sprintf("%x - %s\n", h.Sum(nil), f.Name())
		fileCount.Incr(1)
		f.Close()
	}
	return true
}

func CreateRandomDir(
	path string,
	numDirs int,
	limiter *SafeNumber,
	dirCount *SafeNumber,
	wg *sync.WaitGroup,
	pathqueue chan<- string) {
	for i := 0; i < numDirs; i++ {
		dir, err := os.MkdirTemp(path, "*")
		if err != nil {
			continue
		}
		limiter.Decr(fsFactor)
		dirCount.Incr(1)
		wg.Add(1)
		pathqueue <- dir
	}
}

func parseArgs(args []string) (string, int, int, int, int, error) {
	var path string
	var size int
	numFiles := numFilesDefault
	numDirs := numDirsDefault
	maxFileSize := maxFileSizeDefault
	var err error
	for i, x := range args {
		if i == 0 {
			path = x
			if _, err = os.Stat(path); err != nil || os.IsNotExist(err) {
				return path, size, numFiles, numDirs, maxFileSize, fmt.Errorf("the path %s is not a valid dir", path)
			}
		} else if i == 1 {
			size, err = strconv.Atoi(x)
			if err != nil {
				return path, size, numFiles, numDirs, maxFileSize, fmt.Errorf("the size %s is not a number", x)
			}
		} else if strings.HasPrefix(x, "numFiles=") {
			numFiles, err = strconv.Atoi(x[len("numFiles="):])
			if err != nil {
				return path, size, numFiles, numDirs, maxFileSize, fmt.Errorf("the size %s is not a number", x)
			}
		} else if strings.HasPrefix(x, "numDirs=") {
			numDirs, err = strconv.Atoi(x[len("numDirs="):])
			if err != nil {
				return path, size, numFiles, numDirs, maxFileSize, fmt.Errorf("the size %s is not a number", x)
			}
		} else if strings.HasPrefix(x, "maxFileSize=") {
			maxFileSize, err = strconv.Atoi(x[len("maxFileSize="):])
			if err != nil {
				return path, size, numFiles, numDirs, maxFileSize, fmt.Errorf("the size %s is not a number", x)
			}
		}
	}
	return path, size, numFiles, numDirs, maxFileSize, nil
}

func printHelp() {
	fmt.Printf(
		"Usage: [path] [size] <numDirs=n> <numFiles=n> <maxFileSize=n>\n" +
			"where [x] is required and <x> is optional\n")
}
