package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

type JobStruct struct {
	pathName  string
	startByte int
	endByte   int
}

type ResultDescriptor struct {
	n_primes int
	job      JobStruct
}

var wg sync.WaitGroup
var wg_consolidator sync.WaitGroup

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	slog.Info(name, "elapsed ", elapsed)

}

func timeTrackJob(start time.Time, job_struct JobStruct, result *int) {
	elapsed := time.Since(start)
	slog.Info("Completed Job", "time_elapsed", elapsed, "job struct", job_struct, "n_primes", *result)
}

func readAllUvarints(buf []byte) ([]uint64, error) {
	var numbers []uint64
	reader := bytes.NewReader(buf) // Create a reader from the byte slice
	for {
		var num uint64
		err := binary.Read(reader, binary.LittleEndian, &num)
		if err == io.EOF {
			break // Stop when reaching the end of the buffer
		}
		if err != nil {
			return nil, err // Return error if reading fails
		}
		numbers = append(numbers, num)
	}
	return numbers, nil
}

func isPrime(n uint64) bool {
	if n <= 1 {
		return false
	} else if n == 2 || n == 3 || n%2 == 0 {
		return true
	}

	for i := uint64(3); i <= uint64(math.Sqrt(float64(n))); i += 2 {
		if n%i == 0 {
			return false
		}
	}
	return true
}

func getPrimes(numbers []uint64) int {
	result := 0
	for _, n := range numbers {
		if isPrime(n) {
			result++
		}

	}
	return result
}

func process_job(next_job JobStruct, C int, sectionReader io.SectionReader, results chan ResultDescriptor) {
	result := 0
	defer timeTrackJob(time.Now(), next_job, &result)

	for begin := next_job.startByte; begin < next_job.endByte; begin += C {
		end := begin + C
		buf := make([]byte, C)
		n_bytes := C
		if end > next_job.endByte {
			end = next_job.endByte
			n_bytes = end - begin
			buf = make([]byte, n_bytes)
		}
		_, err := sectionReader.Read(buf)
		if err != nil {
			slog.Error("section reader error", nil)
		}
		numbers, _ := readAllUvarints(buf)
		primes := getPrimes(numbers)
		result += primes
	}
	results <- ResultDescriptor{job: next_job, n_primes: result}
}

func Worker(results chan ResultDescriptor, jobs chan JobStruct, C int) {
	defer wg.Done()
	sleepTime := time.Duration(400+rand.Intn(201)) * time.Millisecond
	time.Sleep(sleepTime)
	for {
		next_job, ok := <-jobs
		if !ok {
			return
		} else {
			file, err := os.Open(next_job.pathName)
			if err != nil {
				log.Println("Error", err)
				return
			}
			reader_start := int64(next_job.startByte)
			reader_end := int64(next_job.endByte)
			sectionReader := io.NewSectionReader(file, reader_start, reader_end)
			process_job(next_job, C, *sectionReader, results)
			file.Close()
		}
	}

}

func Dispatcher(fileSize int, N int, pathName string, jobs chan JobStruct) {
	defer timeTrack(time.Now(), "dispatcher")
	defer wg.Done()
	for i := 0; i < fileSize; i += N {
		endByte := i + N
		if endByte > fileSize {
			endByte = fileSize
		}
		jobs <- JobStruct{pathName: pathName, endByte: endByte, startByte: i}
	}
	close(jobs)
}

func Consolidator(val *int, results chan ResultDescriptor) {
	defer wg_consolidator.Done()
	defer timeTrack(time.Now(), "consolidator")
	for {
		next_result, ok := <-results
		if !ok {
			return
		} else {
			*val = *val + next_result.n_primes
		}
	}
}

func main() {

	argsWithoutProg := os.Args[1:]
	pathName := argsWithoutProg[0]
	M, _ := strconv.Atoi(argsWithoutProg[1])
	_N := argsWithoutProg[2]
	_C := argsWithoutProg[3]

	log_path := "app.log"

	if len(argsWithoutProg) > 4 {
		log_path = argsWithoutProg[4]
	}

	logFile, err := os.OpenFile(log_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		fmt.Println("Error creating log file:", err)
		return
	}
	defer logFile.Close()

	// Set the output of the default logger to the file
	log.SetOutput(logFile)

	//N in {1KB, 32KB, 64KB, 256KB, 1MB, 64MB}; C in {64B, 1KB, 4KB, 8KB}.
	N_dict := make(map[string]int)
	N_dict["1KB"] = 1024
	N_dict["2KB"] = 2 * 1024 //testing only: delete this!
	N_dict["32KB"] = 32 * 1024
	N_dict["64KB"] = 64 * 1024
	N_dict["256KB"] = 256 * 1024
	N_dict["1MB"] = 1024 * 1024
	N_dict["64MB"] = 64 * 1024 * 1024

	C_dict := make(map[string]int)
	C_dict["64B"] = 64
	C_dict["128B"] = 128 //testing only: delete this!
	C_dict["1KB"] = 1024
	C_dict["4KB"] = 4 * 1024
	C_dict["8KB"] = 8 * 1024

	N, existsN := N_dict[_N]

	if !existsN {
		N = 64 * 1024
	}
	C, existsC := C_dict[_C]

	if !existsC {
		C = 1024
	}

	fileInfo, err := os.Stat(pathName)
	if err != nil {
		log.Println("Error:", err)
		return
	}

	fileSize := int(fileInfo.Size())

	results := make(chan ResultDescriptor, 1000)
	jobs := make(chan JobStruct, 1000)

	for j := 0; j < M; j++ {
		wg.Add(1)
		go Worker(results, jobs, C)
	}
	wg.Add(1)
	go Dispatcher(fileSize, N, pathName, jobs)

	wg_consolidator.Add(1)
	val := 0
	go Consolidator(&val, results)

	wg.Wait()
	close(results)

	wg_consolidator.Wait()
	fmt.Println("val ", val)
}
