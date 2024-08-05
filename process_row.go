package main

import (
  "bufio"
  "bytes"
  "flag"
  "fmt"
  "os"
  "runtime"
  "runtime/pprof"
  "sync"
)

// Types
type DataRow struct {
  StationName string
  Temperature float64
}

type CityStats struct {
  Min   float64
  Max   float64
  Sum   float64
  Count int
}

// Variables
var mapSize int

// Functions

// min function to find the minimum of two integers
func min(a, b int) int {
  if a < b {
    return a
  }
  return b
}

func parseTemperature(data []byte) (float64, error) {
  var result float64
  var sign float64 = 1
  var decimal bool
  var decimalFactor float64 = 0.1

  if len(data) == 0 {
    return 0, fmt.Errorf("invalid temperature")
  }

  // Handle optional sign
  if data[0] == '-' {
    sign = -1
    data = data[1:]
  } else if data[0] == '+' {
    data = data[1:]
  }

  for _, b := range data {
    if b >= '0' && b <= '9' {
      if decimal {
        result += float64(b-'0') * decimalFactor
        decimalFactor *= 0.1
      } else {
        result = result*10 + float64(b-'0')
      }
    } else if b == '.' {
      if decimal {
        return 0, fmt.Errorf("invalid temperature")
      }
      decimal = true
    } else {
      return 0, fmt.Errorf("invalid temperature")
    }
  }

  return result * sign, nil
}

func readFileChunks(filePath string, chunkSize int, blockSize int, chunks chan<- []DataRow, wg *sync.WaitGroup) {
  defer wg.Done()

  file, err := os.Open(filePath)
  if err != nil {
    fmt.Printf("Error opening file: %v\n", err)
    close(chunks)
    return
  }
  defer file.Close()

  reader := bufio.NewReaderSize(file, blockSize)
  scanner := bufio.NewScanner(reader)

  // Preallocate the slice with chunkSize
  chunk := make([]DataRow, chunkSize)
  index := 0

  for scanner.Scan() {
    line := scanner.Bytes()
    sepIndex := bytes.IndexByte(line, ';')
    if sepIndex == -1 {
      continue
    }

    stationName := string(line[:sepIndex]) // Direct conversion, unavoidable if needed as string

    temperature, err := parseTemperature(line[sepIndex+1:])
    if err != nil {
      continue
    }

    // Insert at the current index and increment index
    chunk[index] = DataRow{StationName: stationName, Temperature: temperature}
    index++

    if index >= chunkSize {
      // When chunk is full, send it and reset index
      chunks <- chunk
      // chunk = nil
      chunk = make([]DataRow, chunkSize) // Reallocate with chunkSize
      index = 0
    }
  }

  // Send remaining chunk if it has any data
  if index > 0 {
    chunks <- chunk[:index] // Send only the filled part of the slice
  }
  close(chunks)
}

func processChunk(chunk []DataRow, results chan<- map[string]CityStats) {
  cityStats := make(map[string]CityStats, mapSize)
  for _, row := range chunk {
    stats, exists := cityStats[row.StationName]
    if !exists {
      stats = CityStats{Min: row.Temperature, Max: row.Temperature, Sum: row.Temperature, Count: 1}
    } else {
      if row.Temperature < stats.Min {
        stats.Min = row.Temperature
      }
      if row.Temperature > stats.Max {
        stats.Max = row.Temperature
      }
      stats.Sum += row.Temperature
      stats.Count++
    }
    cityStats[row.StationName] = stats
  }
  results <- cityStats
}

func mergeResults(cityStats map[string]CityStats, partialStats map[string]CityStats) {
  for city, partial := range partialStats {
    stats, exists := cityStats[city]
    if !exists {
      cityStats[city] = partial
    } else {
      if partial.Min < stats.Min {
        stats.Min = partial.Min
      }
      if partial.Max > stats.Max {
        stats.Max = partial.Max
      }
      stats.Sum += partial.Sum
      stats.Count += partial.Count
      cityStats[city] = stats
    }
  }
}

func writeResultsToFile(filePath string, cityStats map[string]CityStats) error {
  file, err := os.Create(filePath)
  if err != nil {
    return err
  }
  defer file.Close()

  for city, stats := range cityStats {
    mean := stats.Sum / float64(stats.Count)
    _, err := file.WriteString(fmt.Sprintf("%s;%.1f;%.1f;%.1f\n", city, stats.Min, mean, stats.Max))
    if err != nil {
      return err
    }
  }
  return nil
}

func captureMemoryProfile(filename string) {
  f, err := os.Create(filename)
  if err != nil {
    fmt.Printf("could not create memory profile: %v\n", err)
    return
  }
  defer f.Close()
  runtime.GC() // get up-to-date statistics
  if err := pprof.WriteHeapProfile(f); err != nil {
    fmt.Printf("could not write memory profile: %v\n", err)
    return
  }
}

func main() {
  var filePath string
  var chunkSize int
  var numWorkers int
  var outputFilePath string
  var blockSize int
  var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
  var memprofile = flag.String("memprofile", "", "write memory profile to file")

  flag.StringVar(&filePath, "file", "", "Path to the input file")
  flag.IntVar(&chunkSize, "chunk", 10000, "Number of rows per chunk")
  flag.IntVar(&numWorkers, "workers", 4, "Number of worker goroutines")
  flag.StringVar(&outputFilePath, "output", "", "Path to the output file")
  flag.IntVar(&blockSize, "blocksize", 4096, "Size of the buffer for reading the input file")

  flag.Parse()

  if filePath == "" || outputFilePath == "" || chunkSize <= 0 || numWorkers <= 0 || blockSize <= 0 {
    fmt.Println("Usage: process_data -file <input file> -chunk <chunk size> -workers <number of workers> -output <output file> -blocksize <buffer size> [-cpuprofile <file>] [-memprofile <file>]")
    flag.PrintDefaults()
    return
  }

  mapSize = min(10000, chunkSize)

  if *cpuprofile != "" {
    f, err := os.Create(*cpuprofile)
    if err != nil {
      fmt.Printf("could not create CPU profile: %v\n", err)
      return
    }
    defer f.Close()
    if err := pprof.StartCPUProfile(f); err != nil {
      fmt.Printf("could not start CPU profile: %v\n", err)
      return
    }
    defer pprof.StopCPUProfile()
  }

  if *memprofile != "" {
    captureMemoryProfile(fmt.Sprintf("%s_start.prof", *memprofile))
  }

  chunks := make(chan []DataRow, numWorkers+4)
  results := make(chan map[string]CityStats, numWorkers+8)

  var wg sync.WaitGroup

  // Start the file reader goroutine
  wg.Add(1)
  go readFileChunks(filePath, chunkSize, blockSize, chunks, &wg)

  if *memprofile != "" {
    captureMemoryProfile(fmt.Sprintf("%s_after_read.prof", *memprofile))
  }

  // Start worker goroutines
  for i := 0; i < numWorkers; i++ {
    wg.Add(1)
    go func() {
      defer wg.Done()
      for chunk := range chunks {
        processChunk(chunk, results)
      }
    }()
  }

  // Wait for all goroutines to finish
  go func() {
    wg.Wait()
    close(results)
  }()

  // Aggregate results
  cityStats := make(map[string]CityStats, mapSize)
  for partialStats := range results {
    mergeResults(cityStats, partialStats)
  }

  if *memprofile != "" {
    captureMemoryProfile(fmt.Sprintf("%s_after_processing.prof", *memprofile))
  }

  // Write results to file
  err := writeResultsToFile(outputFilePath, cityStats)
  if err != nil {
    fmt.Printf("Error writing results to file: %v\n", err)
  } else {
    fmt.Printf("Successfully wrote results to %s\n", outputFilePath)
  }

  if *memprofile != "" {
    captureMemoryProfile(fmt.Sprintf("%s_end.prof", *memprofile))
  }
}

