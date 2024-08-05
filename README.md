# 1 Billion Row Challenge Solution

This Go application is designed to process a large dataset efficiently, specifically targeting the 1 billion row challenge. The program reads data from a file, processes it in chunks using multiple goroutines, and calculates statistics for each city based on the temperature data provided.

## Features

- **Chunked Processing:** Reads and processes data in chunks to handle large files efficiently.
- **Concurrency:** Utilizes multiple goroutines to parallelize data processing, optimizing performance.
- **Memory Profiling:** Supports memory profiling to analyze and optimize memory usage.
- **CPU Profiling:** Supports CPU profiling to analyze and optimize performance.
- **Efficient Statistics Calculation:** Computes minimum, maximum, and average temperatures for each city.

## Usage

### Building the Application

To build the application, use the following command:

```sh
go build -o process_data main.go

### Running the Application 

./process_data -file <input_file> -chunk <chunk_size> -workers <number_of_workers> -output <output_file> -blocksize <buffer_size> [-cpuprofile <file>] [-memprofile <file>]

-file: Path to the input file containing the dataset.
-chunk: Number of rows per chunk (default: 10000).
-workers: Number of worker goroutines (default: 4).
-output: Path to the output file where results will be saved.
-blocksize: Size of the buffer for reading the input file (default: 4096).
-cpuprofile: Path to the file for writing CPU profile (optional).
-memprofile: Path to the file for writing memory profile (optional).
