package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

type condition struct {
	totalSize int64
	bufSize   int
	verbose   bool
}

type readResult struct {
	total int
}

type sendResult struct {
	total int
}

type receiveResult struct {
	total int
}

type writeResult struct {
	total int
}

func main() {
	addr := flag.String("h", "localhost:3000", "Host to connect")
	bufSize := flag.Int("b", 300, "Buffer size")
	chanSize := flag.Int("c", 1, "Channel size")
	inFile := flag.String("i", "in.bin", "Input file name")
	outFile := flag.String("o", "", "Output file name")
	verbose := flag.Bool("v", false, "Verbose")
	flag.Parse()

	cond := condition{
		totalSize: 0,
		bufSize:   *bufSize,
		verbose:   *verbose,
	}

	fmt.Printf("Connect to %s\n", *addr)

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}

	defer conn.Close()

	fmt.Printf("Connected to %s\n", conn.RemoteAddr())

	file, err := os.Open(*inFile)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}

	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		fmt.Printf("Failed to stat file: %v\n", err)
		return
	}

	cond.totalSize = fi.Size()

	var oFile *os.File
	if *outFile != "" {
		f, err := os.Create(*outFile)
		if err != nil {
			fmt.Printf("Failed to create file: %v\n", err)
			return
		}

		oFile = f

		fmt.Printf("Write file to %s\n", *outFile)
	}

	// Prepare channels and launch goroutines

	readResultCh := make(chan readResult, *chanSize)
	sendResultCh := make(chan sendResult, *chanSize)
	receiveResultCh := make(chan receiveResult, *chanSize)
	writeResultCh := make(chan writeResult, *chanSize)

	readOutCh := make(chan []byte)
	go reader(cond, file, readOutCh, readResultCh)
	go sender(cond, conn, readOutCh, sendResultCh)
	if oFile != nil {
		receiveOutCh := make(chan []byte)
		go receiver(cond, conn, receiveOutCh, receiveResultCh)
		go writer(cond, oFile, receiveOutCh, writeResultCh)
	}

	// Wait goroutines and check result

	readResult := <-readResultCh
	sendResult := <-sendResultCh
	if readResult.total != sendResult.total {
		fmt.Printf("Result not match: read %d send %d\n", readResult.total, sendResult.total)
		os.Exit(1)
	}
	if oFile != nil {
		receiveResult := <-receiveResultCh
		if readResult.total != receiveResult.total {
			fmt.Printf("Result not match: read %d receive %d\n", readResult.total, receiveResult.total)
			os.Exit(1)
		}
		writeResult := <-writeResultCh
		if readResult.total != writeResult.total {
			fmt.Printf("Result not match: read %d write %d\n", readResult.total, writeResult.total)
			os.Exit(1)
		}
	}

	fmt.Printf("Total sent: %d\n", readResult.total)
}

func reader(cond condition, file *os.File, outCh chan []byte, resultCh chan readResult) {
	var result readResult

	for {
		buf := make([]byte, cond.bufSize)
		read, err := file.Read(buf)
		if err == io.EOF {
			fmt.Println("Connection closed")
			if read != 0 {
				fmt.Printf("Unexpected: %d", read)
				os.Exit(1)
			}
			break
		} else if err != nil {
			fmt.Printf("Failed to read: %v\n", err)
			os.Exit(1)
		}

		if outCh != nil {
			outCh <- buf[:read]
		}

		result.total += read

		if cond.verbose {
			fmt.Printf("Read: %d\n", read)
		}
	}

	if outCh != nil {
		close(outCh)
	}

	resultCh <- result
}

func sender(cond condition, conn net.Conn, inCh chan []byte, resultCh chan sendResult) {
	var result sendResult
	for buf := range inCh {
		sent := 0
		for sent < len(buf) {
			n, err := conn.Write(buf[sent:])
			if err != nil {
				fmt.Printf("Failed to send: %v\n", err)
				os.Exit(1)
			}

			if cond.verbose {
				fmt.Printf("Sent: %d\n", n)
			}

			sent += n
		}

		result.total += sent
	}

	resultCh <- result
}

func receiver(cond condition, conn net.Conn, outCh chan []byte, resultCh chan receiveResult) {
	var result receiveResult

	for {
		buf := make([]byte, cond.bufSize)
		received, err := conn.Read(buf)
		if err == io.EOF {
			fmt.Println("Connection closed")
			if received != 0 {
				fmt.Printf("Unexpected: %d", received)
				os.Exit(1)
			}
			break
		} else if err != nil {
			fmt.Printf("Failed to recv: %v\n", err)
			os.Exit(1)
		}

		if outCh != nil {
			outCh <- buf[:received]
		}

		result.total += received

		if cond.verbose {
			fmt.Printf("Recevied: %d\n", received)
		}

		if int64(result.total) == cond.totalSize {
			break
		}
	}

	if outCh != nil {
		close(outCh)
	}

	resultCh <- result
}

func writer(cond condition, file *os.File, inCh chan []byte, resultCh chan writeResult) {
	var result writeResult

	for buf := range inCh {
		written := 0
		for written < len(buf) {
			n, err := file.Write(buf[written:])
			if err != nil {
				fmt.Printf("Failed to write: %v\n", err)
				os.Exit(1)
			}

			if cond.verbose {
				fmt.Printf("Written: %d\n", n)
			}

			written += n
		}

		result.total += written
	}

	resultCh <- result
}
