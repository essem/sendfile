package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
)

type condition struct {
	totalSize  int64
	bufSize    int
	bufSizeVar int
	verbose    bool
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
	bufSizeVar := flag.Int("bv", 100, "Buffer size variation")
	chanSize := flag.Int("c", 1, "Channel size")
	inFile := flag.String("i", "in.bin", "Input file name")
	outFile := flag.String("o", "", "Output file name")
	verbose := flag.Bool("v", false, "Verbose")
	flag.Parse()

	cond := condition{
		totalSize:  0,
		bufSize:    *bufSize,
		bufSizeVar: *bufSizeVar,
		verbose:    *verbose,
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

	readOutCh := make(chan []byte, *chanSize)
	go reader(cond, file, readOutCh, readResultCh)
	go sender(cond, conn, readOutCh, sendResultCh)
	if oFile != nil {
		receiveOutCh := make(chan []byte, *chanSize)
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
		bufSize := cond.bufSize + rand.Intn(cond.bufSizeVar*2+1) - cond.bufSizeVar // (0,1,2,3,4) - 2 => -2,-1,0,1,2
		buf := make([]byte, bufSize)
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

func sendFull(cond condition, conn net.Conn, buf []byte) {
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
}

func sender(cond condition, conn net.Conn, inCh chan []byte, resultCh chan sendResult) {
	var result sendResult
	headerBuf := make([]byte, 8)
	sendSeq := 1

	for buf := range inCh {

		binary.BigEndian.PutUint32(headerBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(headerBuf[4:8], uint32(sendSeq))

		sendFull(cond, conn, headerBuf)
		sendFull(cond, conn, buf)

		result.total += len(buf)
	}

	resultCh <- result
}

func recvFull(cond condition, conn net.Conn, buf []byte) error {
	received := 0
	for received < len(buf) {
		n, err := conn.Read(buf[received:])
		if err == io.EOF {
			fmt.Println("Connection closed")
			if received != 0 {
				fmt.Printf("Unexpected: %d", received)
				os.Exit(1)
			}
			return err
		} else if err != nil {
			fmt.Printf("Failed to recv: %v\n", err)
			os.Exit(1)
		}

		received += n
	}
	return nil
}

func receiver(cond condition, conn net.Conn, outCh chan []byte, resultCh chan receiveResult) {
	var result receiveResult
	headerBuf := make([]byte, 8)
	recvSeq := 1

	for {
		err := recvFull(cond, conn, headerBuf)
		if err != nil {
			break
		}

		bufSize := int(binary.BigEndian.Uint32(headerBuf[0:4]))
		seq := int(binary.BigEndian.Uint32(headerBuf[4:8]))

		if seq != recvSeq {
			fmt.Printf("Invalid seq: expected %d, actual %d", recvSeq, seq)
			os.Exit(1)
		}

		buf := make([]byte, bufSize)
		err = recvFull(cond, conn, buf)
		if err != nil {
			break
		}

		if outCh != nil {
			outCh <- buf
		}

		result.total += bufSize

		if cond.verbose {
			fmt.Printf("Recevied: %d\n", bufSize)
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
