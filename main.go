package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"time"
)

const headerSize = 8

type condition struct {
	totalSize  int64
	bufSize    int
	bufSizeVar int
	verbose    bool
}

type sentInfo struct {
	reqTime time.Time
	buffer  []byte
}

type sendResult struct {
	total int
}

type receiveResult struct {
	total int
}

func main() {
	addr := flag.String("h", "localhost:3000", "Host to connect")
	bufSize := flag.Int("b", 300, "Buffer size")
	bufSizeVar := flag.Int("bv", 100, "Buffer size variation")
	chanSize := flag.Int("c", 1, "Channel size")
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

	// Prepare channels and launch goroutines

	stashCh := make(chan *sentInfo, *chanSize)
	sendResultCh := make(chan sendResult, 1)
	receiveResultCh := make(chan receiveResult, 1)

	go sender(cond, conn, stashCh, sendResultCh)
	go receiver(cond, conn, stashCh, receiveResultCh)

	// Wait goroutines and check result

	sendResult := <-sendResultCh
	receiveResult := <-receiveResultCh
	if sendResult.total != receiveResult.total {
		fmt.Printf("Result not match: send %d receive %d\n", sendResult.total, receiveResult.total)
		os.Exit(1)
	}

	fmt.Printf("Total sent: %d\n", sendResult.total)
}

func recvFull(rwc net.Conn, buf []byte) error {
	pos := 0
	for pos < len(buf) {
		n, err := rwc.Read(buf[pos:])
		if err != nil {
			return err
		}
		pos += n
	}
	return nil
}

func sendFull(rwc net.Conn, buf []byte) error {
	pos := 0
	for pos < len(buf) {
		n, err := rwc.Write(buf[pos:])
		if err != nil {
			return err
		}
		pos += n
	}
	return nil
}

func sender(cond condition, conn net.Conn, stashCh chan *sentInfo, resultCh chan sendResult) {
	var result sendResult
	headerBuf := make([]byte, 8)
	sendSeq := 1
	startTime := time.Now()

	for time.Since(startTime) < time.Duration(1)*time.Second {
		bufSize := cond.bufSize + rand.Intn(cond.bufSizeVar*2+1) - cond.bufSizeVar // (0,1,2,3,4) - 2 => -2,-1,0,1,2
		buf := make([]byte, bufSize)
		rand.Read(buf)

		stashCh <- &sentInfo{time.Now(), buf}

		binary.BigEndian.PutUint32(headerBuf[0:4], uint32(len(buf)))
		binary.BigEndian.PutUint32(headerBuf[4:8], uint32(sendSeq))

		err := sendFull(conn, headerBuf)
		if err != nil {
			fmt.Printf("Failed to send header: %v\n", err)
			os.Exit(1)
		}

		err = sendFull(conn, buf)
		if err != nil {
			fmt.Printf("Failed to send body: %v\n", err)
			os.Exit(1)
		}

		result.total += len(buf)
	}

	stashCh <- nil
	resultCh <- result
}

func receiver(cond condition, conn net.Conn, stashCh chan *sentInfo, resultCh chan receiveResult) {
	var result receiveResult
	recvBuf := make([]byte, cond.bufSize+cond.bufSizeVar)
	recvSeq := 1

	for {
		sent := <-stashCh
		if sent == nil {
			break
		}

		headerBuf := recvBuf[:headerSize]
		err := recvFull(conn, headerBuf)
		if err != nil {
			//TODO handle close
			fmt.Printf("Failed to read message: %v\n", err)
			os.Exit(1)
		}

		bufSize := int(binary.BigEndian.Uint32(headerBuf[0:4]))
		seq := int(binary.BigEndian.Uint32(headerBuf[4:8]))

		if seq != recvSeq {
			fmt.Printf("Invalid seq: expected %d, actual %d", recvSeq, seq)
			os.Exit(1)
		}

		buf := recvBuf[:bufSize]
		err = recvFull(conn, buf)
		if err != nil {
			fmt.Printf("Failed to read message: %v\n", err)
			os.Exit(1)
		}

		if !bytes.Equal(sent.buffer, buf) {
			fmt.Println("Messages are not equal")
			os.Exit(1)
		}

		result.total += bufSize

		if cond.verbose {
			fmt.Printf("Recevied: %d\n", bufSize)
		}
	}

	resultCh <- result
}
