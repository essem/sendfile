package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"time"
)

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
	sendResultCh := make(chan sendResult, *chanSize)
	receiveResultCh := make(chan receiveResult, *chanSize)

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

		sendFull(cond, conn, headerBuf)
		sendFull(cond, conn, buf)

		result.total += len(buf)
	}

	stashCh <- nil
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

func receiver(cond condition, conn net.Conn, stashCh chan *sentInfo, resultCh chan receiveResult) {
	var result receiveResult
	headerBuf := make([]byte, 8)
	recvSeq := 1

	for {
		sent := <-stashCh
		if sent == nil {
			break
		}

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
