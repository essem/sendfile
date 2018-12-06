package main

import (
	"flag"
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	addr := flag.String("h", "localhost:3000", "Host to connect")
	bufSize := flag.Int("b", 300, "Buffer size")
	inFile := flag.String("i", "in.bin", "Input file name")
	verbose := flag.Bool("v", false, "Verbose")
	multiple := flag.Bool("m", false, "Multiple connect")
	flag.Parse()

	f, err := os.Open(*inFile)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
		return
	}

	defer f.Close()

	fmt.Printf("Connect to %s\n", *addr)

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
		return
	}

	fmt.Printf("Connected to %s\n", conn.RemoteAddr())

	totalSent := 0

	for {
		sent := send(conn, *bufSize, *verbose, f)
		if sent == 0 {
			break
		}
		totalSent += sent

		if *multiple {
			conn.Close()
			conn, err = net.Dial("tcp", *addr)
			if err != nil {
				fmt.Printf("Failed to connect: %v\n", err)
				return
			}
		}
	}

	fmt.Printf("Total sent: %d\n", totalSent)

	conn.Close()
}

func send(conn net.Conn, bufSize int, verbose bool, f *os.File) int {
	buf := make([]byte, bufSize)
	read, err := f.Read(buf)
	if err == io.EOF {
		fmt.Println("Connection closed")
		if read != 0 {
			fmt.Printf("Unexpected: %d", read)
			os.Exit(1)
		}
		return 0
	} else if err != nil {
		fmt.Printf("Failed to read: %v\n", err)
		os.Exit(1)
	}

	if verbose {
		fmt.Printf("Recevied: %d\n", read)
	}

	sent := 0
	for sent < read {
		n, err := conn.Write(buf[sent:read])
		if err != nil {
			fmt.Printf("Failed to send: %v\n", err)
			os.Exit(1)
		}

		if verbose {
			fmt.Printf("Sent: %d\n", n)
		}

		sent += n
	}

	return sent
}
