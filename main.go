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
	flag.Parse()

	fmt.Printf("Connect to %s\n", *addr)

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Printf("Failed to connect: %v\n", err)
	}

	defer conn.Close()

	fmt.Printf("Connected to %s\n", conn.RemoteAddr())

	f, err := os.Open(*inFile)
	if err != nil {
		fmt.Printf("Failed to open file: %v\n", err)
	}

	defer f.Close()

	totalSent := 0

	for {
		buf := make([]byte, *bufSize)
		read, err := f.Read(buf)
		if err == io.EOF {
			fmt.Println("Connection closed")
			if read != 0 {
				fmt.Printf("Unexpected: %d", read)
			}
			break
		} else if err != nil {
			fmt.Printf("Failed to read: %v\n", err)
		}

		if *verbose {
			fmt.Printf("Recevied: %d\n", read)
		}

		sent := 0
		for sent < read {
			n, err := conn.Write(buf[sent:read])
			if err != nil {
				fmt.Printf("Failed to send: %v\n", err)
			}

			if *verbose {
				fmt.Printf("Sent: %d\n", n)
			}

			sent += n
			totalSent += n
		}
	}

	fmt.Printf("Total sent: %d\n", totalSent)
}
