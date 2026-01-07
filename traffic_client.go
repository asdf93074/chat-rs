package main

import (
	"fmt"
	"net"
	"os"
	"time"
)

const PORT = "9001" 
const SPAM_INTERVAL_S = 2

func sender(conn net.Conn) {
	lastMsg := time.Now() 
	for {
		if time.Since(lastMsg).Seconds() > SPAM_INTERVAL_S {
			lastMsg = time.Now()
			msg := []byte("Hi")	
			_,  err := conn.Write(msg)
			if err != nil {
				fmt.Printf("failed to send msg: %s\n%s", msg, err)
				break
			}
		}
	}
}

func client(conn net.Conn, done chan bool) {
	go sender(conn)

	for {
		buf := []byte{512: 0}
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Printf("failed to read: %s", err)
			break
		}
		if n > 0 {
			fmt.Printf("MSG: %s", buf[0:n])
		}
	}

	done <- true
}

func main() {
	fmt.Println("Hello, world.")
	addr := "0.0.0.0:"+PORT
	done := make(chan bool, 1)

	for range 100 {
		conn, err := net.Dial("tcp", addr) 

		if err != nil {
			err = fmt.Errorf("failed to connect to %s\n%s", addr, err)
			fmt.Println(err)
			os.Exit(1)
		}

		go client(conn, done)
	}
	<- done
}
