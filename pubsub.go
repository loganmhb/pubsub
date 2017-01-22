package main

import (
	"bufio"
	"fmt"
	"net"
)

// Basic idea: simplest possible pub/sub server (just TCP, no persistence, etc)
// sub by sending a topic name, which is a string that doesn't contain a colon
// pub by sending 'topic:msg', where the first colon delimits the topic from the msg
// when msgs are pub'd, they arrive on the TCP connection

// Subscriptions are kept in a global hashmap of string -> topic, where string is the
// topic name and topic is a struct that holds a channel for every subscriber.

// But first, even simpler: just broadcast messages.

type peer struct {
	outbox chan string
	conn   net.Conn
}

type topic struct {
	subscribers []*peer
}

func receiveMessages(p peer, topic *topic) {
	reader := bufio.NewReader(p.conn)
	defer p.conn.Close()

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("Error reading from connection ", p.conn, ", closing.")
				fmt.Println(err)
			}
			break
		}

		for _, recipient := range topic.subscribers {
			// Non-blocking send here, so that one bad peer doesn't prevent
			// delivery to the others. If the recipient's outbox is full, they just
			// won't receive the message.
			if p != *recipient {
				select {
				case recipient.outbox <- line:
					fmt.Println("delivered line to peer")
					continue
				default:
					fmt.Println("Failed to deliver message; peer's outbox is full.")
				}
			}
		}
	}
}

func deliverMessages(p peer, outbox chan string) {
	for {
		msg := <-outbox
		_, err := p.conn.Write(([]byte)(msg))
		if err != nil {
			fmt.Println("Error writing to conn: ", err)
			break
		}
	}
}

func handleConnection(conn net.Conn, topic *topic) *peer {
	// Each conn requires two goroutines: one to listen for incoming messages,
	// and one to deliver outgoing messages.
	outbox := make(chan string, 100)

	s := peer{outbox: outbox, conn: conn}
	go receiveMessages(s, topic)
	go deliverMessages(s, outbox)

	return &s
}

func main() {
	ln, err := net.Listen("tcp", ":8000")

	if err != nil {
		panic(err)
	}

	mainTopic := topic{subscribers: make([]*peer, 0)}

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
		} else {
			peerPtr := handleConnection(conn, &mainTopic)
			mainTopic.subscribers = append(mainTopic.subscribers, peerPtr)
			// TOOD: how to remove closed connections from this slice?
		}
	}
}
