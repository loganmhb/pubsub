package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

// Basic idea: simplest possible pub/sub server (just TCP, no persistence, etc)
// sub by sending a topic name, which is a string that doesn't contain a colon
// pub by sending 'topic:msg', where the first colon delimits the topic from the msg
// when msgs are pub'd, they arrive on the TCP connection

// Subscriptions are kept in a global hashmap of string -> topic, where string is the
// topic name and topic is a struct that holds a channel for every subscriber.

// = The protocol =//
//
// A message is a line of text sent over a TCP connection. It can be
// either a publication or a subscription.
//
// A line containing a colon is a publication to
// '<topic>:<message>'. Messages may contain subsequent colons but
// topic names cannot contain any.
//
// A message that does _not_ contain a colon is treated as a topic
// name, and the peer is subscribed to that topic.
//
// = Design for Topics = //
//
// Peers can subscribe to any number of topics, and publish to any topic.
// If the topic doesn't yet exist, subscribing to it will create it. Publishing to
// a topic to which no peers are subscribed is a no-op.
//
// Subscriptions are stored in a map of topic name (string) to list of
// inboxes (a channel belonging to a peer). In order to subscribe to a
// new topic, a peer sends a message on a special subscription channel
// with the peer's inbox and the desired topic name. When a peer
// connects, two goroutines start: one to parse messages coming in on
// the connection and send them to the broker for distribution, and
// one to write broadcasted messages from the broker back to the
// client.
//
// Disconnected peers are currently not handled, which is a problem if this thing
// runs for long enough, of course.

type Peer struct {
	inbox chan Msg
	conn  net.Conn
}

type Topic [](chan Msg)

type Msg struct {
	body  string
	topic string
}

type Subscription struct {
	topic string
	inbox chan Msg
}

type Broker struct {
	subscriptions chan Subscription
	broadcasts    chan Msg
}

// Parses messages from the peer's TCP connection and forwards them to
// the broker for broadcasting.
func sendBroadcasts(p Peer, broker *Broker) {
	reader := bufio.NewReader(p.conn)

	for {
		line, err := reader.ReadString('\n')

		if err != nil {
			if err.Error() != "EOF" {
				fmt.Println("Error reading from connection ", p.conn, ", closing.")
				fmt.Println(err)
			}
			break
		}

		if strings.Contains(line, ":") {
			// It's a pub.
			idx := strings.Index(line, ":")
			msg := Msg{topic: line[:idx], body: line[idx+1:]}
			broker.broadcasts <- msg
		} else {
			broker.subscriptions <- Subscription{
				inbox: p.inbox,
				topic: strings.Trim(line, "\r\n")}
		}
	}
}

// Listens for broadcasts arriving in the peer's inbox and writes them
// out on the TCP connection.
func deliverBroadcasts(p Peer) {
	for msg := range(p.inbox) {
		payload := msg.topic + ": " + msg.body
		_, err := p.conn.Write(([]byte)(payload))
		if err != nil {
			fmt.Println("Error writing to conn: ", err)
			break
		}
	}
}

func connectPeer(conn net.Conn, broker *Broker) *Peer {
	// Each conn requires two goroutines: one to listen for incoming messages,
	// and one to deliver outgoing messages.
	inbox := make(chan Msg, 100)

	peer := Peer{inbox: inbox, conn: conn}
	go deliverBroadcasts(peer)
	go sendBroadcasts(peer, broker)

	return &peer
}

func runBroker(broker Broker) {
	topics := make(map[string]Topic)
	for {
		select {
		case sub := <-broker.subscriptions:
			topic, topicExists := topics[sub.topic]
			if !topicExists {
				topic = make(Topic, 0)
			}
			topics[sub.topic] = append(topic, sub.inbox)
		case msg := <-broker.broadcasts:
			topic, _ := topics[msg.topic]
			for _, recipient := range(topic) {
				// Non-blocking send. If recipient's inbox is full, they
				// don't get the message.
				select {
				case recipient <- msg:
					continue
				default:
					fmt.Println("Failed to deliver message to peer.")
				}
			}
		}
	}
}

func main() {
	ln, err := net.Listen("tcp", ":8000")

	if err != nil {
		panic(err)
	}

	outbox := make(chan Msg, 100)
	subscriptions := make(chan Subscription, 100)
	broker := Broker{broadcasts: outbox, subscriptions: subscriptions}

	go runBroker(broker)

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
		} else {
			connectPeer(conn, &broker)
		}
	}
}
