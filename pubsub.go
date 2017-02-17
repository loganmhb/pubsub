package main

import (
	//	"bufio"
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

// The Client interface is implemented by peers, handling network connections and
// receiving messages.
type Client interface {
	Deliver(Msg)
}

// The Dispatcher interfact is implemented by the broker, and handles
// broadcasting messages and tracking which clients are subscribed to
// which topics.
type Dispatcher interface {
	// used for sending a message
	Broadcast(Msg)
	// used to subscribe a peer to a topic
	Subscribe(Client, string)
	// used to remove all references to a peer, e.g. in case of network error
	Disconnect(Client)
}

type Peer struct {
	inbox chan Msg
	conn  net.Conn
}

type Msg struct {
	body  string
	topic string
}

type Subscription struct {
	topic  string
	client Client
}

type Broker struct {
	subscriptions  chan Subscription
	broadcasts     chan Msg
	disconnections chan Client
	topics         map[string]([]Client)
}

func NewBroker() *Broker {
	return &Broker{
		subscriptions:  make(chan Subscription),
		broadcasts:     make(chan Msg),
		disconnections: make(chan Client),
		topics:         make(map[string]([]Client))}
}

// Starts a loop listening for incoming broadcast and subscription
// requests, handling each appropriately -- for subscription requests,
// adds a peer to a topic, for broadcast requests, broadcasts the
// message to subscribed peers.
func (b *Broker) Run() {
	for {
		select {
		case sub := <-b.subscriptions:
			topic, topicExists := b.topics[sub.topic]
			if !topicExists {
				topic = make([]Client, 0)
			}
			b.topics[sub.topic] = append(topic, sub.client)
		case msg := <-b.broadcasts:
			topic, _ := b.topics[msg.topic]
			for _, client := range topic {
				client.Deliver(msg)
			}
		}
	}
}

// Sends the broker a message to broadcast.
func (b *Broker) Broadcast(msg Msg) {
	b.broadcasts <- msg
}

// Sends the broker a subscription request to add a client to a topic.
func (b *Broker) Subscribe(sub Subscription) {
	b.subscriptions <- sub
}

// Parses messages off the wire into subscription requests and message
// broadcasts.
func (p *Peer) ParseLine(line string) interface{} {
	if strings.Contains(line, ":") {
		idx := strings.Index(line, ":")
		return Msg{topic: line[:idx], body: line[idx+1:]}
	} else {
		return Subscription{client: p, topic: line}
	}
}

// Peer.Deliver implements a non-blocking send. If a Peer's inbox is
// full, then it doesn't get the message (presumably there's a problem
// with the connection).
func (p *Peer) Deliver(m Msg) {
	select {
	case p.inbox <- m:
		// message delivered
	default:
		// message not delivered -- any action needed? disconnect peer?
	}
}

// // Parses messages from the peer's TCP connection and forwards them to
// // the broker for broadcasting.
// func sendBroadcasts(p Peer, broker *Broker) {
// 	reader := bufio.NewReader(p.conn)

// 	for
// 		line, err := reader.ReadString('\n')

// 		if err != nil {
// 			if err.Error() != "EOF" {
// 				fmt.Println("Error reading from connection ", p.conn, ", closing.")
// 				fmt.Println(err)
// 			}
// 			break
// 		}

// 		if strings.Contains(line, ":") {
// 			// It's a pub.
// 		} else {
// 			broker.subscriptions <- Subscription{
// 				inbox: p.inbox,
// 				topic: strings.Trim(line, "\r\n")}
// 		}
// 	}
// }

// // Listens for broadcasts arriving in the peer's inbox and writes them
// // out on the TCP connection.
// func deliverBroadcasts(p Peer) {
// 	for msg := range p.inbox {
// 		payload := msg.topic + ": " + msg.body
// 		_, err := p.conn.Write(([]byte)(payload))
// 		if err != nil {
// 			fmt.Println("Error writing to conn: ", err)
// 			break
// 		}
// 	}
// }

// func connectPeer(conn net.Conn, broker *Broker) *Peer {
// 	// Each conn requires two goroutines: one to listen for incoming messages,
// 	// and one to deliver outgoing messages.
// 	inbox := make(chan Msg, 100)

// 	peer := Peer{inbox: inbox, conn: conn}
// 	go deliverBroadcasts(peer)
// 	go sendBroadcasts(peer, broker)

// 	return &peer
// }

// func runBroker(broker Broker) {
// 	topics := make(map[string]Topic)
// 	for {
// 		select {
//
// 	}
// }

// func main() {
// 	ln, err := net.Listen("tcp", ":8000")

// 	if err != nil {
// 		panic(err)
// 	}

// 	outbox := make(chan Msg, 100)
// 	subscriptions := make(chan Subscription, 100)
// 	broker := Broker{broadcasts: outbox, subscriptions: subscriptions}

// 	go runBroker(broker)

// 	for {
// 		conn, err := ln.Accept()
// 		if err != nil {
// 			fmt.Println("Error accepting connection: ", err)
// 		} else {
// 			connectPeer(conn, &broker)
// 		}
// 	}
// }

func main() {}
