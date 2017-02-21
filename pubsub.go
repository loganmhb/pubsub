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

// The Client interface is implemented by peers, handling network connections and
// receiving messages.
type Client interface {
	Deliver(Msg)
	Connect(LineStream, Dispatcher)
}

// The Dispatcher interface is implemented by the broker, and handles
// broadcasting messages and tracking which clients are subscribed to
// which topics.
type Dispatcher interface {
	// used for sending a message
	Broadcast(Msg)
	// used to subscribe a peer to a topic
	Subscribe(Subscription)
	// used to remove all references to a peer, e.g. in case of network error
	Disconnect(Client)
}

// LineStream encapsulates the behavior of the TCP connection that
// we'll be using, which will be helpful for testing.
type LineStream interface {
	ReadLine() (string, error)
	WriteLine(string) error
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
	subscriptions chan Subscription
	broadcasts    chan Msg
	disconnects   chan Client
	topics        map[string](map[Client]interface{})
}

func NewBroker() *Broker {
	return &Broker{
		subscriptions: make(chan Subscription),
		broadcasts:    make(chan Msg),
		disconnects:   make(chan Client),
		topics:        make(map[string](map[Client]interface{}))}
}

// Starts a loop listening for incoming broadcast and subscription
// requests, handling each appropriately -- for subscription requests,
// adds a peer to a topic, for broadcast requests, broadcasts the
// message to subscribed peers.
func (b *Broker) Run() {
	for {
		select {
		case client := <-b.disconnects:
			for topic, _ := range b.topics {
				delete(b.topics[topic], client)
			}
			fmt.Println(b.topics)
		case sub := <-b.subscriptions:
			_, topicExists := b.topics[sub.topic]
			if !topicExists {
				b.topics[sub.topic] = make(map[Client]interface{}, 0)
			}
			b.topics[sub.topic][sub.client] = true // value doesn't matter
		case msg := <-b.broadcasts:
			topic, _ := b.topics[msg.topic]
			for client, _ := range topic {
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

//TODO disconnect a client (e.g. unsubscribe from all channels)
func (b *Broker) Disconnect(client Client) {
	b.disconnects <- client
}

func NewPeer() *Peer {
	return &Peer{inbox: make(chan Msg, 100)}
}

// Connect "connects" a peer to the broker, starting two goroutines. One reads
// lines off the stream, parses them into events (subscriptions,
// broadcasts), and sends them to the broker. The other listens for
// broadcasts on the peer's inbox and writes messages out onto the
// stream.
func (p *Peer) Connect(stream LineStream, broker Dispatcher) {
	go func() {
		for {
			line, err := stream.ReadLine()
			if err != nil {
				fmt.Println("Error reading from stream. Disconnecting peer", p)
				broker.Disconnect(p)
				break
			}

			event := p.ParseLine(line)
			switch event.(type) {
			case Msg:
				broker.Broadcast(event.(Msg))
			case Subscription:
				broker.Subscribe(event.(Subscription))
			}
		}
	}()

	go func() {
		for broadcast := range p.inbox {
			s := broadcast.topic + ":" + broadcast.body
			err := stream.WriteLine(s)
			if err != nil {
				fmt.Println("Error writing to client",p,", disconnecting.")
				broker.Disconnect(p)
				break
			}
		}
	}()
}

// ParseLine parses message strings (presumably from off the wire)
// into subscription requests and message broadcasts.
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
		fmt.Println("Failed to deliver message to peer.")
		// message not delivered -- any action needed? disconnect peer?
	}
}

// NetworkStream is a LineStream implementation for a TCP connection.
type NetworkStream struct {
	reader *bufio.Reader
	conn   net.Conn
}

func NewNetworkStream(conn net.Conn) NetworkStream {
	return NetworkStream{reader: bufio.NewReader(conn), conn: conn}
}

func (s NetworkStream) ReadLine() (string, error) {
	line, err := s.reader.ReadString('\n')

	return strings.Trim(line, "\r\n"), err
}

func (s NetworkStream) WriteLine(line string) error {
	_, err := s.conn.Write(([]byte)(line + "\r\n"))
	return err
}

func main() {
	ln, err := net.Listen("tcp", ":8000")

	if err != nil {
		panic(err)
	}

	broker := NewBroker()
	go broker.Run()

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err)
		} else {
			peer := NewPeer()
			stream := NewNetworkStream(conn)
			peer.Connect(stream, broker)
		}
	}
}
