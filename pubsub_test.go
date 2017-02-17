package main

import (
	"testing"
	"time"
)

type StubPeer struct {
	msgsReceived []Msg
}

func (p *StubPeer) Deliver(m Msg) {
	p.msgsReceived = append(p.msgsReceived, m)
}

func TestBrokerSubscribe(t *testing.T) {
	peer := &StubPeer{msgsReceived: make([]Msg, 0)}
	subscription := &Subscription{topic: "hi", client: peer}
	broker := NewBroker()
	go broker.Run()
	broker.Subscribe(*subscription)	
	if broker.topics["hi"][0] != peer {
		t.Fail()
	}
}

func TestPeersReceiveMessages(t *testing.T) {
	peer := &StubPeer{msgsReceived: make([]Msg, 0)}
	peer2 := &StubPeer{msgsReceived: make([]Msg, 0)}
	subscription := &Subscription{topic: "hi", client: peer}
	subscription2 := &Subscription{topic: "hi", client: peer2}
	message := &Msg{topic: "hi", body:"test"}
	broker := NewBroker()
	go broker.Run()
	broker.Subscribe(*subscription)
	broker.Subscribe(*subscription2)
	broker.Broadcast(*message)
	for (len(peer.msgsReceived) < 1) {
		time.Sleep(5)
	}
	if peer.msgsReceived[0] != *message {
		t.Fail()
	}
	if peer2.msgsReceived[0] != *message {
		t.Fail()
	}
}

func TestMessageParsing(t *testing.T) {
	peer := Peer{}
	m, mok := peer.ParseLine("topic:i'm a message").(Msg)
	s, sok := peer.ParseLine("i'm a topic subscription").(Subscription)
	if !mok || !sok {
		t.Fail()
	}
	if !(m == Msg{topic: "topic", body: "i'm a message"}) {
		t.Fail()
	}
	if !(s == Subscription{topic:"i'm a topic subscription", client: &peer}) {
		t.Fail()
	}
}

