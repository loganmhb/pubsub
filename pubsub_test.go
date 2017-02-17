package main

import (
	"testing"
)

type StubPeer struct {
	msgsReceived []Msg
}

func (p *StubPeer) Deliver(m Msg) {
	p.msgsReceived = append(p.msgsReceived, m)
}

func TestBrokerSubscribe(t *testing.T) {
	peer := &StubPeer{msgsReceived: make([]Msg, 5)}
	subscription := &Subscription{topic: "hi", client: peer}
	broker := NewBroker()
	broker.Subscribe(*subscription)	
	select {
	case s := <-broker.subscriptions:
		if s != *subscription {
			t.Error("Wrong subscription on channel")
		}
	default:
		t.Error("No subscription on channel")
	}
}

func TestBrokerBroadcast(t *testing.T) {
	
}
