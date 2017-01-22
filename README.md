# pubsub

Very simple pub/sub message broker.

Run the binary, then connect clients with telnet:

    ./pubsub

    # in another terminal
    telnet localhost 8000
    # in a third terminal
    telnet localhost 8000


When you type a line into one of the clients, you should see it appear in the other.