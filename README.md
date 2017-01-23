# pubsub

Very simple pub/sub message broker.

Run the binary, then connect clients with telnet:

    ./pubsub

    # in another terminal
    telnet localhost 8000
    # in a third terminal
    telnet localhost 8000


When you type a line into one of the clients, you should see it appear in the other. You can subscribe to a topic by typing the topic name followed by a newline (cannot contain a colon), and broadcast a message on a topic by typing the topic name, a colon, the message body and a newline.