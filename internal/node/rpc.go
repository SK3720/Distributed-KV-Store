package node

import ("encoding/json", "net") //communication package for TCP connections

type Request struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`
}

func (n *Node) StartRPC() error {
	listener, err := net.Listen("tcp", n.Address) //creates a listener on the specified address and port. The listener will accept incoming connections from clients.
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept() //accepts incoming connections from clients.
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}

		go n.handleConnection(conn) //go initiates a new goroutine to handle the connection concurrently --> allows the node to handle multiple connections without blocking the main thread --> scalability and responsiveness.
		//so the benefit of Go is that it allows for Goroutines, which are a lightweight mechanism for concurrent execution.
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024) //create space for the incoming data. The buffer is a byte slice of size 1024 bytes. This is a common pattern in Go for reading data from a connection..

	bytesRead, err := conn.Read(buffer) //stores the bytes into bytesRead.
	if err != nil {
		return
	}

	data := buffer[:bytesRead] //only reading the bytes that were actually read from the connection, rather than the entire buffer.
	
	var request Request

	err = json.Unmarshal(data, &request)
	if err != nil {
		return
	}

	// Request is now decoded and ready to process.
}
