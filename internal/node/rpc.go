package node

import (
	"encoding/json"
	"net" // communication package for TCP connections
)

// The Request struct allows us to define the structure of the incoming requests that the node will handle.
type Request struct {
	Operation string `json:"operation"`
	Key       string `json:"key"`
	Value     string `json:"value"`

	// Raft fields
	Term        int    `json:"term"`
	CandidateID string `json:"candidate_id"`
}

// The Response struct allows us to define the structure of the outgoing responses that the node will send back to the clients.
// (This is mainly used for the GET operation, where we need to send back the value associated with a key and whether the key was found or not.)
type Response struct {
	Value string `json:"value"`
	Found bool   `json:"found"`
	Error string `json:"error"`

	// Raft fields
	Term        int  `json:"term"`
	VoteGranted bool `json:"vote_granted"`
}

func (n *Node) StartRPC() error {
	listener, err := net.Listen("tcp", n.Address)
	// Creates a listener on the specified address and port.
	// The listener will accept incoming connections from clients.
	if err != nil {
		return err
	}

	for {
		conn, err := listener.Accept()
		// Accepts incoming connections from clients.
		if err != nil {
			return err
		}

		go n.handleConnection(conn)
		// go initiates a new goroutine to handle the connection concurrently.
		// This allows the node to handle multiple connections without blocking the main thread,
		// improving scalability and responsiveness.
		// The benefit of Go is that it allows for Goroutines, which are a lightweight mechanism for concurrent execution.
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	buffer := make([]byte, 1024)
	// Create space for the incoming data.
	// The buffer is a byte slice of size 1024 bytes.
	// This is a common pattern in Go for reading data from a connection.

	bytesRead, err := conn.Read(buffer)
	// Stores the number of bytes that were read into bytesRead.
	if err != nil {
		return
	}

	data := buffer[:bytesRead]
	// Only reading the bytes that were actually read from the connection,
	// rather than the entire buffer.

	var request Request
	// Set up a Request instance to hold the decoded data from the incoming request.

	err = json.Unmarshal(data, &request)
	if err != nil {
		return
	}

	var response Response
	// Set up a Response instance to hold the data that will be sent back to the client.

	switch request.Operation {

	case "PUT":
		n.Store.Put(request.Key, request.Value)

	case "GET":
		value, ok := n.Store.Get(request.Key)
		response.Value = value
		response.Found = ok

	case "DELETE":
		n.Store.Delete(request.Key)

	case "REQUEST_VOTE":
		voteRequest := RequestVoteRequest{
			Term:        request.Term,
			CandidateID: request.CandidateID,
		}

		voteResponse := n.handleRequestVote(voteRequest)

		response.Term = voteResponse.Term
		response.VoteGranted = voteResponse.VoteGranted

	default:
		response.Error = "unknown operation"
	}

	responseData, err := json.Marshal(response)
	if err != nil {
		return
	}

	_, err = conn.Write(responseData)
	if err != nil {
		return
	}
}
