package node

import (
	"encoding/json"
	"net"
)

// Client communicates with a node over TCP.
type Client struct {
	Address string
}

// NewClient creates a new RPC client.
func NewClient(address string) *Client {
	return &Client{
		Address: address,
	}
}

// Get retrieves a value from the node.
func (c *Client) Get(key string) (string, bool, error) {
	conn, err := net.Dial("tcp", c.Address)
	if err != nil {
		return "", false, err
	}
	defer conn.Close()

	request := Request{
		Operation: "GET",
		Key:       key,
	}

	requestData, err := json.Marshal(request)
	if err != nil {
		return "", false, err
	}

	_, err = conn.Write(requestData)
	if err != nil {
		return "", false, err
	}

	buffer := make([]byte, 1024)

	bytesRead, err := conn.Read(buffer)
	if err != nil {
		return "", false, err
	}

	var response Response

	err = json.Unmarshal(buffer[:bytesRead], &response)
	if err != nil {
		return "", false, err
	}

	if response.Error != "" {
		return "", false, nil
	}

	return response.Value, response.Found, nil
}

// Put stores a key-value pair on the node.
func (c *Client) Put(key string, value string) error {
	conn, err := net.Dial("tcp", c.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := Request{
		Operation: "PUT",
		Key:       key,
		Value:     value,
	}

	requestData, err := json.Marshal(request)
	if err != nil {
		return err
	}

	_, err = conn.Write(requestData)
	if err != nil {
		return err
	}

	buffer := make([]byte, 1024)

	bytesRead, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	var response Response

	err = json.Unmarshal(buffer[:bytesRead], &response)
	if err != nil {
		return err
	}

	if response.Error != "" {
		return nil
	}

	return nil
}

// Delete removes a key-value pair from the node.
func (c *Client) Delete(key string) error {
	conn, err := net.Dial("tcp", c.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	request := Request{
		Operation: "DELETE",
		Key:       key,
	}

	requestData, err := json.Marshal(request)
	if err != nil {
		return err
	}

	_, err = conn.Write(requestData)
	if err != nil {
		return err
	}

	buffer := make([]byte, 1024)

	bytesRead, err := conn.Read(buffer)
	if err != nil {
		return err
	}

	var response Response

	err = json.Unmarshal(buffer[:bytesRead], &response)
	if err != nil {
		return err
	}

	if response.Error != "" {
		return nil
	}

	return nil
}

// RequestVote asks another node to vote for this candidate.
func (c *Client) RequestVote(term int, candidateID string) (int, bool, error) {
	conn, err := net.Dial("tcp", c.Address)
	if err != nil {
		return 0, false, err
	}
	defer conn.Close()

	request := Request{
		Operation:   "REQUEST_VOTE",
		Term:        term,
		CandidateID: candidateID,
	}

	requestData, err := json.Marshal(request)
	if err != nil {
		return 0, false, err
	}

	_, err = conn.Write(requestData)
	if err != nil {
		return 0, false, err
	}

	buffer := make([]byte, 1024)

	bytesRead, err := conn.Read(buffer)
	if err != nil {
		return 0, false, err
	}

	var response Response

	err = json.Unmarshal(buffer[:bytesRead], &response)
	if err != nil {
		return 0, false, err
	}

	return response.Term, response.VoteGranted, nil
}
