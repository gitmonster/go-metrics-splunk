package splunk

import (
	"bytes"
	"encoding/json"
	"net"
	"time"

	"github.com/juju/errors"
)

type SplunkClient struct {
	address    *net.TCPAddr
	connection *net.TCPConn
	queue      chan SplunkMessage
	done       chan bool
}

type SplunkMessage struct {
	Time        time.Time              `json:"time"`
	Measurement string                 `json:"measurement"`
	Fields      map[string]interface{} `json:"fields"`
}

func NewSplunkClient(addrStr string) (*SplunkClient, error) {
	if len(addrStr) == 0 {
		return nil, errors.New("address missing")
	}

	address, err := net.ResolveTCPAddr("tcp", addrStr)
	if err != nil {
		return nil, errors.Annotate(err, "resolve address")
	}

	queue := make(chan SplunkMessage, 1024)
	done := make(chan bool, 1)

	client := &SplunkClient{
		address: address,
		queue:   queue,
		done:    done,
	}

	if err = client.connect(); err != nil {
		return nil, errors.Annotate(err, "connect")
	}

	go client.writer()
	return client, nil
}

func (p *SplunkClient) connect() error {
	connection, err := net.DialTCP("tcp", nil, p.address)
	if err != nil {
		return errors.Annotate(err, "dialtcp")
	}

	if err = connection.SetKeepAlive(true); err != nil {
		return errors.Annotate(err, "set keep alive")
	}

	p.connection = connection
	return nil
}

func (p *SplunkClient) disconnect() error {
	if p.connection == nil {
		return nil
	}

	return p.connection.Close()
}

func (p *SplunkClient) reconnectLoop() {
	p.disconnect()

	var err error

	for {
		select {
		case <-p.done:
			break
		default:
		}

		err = p.connect()
		if err == nil {
			break
		}

		logger.Errorf("reconnect failed: %s", err)
		time.Sleep(1 * time.Second)
	}
}

func (p *SplunkClient) writeData(b []byte) {
	for {
		bytesWritten, err := p.connection.Write(b)
		if err != nil {
			logger.Errorf("failed to write message: %s", err)
			p.reconnectLoop()
			return
		}

		b = b[bytesWritten:]
		if len(b) == 0 {
			break
		}
	}
}

func (p *SplunkClient) writer() {
	for msg := range p.queue {
		buf, err := p.buildMessage(msg)
		if err != nil {
			logger.Error(errors.Annotate(err, "build message"))
			return
		}
		p.writeData(buf.Bytes())
	}
}

func (p *SplunkClient) buildMessage(m SplunkMessage) (*bytes.Buffer, error) {
	data, err := json.Marshal(&m)
	if err != nil {
		return nil, errors.Annotate(err, "marshal")
	}

	buf := bytes.NewBuffer(data)
	buf.WriteString("\n")
	return buf, nil

}

func (p *SplunkClient) Stream(stream chan SplunkMessage) {
	for message := range stream {
		select {
		case p.queue <- message:
		default:
			logger.Warning("channel is full. dropping messages")
			continue
		}
	}

	p.Close()
}

func (p *SplunkClient) Close() {
	close(p.queue)
	p.disconnect()
	p.done <- true
}
