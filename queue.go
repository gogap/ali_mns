package ali_mns

import (
	"fmt"
	"os"
	"strings"
)

var (
	DefaultNumOfMessages int32 = 16
)

const (
	PROXY_PREFIX = "MNS_PROXY_"
	GLOBAL_PROXY = "MNS_GLOBAL_PROXY"
)

type AliMNSQueue interface {
	Name() string
	SendMessage(message MessageSendRequest) (resp MessageSendResponse, err error)
	BatchSendMessage(messages ...MessageSendRequest) (resp BatchMessageSendResponse, err error)
	ReceiveMessage(respChan chan MessageReceiveResponse, errChan chan error, waitseconds ...int64)
	BatchReceiveMessage(respChan chan BatchMessageReceiveResponse, errChan chan error, numOfMessages int32, waitseconds ...int64)
	PeekMessage(respChan chan MessageReceiveResponse, errChan chan error)
	BatchPeekMessage(respChan chan BatchMessageReceiveResponse, errChan chan error, numOfMessages int32)
	DeleteMessage(receiptHandle string) (err error)
	BatchDeleteMessage(receiptHandles ...string) (err error)
	ChangeMessageVisibility(receiptHandle string, visibilityTimeout int64) (resp MessageVisibilityChangeResponse, err error)
	Stop()
}

type MNSQueue struct {
	name     string
	client   MNSClient
	stopChan chan bool
}

func NewMNSQueue(name string, client MNSClient) AliMNSQueue {
	if name == "" {
		panic("ali_mns: queue name could not be empty")
	}

	queue := new(MNSQueue)
	queue.client = client
	queue.name = name
	queue.stopChan = make(chan bool)

	proxyURL := ""
	queueProxyEnvKey := PROXY_PREFIX + strings.Replace(strings.ToUpper(name), "-", "_", -1)
	if url := os.Getenv(queueProxyEnvKey); url != "" {
		proxyURL = url
	} else if globalurl := os.Getenv(GLOBAL_PROXY); globalurl != "" {
		proxyURL = globalurl
	}

	if proxyURL != "" {
		queue.client.SetProxy(proxyURL)
	}

	return queue
}

func (p *MNSQueue) Name() string {
	return p.name
}

func (p *MNSQueue) SendMessage(message MessageSendRequest) (resp MessageSendResponse, err error) {
	_, err = p.client.Send(_POST, nil, message, fmt.Sprintf("queues/%s/%s", p.name, "messages"), &resp)
	return
}

func (p *MNSQueue) BatchSendMessage(messages ...MessageSendRequest) (resp BatchMessageSendResponse, err error) {
	if messages == nil || len(messages) == 0 {
		return
	}

	batchRequest := BatchMessageSendRequest{}
	for _, message := range messages {
		batchRequest.Messages = append(batchRequest.Messages, message)
	}

	_, err = p.client.Send(_POST, nil, batchRequest, fmt.Sprintf("queues/%s/%s", p.name, "messages"), &resp)
	return
}

func (p *MNSQueue) Stop() {
	p.stopChan <- true
}

func (p *MNSQueue) ReceiveMessage(respChan chan MessageReceiveResponse, errChan chan error, waitseconds ...int64) {
	resource := fmt.Sprintf("queues/%s/%s", p.name, "messages")
	if waitseconds != nil && len(waitseconds) == 1 {
		resource = fmt.Sprintf("queues/%s/%s?waitseconds=%d", p.name, "messages", waitseconds[0])
	}

	for {
		resp := MessageReceiveResponse{}
		_, err := p.client.Send(_GET, nil, nil, resource, &resp)
		if err != nil {
			errChan <- err
		} else {
			respChan <- resp
		}

		select {
		case _ = <-p.stopChan:
			{
				return
			}
		default:
		}
	}

	return
}

func (p *MNSQueue) BatchReceiveMessage(respChan chan BatchMessageReceiveResponse, errChan chan error, numOfMessages int32, waitseconds ...int64) {
	if numOfMessages <= 0 {
		numOfMessages = DefaultNumOfMessages
	}

	resource := fmt.Sprintf("queues/%s/%s?numOfMessages=%d", p.name, "messages", numOfMessages)
	if waitseconds != nil && len(waitseconds) == 1 {
		resource = fmt.Sprintf("queues/%s/%s?numOfMessages=%d&waitseconds=%d", p.name, "messages", numOfMessages, waitseconds[0])
	}

	for {
		resp := BatchMessageReceiveResponse{}
		_, err := p.client.Send(_GET, nil, nil, resource, &resp)
		if err != nil {
			errChan <- err
		} else {
			respChan <- resp
		}

		select {
		case _ = <-p.stopChan:
			{
				return
			}
		default:
		}
	}

	return
}

func (p *MNSQueue) PeekMessage(respChan chan MessageReceiveResponse, errChan chan error) {
	for {
		resp := MessageReceiveResponse{}
		_, err := p.client.Send(_GET, nil, nil, fmt.Sprintf("queues/%s/%s?peekonly=true", p.name, "messages"), &resp)
		if err != nil {
			errChan <- err
		} else {
			respChan <- resp
		}
	}
	return
}

func (p *MNSQueue) BatchPeekMessage(respChan chan BatchMessageReceiveResponse, errChan chan error, numOfMessages int32) {
	if numOfMessages <= 0 {
		numOfMessages = DefaultNumOfMessages
	}

	for {
		resp := BatchMessageReceiveResponse{}
		_, err := p.client.Send(_GET, nil, nil, fmt.Sprintf("queues/%s/%s?numOfMessages=%d&peekonly=true", p.name, "messages", numOfMessages), &resp)
		if err != nil {
			errChan <- err
		} else {
			respChan <- resp
		}
	}
	return
}

func (p *MNSQueue) DeleteMessage(receiptHandle string) (err error) {
	_, err = p.client.Send(_DELETE, nil, nil, fmt.Sprintf("queues/%s/%s?ReceiptHandle=%s", p.name, "messages", receiptHandle), nil)
	return
}

func (p *MNSQueue) BatchDeleteMessage(receiptHandles ...string) (err error) {
	if receiptHandles == nil || len(receiptHandles) == 0 {
		return
	}

	handlers := ReceiptHandles{}

	for _, handler := range receiptHandles {
		handlers.ReceiptHandles = append(handlers.ReceiptHandles, handler)
	}

	_, err = p.client.Send(_DELETE, nil, handlers, fmt.Sprintf("queues/%s/%s", p.name, "messages"), nil)
	return
}

func (p *MNSQueue) ChangeMessageVisibility(receiptHandle string, visibilityTimeout int64) (resp MessageVisibilityChangeResponse, err error) {
	_, err = p.client.Send(_PUT, nil, nil, fmt.Sprintf("queues/%s/%s?ReceiptHandle=%s&VisibilityTimeout=%d", p.name, "messages", receiptHandle, visibilityTimeout), &resp)
	return
}
