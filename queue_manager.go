package ali_mns

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gogap/errors"
)

type AliQueueManager interface {
	CreateQueue(endpoint string, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error)
	SetQueueAttributes(endpoint string, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error)
	GetQueueAttributes(endpoint string, queueName string) (attr QueueAttribute, err error)
	DeleteQueue(endpoint string, queueName string) (err error)
	ListQueue(endpoint string, nextMarker string, retNumber int32, prefix string) (queues Queues, err error)
}

type MNSQueueManager struct {
	credential      Credential
	accessKeyId     string
	accessKeySecret string

	decoder MNSDecoder
}

func checkQueueName(queueName string) (err error) {
	if len(queueName) > 256 {
		err = ERR_MNS_QUEUE_NAME_IS_TOO_LONG.New()
		return
	}
	return
}

func checkDelaySeconds(seconds int32) (err error) {
	if seconds > 60480 || seconds < 0 {
		err = ERR_MNS_DELAY_SECONDS_RANGE_ERROR.New()
		return
	}
	return
}

func checkMaxMessageSize(maxSize int32) (err error) {
	if maxSize < 1024 || maxSize > 65536 {
		err = ERR_MNS_MAX_MESSAGE_SIZE_RANGE_ERROR.New()
		return
	}
	return
}

func checkMessageRetentionPeriod(retentionPeriod int32) (err error) {
	if retentionPeriod < 60 || retentionPeriod > 1296000 {
		err = ERR_MNS_MSG_RETENTION_PERIOD_RANGE_ERROR.New()
		return
	}
	return
}

func checkVisibilityTimeout(visibilityTimeout int32) (err error) {
	if visibilityTimeout < 1 || visibilityTimeout > 43200 {
		err = ERR_MNS_MSG_VISIBILITY_TIMEOUT_RANGE_ERROR.New()
		return
	}
	return
}

func checkPollingWaitSeconds(pollingWaitSeconds int32) (err error) {
	if pollingWaitSeconds < 0 || pollingWaitSeconds > 30 {
		err = ERR_MNS_MSG_POOLLING_WAIT_SECONDS_RANGE_ERROR.New()
		return
	}
	return
}

func NewMNSQueueManager(accessKeyId, accessKeySecret string) AliQueueManager {
	return &MNSQueueManager{
		accessKeyId:     accessKeyId,
		accessKeySecret: accessKeySecret,
		decoder:         new(AliMNSDecoder),
	}
}

func checkAttributes(delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error) {
	if err = checkDelaySeconds(delaySeconds); err != nil {
		return
	}
	if err = checkMaxMessageSize(maxMessageSize); err != nil {
		return
	}
	if err = checkMessageRetentionPeriod(messageRetentionPeriod); err != nil {
		return
	}
	if err = checkVisibilityTimeout(visibilityTimeout); err != nil {
		return
	}
	if err = checkPollingWaitSeconds(pollingWaitSeconds); err != nil {
		return
	}
	return
}

func (p *MNSQueueManager) CreateQueue(endpoint string, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error) {
	queueName = strings.TrimSpace(queueName)

	if err = checkQueueName(queueName); err != nil {
		return
	}

	if err = checkAttributes(delaySeconds,
		maxMessageSize,
		messageRetentionPeriod,
		visibilityTimeout,
		pollingWaitSeconds); err != nil {
		return
	}

	message := CreateQueueRequest{
		DelaySeconds:           delaySeconds,
		MaxMessageSize:         maxMessageSize,
		MessageRetentionPeriod: messageRetentionPeriod,
		VisibilityTimeout:      visibilityTimeout,
		PollingWaitSeconds:     pollingWaitSeconds,
	}

	cli := NewAliMNSClient(endpoint, p.accessKeyId, p.accessKeySecret)

	var code int
	if code, err = send(cli, p.decoder, PUT, nil, &message, "queues/"+queueName, nil); err != nil {
		return
	}

	switch code {
	case http.StatusOK:
		return
	case http.StatusNoContent:
		{
			err = ERR_MNS_QUEUE_ALREADY_EXIST_AND_HAVE_SAME_ATTR.New(errors.Params{"name": queueName})
			return
		}
	case http.StatusConflict:
		{
			err = ERR_MNS_QUEUE_ALREADY_EXIST.New(errors.Params{"name": queueName})
			return
		}
	}

	return
}

func (p *MNSQueueManager) SetQueueAttributes(endpoint string, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error) {
	queueName = strings.TrimSpace(queueName)

	if err = checkQueueName(queueName); err != nil {
		return
	}

	if err = checkAttributes(delaySeconds,
		maxMessageSize,
		messageRetentionPeriod,
		visibilityTimeout,
		pollingWaitSeconds); err != nil {
		return
	}

	message := CreateQueueRequest{
		DelaySeconds:           delaySeconds,
		MaxMessageSize:         maxMessageSize,
		MessageRetentionPeriod: messageRetentionPeriod,
		VisibilityTimeout:      visibilityTimeout,
		PollingWaitSeconds:     pollingWaitSeconds,
	}

	cli := NewAliMNSClient(endpoint, p.accessKeyId, p.accessKeySecret)

	_, err = send(cli, p.decoder, PUT, nil, &message, fmt.Sprintf("queues/%s?metaoverride=true", queueName), nil)
	return
}

func (p *MNSQueueManager) GetQueueAttributes(endpoint string, queueName string) (attr QueueAttribute, err error) {
	queueName = strings.TrimSpace(queueName)

	if err = checkQueueName(queueName); err != nil {
		return
	}

	cli := NewAliMNSClient(endpoint, p.accessKeyId, p.accessKeySecret)

	_, err = send(cli, p.decoder, GET, nil, nil, "queues/"+queueName, &attr)

	return
}

func (p *MNSQueueManager) DeleteQueue(endpoint string, queueName string) (err error) {
	queueName = strings.TrimSpace(queueName)

	if err = checkQueueName(queueName); err != nil {
		return
	}

	cli := NewAliMNSClient(endpoint, p.accessKeyId, p.accessKeySecret)

	_, err = send(cli, p.decoder, DELETE, nil, nil, "queues/"+queueName, nil)

	return
}

func (p *MNSQueueManager) ListQueue(endpoint string, nextMarker string, retNumber int32, prefix string) (queues Queues, err error) {

	cli := NewAliMNSClient(endpoint, p.accessKeyId, p.accessKeySecret)

	header := map[string]string{}

	marker := strings.TrimSpace(nextMarker)
	if len(marker) > 0 {
		if marker != "" {
			header["x-mns-marker"] = marker
		}
	}

	if retNumber > 0 {
		if retNumber >= 1 && retNumber <= 1000 {
			header["x-mns-ret-number"] = strconv.Itoa(int(retNumber))
		} else {
			err = REE_MNS_GET_QUEUE_RET_NUMBER_RANGE_ERROR.New()
			return
		}
	}

	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		header["x-mns-prefix"] = prefix
	}

	_, err = send(cli, p.decoder, GET, header, nil, "queues", &queues)

	return
}
