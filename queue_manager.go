package ali_mns

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gogap/errors"
)

type MNSLocation string

const (
	Beijing   MNSLocation = "cn-beijing"
	Hangzhou  MNSLocation = "cn-hangzhou"
	Qingdao   MNSLocation = "cn-qingdao"
	Singapore MNSLocation = "ap-southeast-1"
)

type AliQueueManager interface {
	CreateQueue(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error)
	SetQueueAttributes(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error)
	GetQueueAttributes(location MNSLocation, queueName string) (attr QueueAttribute, err error)
	DeleteQueue(location MNSLocation, queueName string) (err error)
	ListQueue(location MNSLocation, nextMarker Base64Bytes, retNumber int32, prefix string) (queues Queues, err error)
}

type MNSQueueManager struct {
	ownerId         string
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

func NewMNSQueueManager(ownerId, accessKeyId, accessKeySecret string) AliQueueManager {
	return &MNSQueueManager{
		ownerId:         ownerId,
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

func (p *MNSQueueManager) CreateQueue(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error) {
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

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	var code int
	code, err = send(cli, p.decoder, PUT, nil, &message, "queues/"+queueName, nil)

	if code == http.StatusNoContent {
		err = ERR_MNS_QUEUE_ALREADY_EXIST_AND_HAVE_SAME_ATTR.New(errors.Params{"name": queueName})
		return
	}

	return
}

func (p *MNSQueueManager) SetQueueAttributes(location MNSLocation, queueName string, delaySeconds int32, maxMessageSize int32, messageRetentionPeriod int32, visibilityTimeout int32, pollingWaitSeconds int32) (err error) {
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

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	_, err = send(cli, p.decoder, PUT, nil, &message, fmt.Sprintf("queues/%s?metaoverride=true", queueName), nil)
	return
}

func (p *MNSQueueManager) GetQueueAttributes(location MNSLocation, queueName string) (attr QueueAttribute, err error) {
	queueName = strings.TrimSpace(queueName)

	if err = checkQueueName(queueName); err != nil {
		return
	}

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	_, err = send(cli, p.decoder, GET, nil, nil, "queues/"+queueName, &attr)

	return
}

func (p *MNSQueueManager) DeleteQueue(location MNSLocation, queueName string) (err error) {
	queueName = strings.TrimSpace(queueName)

	if err = checkQueueName(queueName); err != nil {
		return
	}

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	_, err = send(cli, p.decoder, DELETE, nil, nil, "queues/"+queueName, nil)

	return
}

func (p *MNSQueueManager) ListQueue(location MNSLocation, nextMarker Base64Bytes, retNumber int32, prefix string) (queues Queues, err error) {

	url := fmt.Sprintf("http://%s.mns.%s.aliyuncs.com", p.ownerId, string(location))

	cli := NewAliMNSClient(url, p.accessKeyId, p.accessKeySecret)

	header := map[string]string{}

	marker := ""
	if nextMarker != nil && len(nextMarker) > 0 {
		marker = strings.TrimSpace(string(nextMarker))
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
