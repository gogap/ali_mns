package main

import (
	"encoding/json"
	"io/ioutil"
	"time"

	"github.com/gogap/ali_mns"
	"github.com/gogap/logs"
)

type appConf struct {
	Url             string `json:"url"`
	Queue           string `json:"queue"`
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	Delete          bool   `json:"delete"`
}

func main() {
	conf := appConf{}

	if bFile, e := ioutil.ReadFile("app.conf"); e != nil {
		panic(e)
	} else {
		if e := json.Unmarshal(bFile, &conf); e != nil {
			panic(e)
		}
	}

	client := ali_mns.NewAliMNSClient(conf.Url,
		conf.AccessKeyId,
		conf.AccessKeySecret)

	queue := ali_mns.NewMNSQueue(conf.Queue, client)

	respChan := make(chan ali_mns.MessageReceiveResponse)
	errChan := make(chan error)
	go func() {
		for {
			select {
			case resp := <-respChan:
				{
					logs.Pretty("message:", string(resp.MessageBody))
					if conf.Delete {
						if e := queue.DeleteMessage(resp.ReceiptHandle); e != nil {
							logs.Error(e)
						}
					}
				}
			case err := <-errChan:
				{
					logs.Error(err)
				}
			}
		}

	}()

	queue.ReceiveMessage(respChan, errChan)
	for {
		time.Sleep(time.Second * 1)
	}

}
