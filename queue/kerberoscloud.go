package queue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"

	models "github.com/uug-ai/cli/models"
)

type KerberosCloudQueue struct {
	Url      string
	CloudKey string
}

func CreateKerberosCloudQueue(connection models.Queue) (*KerberosCloudQueue, error) {
	return &KerberosCloudQueue{
		Url:      connection.Url,
		CloudKey: connection.CloudKey,
	}, nil
}

func (q KerberosCloudQueue) ReadMessagesForever(handleMessage func(Payload) (string, int), sync ...bool) {

}

func (q KerberosCloudQueue) ReadEventsForever() {

}

func (q KerberosCloudQueue) SendMessage(queueName string, payload string, delay int) {

	url := q.Url + "/queue"
	fmt.Println("Sending message to (Kerberos Hub) " + url + ".")
	fmt.Println(q)

	// if on demand is enabled.
	// event.Events = []string{"monitor", "sequence", ....., "throttler", "notification"}
	// event.Payload: 1,

	var jsonStr = []byte(`{"message": ` + payload + `}`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Kerberos-Cloud-Key", q.CloudKey)
	//req.Header.Set("X-Kerberos-Storage-License", licenseId)
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println("Something went wrong while sending to Kerberos Hub Queue: " + err.Error())
	}

	if resp.Body != nil {
		defer resp.Body.Close()
		bodyBytes, err := ioutil.ReadAll(resp.Body)
		bodyString := string(bodyBytes)

		if resp.StatusCode == 200 && err == nil {
			fmt.Println("Message send to Kerberos Hub Queue.")
		} else {
			fmt.Println("Wasn't able to reach Kerberos Hub queue: " + bodyString)
		}
	} else {
		fmt.Println("Wasn't able to reach Kerberos Hub queue: " + strconv.Itoa(resp.StatusCode))

	}
}

func (q KerberosCloudQueue) DeleteMessage(queueUrl *string, receiptHandle *string) {

}

func (q KerberosCloudQueue) SendMessageFifo(queueName string, bytes []byte, deduplicationId string) {

}

func (q KerberosCloudQueue) Close() {

}

func (q KerberosCloudQueue) Test(queueName string) error {
	url := q.Url + "/queue/test"

	// This is a test (Fake) event
	var testEvent Payload
	testEvent.Events = []string{"monitor", "sequence"}
	testEvent.Provider = "kstorage"
	testEvent.Payload = Media{}
	payload, err := json.Marshal(testEvent)

	var jsonStr = []byte(`{"message": ` + string(payload) + `}`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Kerberos-Cloud-Key", q.CloudKey)
	//req.Header.Set("X-Kerberos-Storage-License", licenseId)
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println("Something went wrong while sending to Kerberos Hub Queue: " + err.Error())
		return errors.New("Something went wrong while sending to Kerberos Hub Queue: " + err.Error())
	}
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	bodyString := string(bodyBytes)
	if resp.StatusCode == 200 && err == nil {
		fmt.Println("Message send to Kerberos Hub Queue.")
		return nil
	} else {
		fmt.Println("Wasn't able to reach Kerberos Hub queue: " + bodyString)
		return errors.New("Wasn't able to reach Kerberos Hub queue: " + bodyString)
	}
}
