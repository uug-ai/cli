package queue

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	models "github.com/uug-ai/cli/models"
)

type KerberosVault struct {
	VaultUrl       string
	Provider       string
	AccessKey      string
	Secret         string
	Url            string
	CloudKey       string
	CloudUsername  string
	PreviewEnabled string
	ForwardType    string
}

func CreateKerberosVault(connection models.Queue) (*KerberosVault, error) {
	return &KerberosVault{
		VaultUrl:       connection.VaultUrl,
		Provider:       connection.Provider,
		AccessKey:      connection.AccessKey,
		Secret:         connection.Secret,
		Url:            connection.Url,
		CloudKey:       connection.CloudKey,
		CloudUsername:  connection.Username,
		PreviewEnabled: connection.PreviewEnabled,
		ForwardType:    connection.ForwardType,
	}, nil
}

func (q KerberosVault) Test(queueName string) error {
	url := q.VaultUrl + "/ping"

	var jsonStr = []byte(`{"message": ` + "fake-message" + `}`)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Kerberos-Storage-AccessKey", q.AccessKey)
	req.Header.Set("X-Kerberos-Storage-SecretAccessKey", q.Secret)
	req.Header.Set("X-Kerberos-Storage-Provider", q.Provider)
	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		fmt.Println("Something went wrong while pinging Kerberos Vault: " + err.Error())
		return errors.New("Something went wrong while pinging Kerberos Vault: " + err.Error())
	}
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	bodyString := string(bodyBytes)
	if resp.StatusCode == 200 && err == nil {
		fmt.Println("Ping send to Vault.")
		// So we are able to ping the vault, now if ondemand is enabled, we also need to verify the
		// Kerberos hub connection

		if q.ForwardType == "ondemand" {
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

		return nil
	} else {
		fmt.Println("Wasn't able to reach Kerberos Vault: " + bodyString)
		return errors.New("Wasn't able to reach Kerberos Vault: " + bodyString)
	}
}

func (q KerberosVault) Close() {

}

func (q KerberosVault) ReadMessagesForever(handleMessage func(Payload) (string, int), sync ...bool) {

}

func (q KerberosVault) ReadEventsForever() {

}

func (q KerberosVault) SendMessage(queueName string, payload string, delay int) {

}

func (q KerberosVault) SendMessageFifo(queueName string, bytes []byte, deduplicationId string) {

}
