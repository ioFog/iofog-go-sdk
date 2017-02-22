package container_sdk_go

import (
	"os"
	"strconv"
	"os/exec"
)

type ioFogClient struct {
	id         string
	httpClient *ioFogHttpClient
	wsClient   *ioFogWsClient
}

func (client *ioFogClient) initClient(host string, port int, ssl bool) {
	client.httpClient = newIoFogHttpClient(client.id, ssl, host, port)
	client.wsClient = newIoFogWsClient(client.id, ssl, host, port)
}

func NewIoFogClient(id string, ssl bool, host string, port int) *ioFogClient {
	if id == "" {
		logger.Print("Id is empty. IoFog client is not created")
		return nil
	}
	client := ioFogClient{id: id}
	client.initClient(host, port, ssl)
	return &client
}

func NewDefaultIoFogClient() *ioFogClient {
	selfname := os.Getenv(SELFNAME)
	if selfname == "" {
		logger.Println("Empty ", SELFNAME, " environment virable. IoFog client is not created")
		return nil
	}
	ssl, err := strconv.ParseBool(os.Getenv(SSL))
	if err != nil {
		logger.Println("Empty or malformed ", SSL, " environment variable. Using default value of ", SSL_DEFAULT)
		ssl = SSL_DEFAULT
	}

	host := IOFOG
	if cmd := exec.Command("ping", "-c 3", host); cmd.Run() != nil {
		logger.Println("Host ", host, " is unreachable. Switching to ", HOST_DEFAULT)
		host = HOST_DEFAULT
	}

	client := ioFogClient{id: selfname}
	client.initClient(host, PORT_IOFOG, ssl)
	return &client
}

func (client *ioFogClient) GetConfig() (map[string]interface{}, error) {
	return client.httpClient.getConfig()
}

func (client *ioFogClient) GetNextMessages() ([]IoMessage, error) {
	return client.httpClient.getNextMessages()
}

func (client *ioFogClient) PostMessage(msg *IoMessage) (*PostMessageResponse, error) {
	msg.Publisher = client.id
	if msg.Version == 0 {
		msg.Version = IOMESSAGE_VERSION
	}
	return client.httpClient.postMessage(msg)
}

func (client *ioFogClient) GetMessagesFromPublishersWithinTimeFrame(query *MessagesQueryParameters) (*TimeFrameMessages, error) {
	query.ID = client.id
	return client.httpClient.getMessagesFromPublishersWithinTimeFrame(query)
}

func (client *ioFogClient) EstablishControlWsConnection(signalChannel chan <- int) {
	go client.wsClient.establishControlWsConnection(signalChannel)
}

func (client *ioFogClient) EstablishMessageWsConnection(messageChannel chan <- *IoMessage, receiptChannel chan <- *PostMessageResponse) {
	go client.wsClient.establishMessageWsConnection(messageChannel, receiptChannel)
}

func (client *ioFogClient) SendMessageViaSocket(msg *IoMessage) error {
	msg.ID = "";
	msg.Timestamp = 0
	if msg.Version == 0 {
		msg.Version = IOMESSAGE_VERSION
	}
	msg.Publisher = client.id
	return client.wsClient.sendMessage(msg)
}