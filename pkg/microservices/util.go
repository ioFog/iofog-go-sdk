/*
 *******************************************************************************
 * Copyright (c) 2018 Edgeworx, Inc.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v. 2.0 which is available at
 * http://www.eclipse.org/legal/epl-2.0
 *
 * SPDX-License-Identifier: EPL-2.0
 *******************************************************************************
 */

package microservices

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"time"

	ws "github.com/gorilla/websocket"
)

func intToBytesBE(num int) ([]byte, int) {
	if num == 0 {
		return []byte{0}, 1
	}
	numOfBits := int(math.Log2(float64(num))) + 1
	numOfBytes := int(math.Ceil(float64(numOfBits) / 8.0))
	b := make([]byte, numOfBytes)
	shift := uint(8 * (numOfBytes - 1))
	for i := 0; i < numOfBytes; i++ {
		b[i] = byte(num >> shift)
		shift -= 8
	}
	return b, numOfBytes

}

func int64ToBytesBE(num int64) ([]byte, int) {
	if num == 0 {
		return []byte{0}, 1
	}
	numOfBits := int(math.Log2(float64(num))) + 1
	numOfBytes := int(math.Ceil(float64(numOfBits) / 8.0))
	b := make([]byte, numOfBytes)
	shift := uint(8 * (numOfBytes - 1))
	for i := 0; i < numOfBytes; i++ {
		b[i] = byte(num >> shift)
		shift -= 8
	}
	return b, numOfBytes
}

func setCustomPingHandler(conn *ws.Conn) {
	conn.SetPingHandler(func(message string) error {
		if message == fmt.Sprint(ws.PingMessage) {
			message = fmt.Sprint(ws.PongMessage)
		}
		err := conn.WriteControl(ws.PongMessage, []byte(message), time.Now().Add(time.Second))
		if err == ws.ErrCloseSent {
			return nil
		} else if e, ok := err.(net.Error); ok && e.Temporary() {
			return nil
		}
		return err
	})
}

func makePostRequest(url, bodyType string, body io.Reader) ([]byte, error) {
	resp, err := http.Post(url, bodyType, body)
	if err != nil {
		return nil, err
	}
	respBodyBytes := make([]byte, resp.ContentLength)
	resp.Body.Read(respBodyBytes)
	resp.Body.Close()
	if resp.StatusCode == http.StatusBadRequest {
		return nil, errors.New(string(respBodyBytes))
	}
	return respBodyBytes, nil
}

func PrepareMessageForSendingViaSocket(msg *IoMessage) ([]byte, error) {
	msgBytes, err := msg.EncodeBinary()
	if err != nil {
		return nil, err
	}
	lengthBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lengthBytes, uint32(len(msgBytes)))
	bytesToSend := make([]byte, 0, len(msgBytes)+5)
	bytesToSend = append(bytesToSend, CODE_MSG)
	bytesToSend = append(bytesToSend, lengthBytes...)
	bytesToSend = append(bytesToSend, msgBytes...)
	return bytesToSend, nil
}

func GetMessageReceivedViaSocket(msgBytes []byte) (*IoMessage, error) {
	msgLen := binary.BigEndian.Uint32(msgBytes[1:5])
	if cap(msgBytes) < int(msgLen)+5 {
		return nil, errors.New("msg length is incorrect")
	}
	msg := new(IoMessage)
	err := msg.DecodeBinary(msgBytes[5 : 5+msgLen])
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func getReceiptReceivedViaSocket(receipt []byte) (*PostMessageResponse, error) {
	idLen := int(receipt[1])
	tsLen := int(receipt[2])
	receiptResponse := new(PostMessageResponse)
	dataPos := 3
	if idLen != 0 {
		receiptResponse.ID = string(receipt[dataPos : dataPos+idLen])
		dataPos += idLen
	}
	if tsLen != 0 {
		receiptResponse.Timestamp = int64(binary.BigEndian.Uint64(receipt[dataPos : dataPos+tsLen]))
	}
	return receiptResponse, nil
}
