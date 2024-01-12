package main

import (
	"fmt"
	"log"
	"pmq_responder"
	"time"

	"github.com/joe-at-startupmedia/posix_mq"
)

const maxSendTickNum = 10

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	time.Sleep(1 * time.Second)
	send_c := make(chan int)
	go sender(send_c)
	<-resp_c
	<-send_c
}

func responder(c chan int) {
	mqr, err := pmq_responder.NewResponder(pmq_responder.QueueConfig{
		Name:  "posix_mq_example_duplex_lag",
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT,
	}, nil)

	if err != nil {
		log.Printf("Responder: could not initialize: %s", err)
		c <- 1
	}
	defer func() {
		(*pmq_responder.BidirectionalQueue)(mqr).Close()
		fmt.Println("Responder: finished and unlinked")
		c <- 0
	}()

	count := 0
	for {
		time.Sleep(1 * time.Second)
		count++
		var err error
		if count > 5 {
			err = mqr.HandleRequestWithLag(handleMessage, count-4)
		} else {
			err = mqr.HandleRequest(handleMessage)
		}

		if err != nil {
			fmt.Printf("Responder: error handling request: %s\n", err)
			continue
		}

		fmt.Println("Responder: Sent a response")

		if count >= maxSendTickNum {
			break
		}
	}
}

func sender(c chan int) {
	mqs, err := pmq_responder.NewSender(pmq_responder.QueueConfig{
		Name: "posix_mq_example_duplex_lag",
	}, nil)
	if err != nil {
		log.Printf("Sender: could not initialize: %s", err)
		c <- 1
	}
	defer func() {
		(*pmq_responder.BidirectionalQueue)(mqs).Close()
		fmt.Println("Sender: finished and closed")
		c <- 0
	}()
	count := 0
	ch := make(chan pmqResponse)
	for {
		count++
		request := fmt.Sprintf("Hello, World : %d\n", count)
		go requestResponse(mqs, request, ch)

		if count >= maxSendTickNum {
			break
		}

		time.Sleep(1 * time.Second)
	}

	result := make([]pmqResponse, maxSendTickNum)
	for i := range result {
		result[i] = <-ch
		if result[i].status {
			fmt.Println(result[i].response)
		} else {
			fmt.Printf("Sender: Got error: %s \n", result[i].response)
		}
	}
}

func requestResponse(mqs *pmq_responder.MqSender, msg string, c chan pmqResponse) {
	if err := mqs.Send([]byte(msg), 0); err != nil {
		c <- pmqResponse{fmt.Sprintf("%s", err), false}
		return
	}
	fmt.Printf("Sender: sent a new request: %s", msg)

	resp, _, err := mqs.WaitForResponse(time.Second)

	if err != nil {
		c <- pmqResponse{fmt.Sprintf("%s", err), false}
		return
	}

	c <- pmqResponse{fmt.Sprintf("Sender: got a response: %s\n", resp), true}
}

type pmqResponse struct {
	response string
	status   bool
}

func handleMessage(request []byte) (processed []byte, err error) {
	return []byte(fmt.Sprintf("I recieved request: %s\n", request)), nil
}
