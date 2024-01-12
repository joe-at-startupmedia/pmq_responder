package main

import (
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder"
	"log"
	"time"

	"github.com/joe-at-startupmedia/posix_mq"
)

const maxRequestTickNum = 10

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	time.Sleep(1 * time.Second)
	request_c := make(chan int)
	go requester(request_c)
	<-resp_c
	<-request_c
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
		mqr.CloseResponder()
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

		if count >= maxRequestTickNum {
			break
		}
	}
}

func requester(c chan int) {
	mqs, err := pmq_responder.NewRequester(pmq_responder.QueueConfig{
		Name: "posix_mq_example_duplex_lag",
	}, nil)
	if err != nil {
		log.Printf("Requester: could not initialize: %s", err)
		c <- 1
	}
	defer func() {
		(*pmq_responder.BidirectionalQueue)(mqs).Close()
		fmt.Println("Requester: finished and closed")
		c <- 0
	}()
	count := 0
	ch := make(chan pmqResponse)
	for {
		count++
		request := fmt.Sprintf("Hello, World : %d\n", count)
		go requestResponse(mqs, request, ch)

		if count >= maxRequestTickNum {
			break
		}

		time.Sleep(1 * time.Second)
	}

	result := make([]pmqResponse, maxRequestTickNum)
	for i := range result {
		result[i] = <-ch
		if result[i].status {
			fmt.Println(result[i].response)
		} else {
			fmt.Printf("Requester: Got error: %s \n", result[i].response)
		}
	}
}

func requestResponse(mqs *pmq_responder.MqRequester, msg string, c chan pmqResponse) {
	if err := mqs.Request([]byte(msg), 0); err != nil {
		c <- pmqResponse{fmt.Sprintf("%s", err), false}
		return
	}
	fmt.Printf("Requester: sent a new request: %s", msg)

	resp, _, err := mqs.WaitForResponse(time.Second)

	if err != nil {
		c <- pmqResponse{fmt.Sprintf("%s", err), false}
		return
	}

	c <- pmqResponse{fmt.Sprintf("Requester: got a response: %s\n", resp), true}
}

type pmqResponse struct {
	response string
	status   bool
}

func handleMessage(request []byte) (processed []byte, err error) {
	return []byte(fmt.Sprintf("I recieved request: %s\n", request)), nil
}
