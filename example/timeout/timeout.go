package main

import (
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder"
	"log"
	"time"

	"github.com/joe-at-startupmedia/posix_mq"
)

const maxRequestTickNum = 10

const queue_name = "pmqr_example_timeout"

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	time.Sleep(1 * time.Second)
	requester(resp_c)
	//gives time for deferred functions to complete
	time.Sleep(2 * time.Second)
}

func responder(c chan int) {
	config := pmq_responder.QueueConfig{
		Name:  queue_name,
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT,
	}
	mqr := pmq_responder.NewResponder(&config, nil)
	defer func() {
		pmq_responder.UnlinkResponder(mqr)
		fmt.Println("Responder: finished and unlinked")
		c <- 0
	}()
	if mqr.HasErrors() {
		log.Fatalf("Responder: could not initialize: %s", mqr.Error())
	}

	count := 0
	for {
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

	mqs := pmq_responder.NewRequester(&pmq_responder.QueueConfig{
		Name: queue_name,
	}, nil)
	defer func() {
		pmq_responder.CloseRequester(mqs)
		fmt.Println("Requester: finished and closed")
	}()
	if mqs.HasErrors() {
		log.Fatalf("Requester: could not initialize: %s", mqs.Error())
	}

	count := 0
	ch := make(chan pmqResponse)
	for {
		count++
		request := fmt.Sprintf("Hello, World : %d\n", count)
		go requestResponse(mqs, request, ch)

		if count >= maxRequestTickNum {
			break
		}
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
	<-c
}

func requestResponse(mqs *pmq_responder.MqRequester, msg string, c chan pmqResponse) {
	if err := mqs.Request([]byte(msg)); err != nil {
		c <- pmqResponse{fmt.Sprintf("%s", err), false}
		return
	}
	fmt.Printf("Requester: sent a new request: %s", msg)

	resp, err := mqs.WaitForResponse(time.Second * 10)

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
