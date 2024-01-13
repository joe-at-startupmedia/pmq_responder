package main

import (
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder"
	"github.com/joe-at-startupmedia/posix_mq"
	"log"
	"time"
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
		Name:  "posix_mq_example_duplex",
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT,
	}, nil)

	if err != nil {
		log.Printf("Responder: could not initialize: %s", err)
		c <- 1
	}
	defer func() {
		mqr.UnlinkResponder()
		fmt.Println("Responder: finished and unlinked")
		c <- 0
	}()

	count := 0
	for {
		time.Sleep(1 * time.Second)
		count++
		if err := mqr.HandleRequestProto(handleMessage); err != nil {
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
		Name: "posix_mq_example_duplex",
	}, nil)

	if err != nil {
		log.Printf("Requester: could not initialize: %s", err)
		c <- 1
	}
	defer func() {
		mqs.CloseRequester()
		fmt.Println("Requester: finished and closed")
		c <- 0
	}()

	count := 0
	for {
		count++
		request := fmt.Sprintf("Hello, World : %d\n", count)
		if err := mqs.RequestProto(&pmq_responder.MqRequest{
			Arg1: request,
		}, 0); err != nil {
			fmt.Printf("Requester: error requesting request: %s\n", err)
			continue
		}

		fmt.Printf("Requester: sent a new request: %s", request)

		msg, _, err := mqs.WaitForResponseProto(time.Second)

		if err != nil {
			fmt.Printf("Requester: error getting response: %s\n", err)
			continue
		}

		fmt.Printf("Requester: got a response: %s\n", msg.ValueStr)

		if count >= maxRequestTickNum {
			break
		}

		time.Sleep(1 * time.Second)
	}
}

func handleMessage(request *pmq_responder.MqRequest) (*pmq_responder.MqResponse, error) {
	response := pmq_responder.MqResponse{}
	response.PrepareFromRequest(request)
	response.ValueStr = fmt.Sprintf("I recieved request: %s\n", request.Arg1)
	return &response, nil
}