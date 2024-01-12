package main

import (
	"fmt"
	"github.com/joe-at-startupmedia/pmq_responder"
	"github.com/joe-at-startupmedia/posix_mq"
	"log"
	"time"
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
		Name:  "posix_mq_example_duplex",
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
		if err := mqr.HandleRequest(handleMessage); err != nil {
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
		Name: "posix_mq_example_duplex",
	}, nil)

	if err != nil {
		log.Printf("Sender: could not initialize: %s", err)
		c <- 1
	}
	defer func() {
		mqs.CloseSender()
		fmt.Println("Sender: finished and closed")
		c <- 0
	}()

	count := 0
	for {
		count++
		request := fmt.Sprintf("Hello, World : %d\n", count)
		if err := mqs.Send([]byte(request), 0); err != nil {
			fmt.Printf("Sender: error sending request: %s\n", err)
			continue
		}

		fmt.Printf("Sender: sent a new request: %s", request)

		msg, _, err := mqs.WaitForResponse(time.Second)

		if err != nil {
			fmt.Printf("Sender: error getting response: %s\n", err)
			continue
		}

		fmt.Printf("Sender: got a response: %s\n", msg)

		if count >= maxSendTickNum {
			break
		}

		time.Sleep(1 * time.Second)
	}
}

func handleMessage(request []byte) (processed []byte, err error) {
	return []byte(fmt.Sprintf("I recieved request: %s\n", request)), nil
}
