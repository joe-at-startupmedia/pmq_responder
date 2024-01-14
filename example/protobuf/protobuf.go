package main

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/joe-at-startupmedia/pmq_responder"
	"github.com/joe-at-startupmedia/pmq_responder/example/protobuf/protos"
	"github.com/joe-at-startupmedia/posix_mq"
	"google.golang.org/protobuf/proto"
	"log"
	"time"
)

const maxRequestTickNum = 10

const queue_name = "pmqr_example_protobuf"

var owner = pmq_responder.Ownership{
	//Username: "nobody", //uncomment to test ownership handling and errors
}

func main() {
	resp_c := make(chan int)
	go responder(resp_c)
	//wait for the responder to create the posix_mq files
	time.Sleep(1 * time.Second)
	request_c := make(chan int)
	go requester(request_c)
	<-resp_c
	<-request_c
	//gives time for deferred functions to complete
	time.Sleep(2 * time.Second)
}

func responder(c chan int) {
	config := pmq_responder.QueueConfig{
		Name:  queue_name,
		Flags: posix_mq.O_RDWR | posix_mq.O_CREAT,
	}
	mqr, err := pmq_responder.NewResponder(&config, &owner)
	defer func() {
		pmq_responder.UnlinkResponder(mqr)
		fmt.Println("Responder: finished and unlinked")
		c <- 0
	}()
	if err != nil {
		log.Printf("Responder: could not initialize: %s", err)
		c <- 1
		return
	}

	count := 0
	for {
		time.Sleep(1 * time.Second)
		count++
		if err := handleCmdRequest(mqr); err != nil {
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
	mqs, err := pmq_responder.NewRequester(&pmq_responder.QueueConfig{
		Name: queue_name,
	}, &owner)
	defer func() {
		pmq_responder.CloseRequester(mqs)
		fmt.Println("Requester: finished and closed")
		c <- 0
	}()
	if err != nil {
		log.Printf("Requester: could not initialize: %s", err)
		c <- 1
		return
	}

	count := 0
	for {
		count++
		cmd := &protos.Cmd{
			Name: "restart",
			Arg1: fmt.Sprintf("%d", count), //using count as the id of the process
			ExecFlags: &protos.ExecFlags{
				User: "nonroot",
			},
		}
		if err := requestUsingCmd(mqs, cmd, 0); err != nil {
			fmt.Printf("Requester: error requesting request: %s\n", err)
			continue
		}

		fmt.Printf("Requester: sent a new request: %s \n", cmd.String())

		cmdResp, _, err := waitForCmdResponse(mqs, time.Second)

		if err != nil {
			fmt.Printf("Requester: error getting response: %s\n", err)
			continue
		}

		fmt.Printf("Requester: got a response: %s\n", cmdResp.ValueStr)
		//fmt.Printf("Requester: got a response: %-v\n", msg)

		if count >= maxRequestTickNum {
			break
		}

		time.Sleep(1 * time.Second)
	}
}

func requestUsingCmd(mqs *pmq_responder.MqRequester, req *protos.Cmd, priority uint) error {
	if len(req.Id) == 0 {
		req.Id = uuid.NewString()
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshaling error: %w", err)
	}
	return mqs.Request(data, priority)
}

func waitForCmdResponse(mqs *pmq_responder.MqRequester, duration time.Duration) (*protos.CmdResp, uint, error) {
	pbm, prio, err := mqs.WaitForProto(&protos.CmdResp{}, duration)
	mqResp, err := protoMessageToCmdResp(pbm)
	if err != nil {
		return nil, 0, err
	}
	return mqResp, prio, err
}

// protoMessageToCmdResp used to convert a generic protobuf message to a CmdResp
func protoMessageToCmdResp(pbm *proto.Message) (*protos.CmdResp, error) {
	msg, err := proto.Marshal(*pbm)
	if err != nil {
		return nil, fmt.Errorf("marshaling error: %w", err)
	}
	cmdResp := protos.CmdResp{}
	err = proto.Unmarshal(msg, &cmdResp)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling error: %w", err)
	}
	return &cmdResp, nil
}

// protoMessageToCmd used to convert a generic protobuf message to a Cmd protofbuf
func protoMessageToCmd(pbm *proto.Message) (*protos.Cmd, error) {
	msg, err := proto.Marshal(*pbm)
	if err != nil {
		return nil, fmt.Errorf("marshaling error: %w", err)
	}
	cmd := protos.Cmd{}
	err = proto.Unmarshal(msg, &cmd)
	if err != nil {
		return nil, fmt.Errorf("unmarshaling error: %w", err)
	}
	return &cmd, nil
}

// handleCmdRequest provides a concrete implementation of HandleRequestFromProto using the local Cmd protobuf type
func handleCmdRequest(mqr *pmq_responder.MqResponder) error {

	return mqr.HandleRequestFromProto(&protos.Cmd{}, func(pbm *proto.Message) (processed []byte, err error) {

		cmd, err := protoMessageToCmd(pbm)
		if err != nil {
			return nil, err
		}

		cmdResp := protos.CmdResp{}
		cmdResp.Id = cmd.Id
		cmdResp.ValueStr = fmt.Sprintf("I recieved request: %s(%s) - %s\n", cmd.Name, cmd.Id, cmd.Arg1)

		data, err := proto.Marshal(&cmdResp)
		if err != nil {
			return nil, fmt.Errorf("marshaling error: %w", err)
		}

		return data, nil
	})
}
