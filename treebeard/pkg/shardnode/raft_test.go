package shardnode

import (
	"sync"
	"testing"
	"time"
)

func TestHandleBatchReplicateRequestAndPathAndStorageToEmptyFSM(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	payload := BatchReplicateRequestAndPathAndStoragePayload{
		Requests: []ReplicateRequestAndPathAndStoragePayload{
			{RequestedBlock: "block1", Path: 1, StorageID: 2, RequestID: "request1"},
			{RequestedBlock: "block2", Path: 3, StorageID: 4, RequestID: "request2"},
		},
		LeaderID: 0,
	}
	isFirstMap := shardNodeFSM.handleBatchReplicateRequestAndPathAndStorage(payload)
	expectedIsFirstMap := map[string]bool{"request1": true, "request2": true}
	if len(isFirstMap) != len(expectedIsFirstMap) {
		t.Errorf("Expected isFirstMap to have length %d, but it has length %d", len(expectedIsFirstMap), len(isFirstMap))
	}
	for key, val := range isFirstMap {
		if val != expectedIsFirstMap[key] {
			t.Errorf("Expected isFirstMap[%s] to be %t, but it's %t", key, expectedIsFirstMap[key], val)
		}
	}
	if len(shardNodeFSM.requestLog["block1"]) != 1 || shardNodeFSM.requestLog["block1"][0] != "request1" {
		t.Errorf("Expected request1 to be in the requestLog, but the array is equal to %v", shardNodeFSM.requestLog["block1"])
	}
	if len(shardNodeFSM.requestLog["block2"]) != 1 || shardNodeFSM.requestLog["block2"][0] != "request2" {
		t.Errorf("Expected request2 to be in the requestLog, but the array is equal to %v", shardNodeFSM.requestLog["block2"])
	}
	if shardNodeFSM.pathMap["request1"] != 1 {
		t.Errorf("Expected path for request1 to be equal to 1, but the path is equal to %d", shardNodeFSM.pathMap["request1"])
	}
	if shardNodeFSM.pathMap["request2"] != 3 {
		t.Errorf("Expected path for request2 to be equal to 3, but the path is equal to %d", shardNodeFSM.pathMap["request2"])
	}
	if shardNodeFSM.storageIDMap["request1"] != 2 {
		t.Errorf("Expected storage id for request1 to be equal to 2, but the storage id is equal to %d", shardNodeFSM.storageIDMap["request1"])
	}
	if shardNodeFSM.storageIDMap["request2"] != 4 {
		t.Errorf("Expected storage id for request2 to be equal to 4, but the storage id is equal to %d", shardNodeFSM.storageIDMap["request2"])
	}
}

func TestHandleReplicateRequestAndPathAndStorageToWithValueFSM(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	shardNodeFSM.requestLog["block"] = []string{"randomrequest"}
	shardNodeFSM.pathMap["request1"] = 20
	shardNodeFSM.storageIDMap["request1"] = 30
	payload := BatchReplicateRequestAndPathAndStoragePayload{
		Requests: []ReplicateRequestAndPathAndStoragePayload{
			{RequestedBlock: "block", Path: 11, StorageID: 12, RequestID: "request1"},
			{RequestedBlock: "block", Path: 3, StorageID: 4, RequestID: "request2"},
		},
		LeaderID: 0,
	}
	isFirstMap := shardNodeFSM.handleBatchReplicateRequestAndPathAndStorage(payload)
	expectedIsFirstMap := map[string]bool{"request1": false, "request2": false}
	if len(isFirstMap) != len(expectedIsFirstMap) {
		t.Errorf("Expected isFirstMap to have length %d, but it has length %d", len(expectedIsFirstMap), len(isFirstMap))
	}
	for key, val := range isFirstMap {
		if val != expectedIsFirstMap[key] {
			t.Errorf("Expected isFirstMap[%s] to be %t, but it's %t", key, expectedIsFirstMap[key], val)
		}
	}
	if len(shardNodeFSM.requestLog["block"]) != 3 ||
		shardNodeFSM.requestLog["block"][0] != "randomrequest" ||
		shardNodeFSM.requestLog["block"][1] != "request1" ||
		shardNodeFSM.requestLog["block"][2] != "request2" {
		t.Errorf("Expected request1 and request2 to be in the requestLog, but the array is equal to %v", shardNodeFSM.requestLog["block"])
	}
	if shardNodeFSM.pathMap["request1"] != 11 {
		t.Errorf("Expected path for request1 to be equal to 11, but the path is equal to %d", shardNodeFSM.pathMap["request1"])
	}
	if shardNodeFSM.storageIDMap["request1"] != 12 {
		t.Errorf("Expected storage id for request1 to be equal to 12, but the storage id is equal to %d", shardNodeFSM.storageIDMap["request1"])
	}
	if shardNodeFSM.pathMap["request2"] != 3 {
		t.Errorf("Expected path for request2 to be equal to 3, but the path is equal to %d", shardNodeFSM.pathMap["request2"])
	}
	if shardNodeFSM.storageIDMap["request2"] != 4 {
		t.Errorf("Expected storage id for request2 to be equal to 4, but the storage id is equal to %d", shardNodeFSM.storageIDMap["request2"])
	}
}

func TestHandleReplicateRequestAndPathDoesntAddToRequestLogIfNotTheLeader(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	shardNodeFSM.pathMap["request1"] = 20
	shardNodeFSM.storageIDMap["request1"] = 30
	payload := BatchReplicateRequestAndPathAndStoragePayload{
		Requests: []ReplicateRequestAndPathAndStoragePayload{
			{RequestedBlock: "block", Path: 11, StorageID: 12, RequestID: "request1"},
			{RequestedBlock: "block", Path: 3, StorageID: 4, RequestID: "request2"},
		},
		LeaderID: 1,
	}
	shardNodeFSM.handleBatchReplicateRequestAndPathAndStorage(payload)
	if len(shardNodeFSM.requestLog["block"]) != 0 {
		t.Errorf("Expected requestLog to be empty, but it's equal to %v", shardNodeFSM.requestLog)
	}
}

func createTestReplicateResponsePayload(block string, requestID string, response string, value string, op OperationType, leaderID int) ReplicateResponsePayload {
	return ReplicateResponsePayload{
		RequestedBlock: block,
		RequestID:      requestID,
		Response:       response,
		NewValue:       value,
		OpType:         op,
		LeaderID:       leaderID,
	}
}

type responseMessage struct {
	requestID string
	response  string
}

// This helper function gets a map of requestID to its channel.
// It waits for all channels to recieve the determined response.
// It will timeout if at least one channel doesn't recieve the response.
func checkWaitingChannelsHelper(t *testing.T, waitChannels sync.Map, expectedResponse string) {
	waitingSet := make(map[string]bool) // keeps the request in the set until a response for request is recieved from channel
	agg := make(chan responseMessage)
	var keys []string
	waitChannels.Range(func(key any, value any) bool {
		keys = append(keys, key.(string))
		return true
	})
	for _, key := range keys {
		waitingSet[key] = true
		chAny, _ := waitChannels.Load(key)
		go func(requestID string, c chan string) {
			for msg := range c {
				agg <- responseMessage{requestID: requestID, response: msg}
			}
		}(key, chAny.(chan string))
	}
	for {
		if len(waitingSet) == 0 {
			return
		}
		select {
		case val := <-agg:
			if val.response != expectedResponse {
				t.Errorf("The channel for %s got the value %s while it should get %s", val.requestID, val.response, expectedResponse)
			}
			delete(waitingSet, val.requestID)
		case <-time.After(1 * time.Second):
			t.Errorf("timeout occured, failed to recieve the value on channels")
		}
	}
}

// In this case all the go routines should get the value that resides in stash.
// The stash value has priority over the response value.
func TestHandleReplicateResponseWhenValueInStashReturnsCorrectReadValueToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "request1", "response", "value", Read, 0)
	go shardNodeFSM.handleReplicateResponse(payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "test_value")
}

func TestHandleReplicateResponseWhenValueInStashReturnsCorrectWriteValueToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "request1", "response", "value_write", Write, 0)
	go shardNodeFSM.handleReplicateResponse(payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "value_write")

	if shardNodeFSM.stash["block"].value != "value_write" {
		t.Errorf("The stash value should be equal to \"value_write\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleReplicateResponseWhenValueNotInStashReturnsResponseToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))

	payload := createTestReplicateResponsePayload("block", "request1", "response_from_oramnode", "", Read, 0)
	go shardNodeFSM.handleReplicateResponse(payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "response_from_oramnode")

	if shardNodeFSM.stash["block"].value != "response_from_oramnode" {
		t.Errorf("The stash value should be equal to \"response_from_oramnode\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleReplicateResponseWhenValueNotInStashReturnsWriteResponseToAllWaitingRequests(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2", "request3"}
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.responseChannel.Store("request3", make(chan string))

	payload := createTestReplicateResponsePayload("block", "request1", "response", "write_val", Write, 0)
	go shardNodeFSM.handleReplicateResponse(payload)

	checkWaitingChannelsHelper(t, shardNodeFSM.responseChannel, "write_val")

	if shardNodeFSM.stash["block"].value != "write_val" {
		t.Errorf("The stash value should be equal to \"write_val\" after Write request, but it's equal to %s", shardNodeFSM.stash["block"].value)
	}
}

func TestHandleReplicateResponseWhenNotLeaderDoesNotWriteOnChannels(t *testing.T) {
	shardNodeFSM := newShardNodeFSM(0)
	shardNodeFSM.requestLog["block"] = []string{"request1", "request2"}
	shardNodeFSM.responseChannel.Store("request1", make(chan string))
	shardNodeFSM.responseChannel.Store("request2", make(chan string))
	shardNodeFSM.stash["block"] = stashState{value: "test_value"}

	payload := createTestReplicateResponsePayload("block", "request1", "response", "", Read, 1)
	go shardNodeFSM.handleReplicateResponse(payload)

	for {
		ch1Any, _ := shardNodeFSM.responseChannel.Load("request1")
		ch2Any, _ := shardNodeFSM.responseChannel.Load("request2")
		select {
		case <-ch1Any.(chan string):
			t.Errorf("The followers in the raft cluster should not send messages on channels!")
		case <-ch2Any.(chan string):
			t.Errorf("The followers in the raft cluster should not send messages on channels!")
		case <-time.After(1 * time.Second):
			return
		}
	}
}
