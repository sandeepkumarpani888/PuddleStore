package chord

import (
	"fmt"
	"testing"
	"time"
)

func TestSimple(t *testing.T) {
	_, err := CreateNode(nil)
	if err != nil {
		t.Errorf("Unable to create node, received error:%v\n", err)
	}
}

func TestTwoNodes(t *testing.T) {
	firstNode, errFirst := CreateNode(nil)
	if errFirst != nil {
		t.Errorf("Unable to create node, error: %v\n", errFirst)
	}
	fmt.Print("Created first node\n")
	secondNode, errSecond := CreateNode(firstNode.RemoteSelf)
	fmt.Print("Created second node\n")
	if errSecond != nil {
		t.Errorf("Unable to create node, error: %v\n", errSecond)
	}
	time.Sleep(5 * time.Second)
	PrintFingerTable(firstNode)
	PrintFingerTable(secondNode)
}
