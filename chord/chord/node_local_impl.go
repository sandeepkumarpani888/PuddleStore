/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Local Chord node functions to interact with the Chord ring.     */
/*                                                                           */

package chord

import (
	"fmt"
	"log"
	"time"
)

// This node is trying to join an existing ring that a remote node is a part of (i.e., other)
func (node *Node) join(other *RemoteNode) error {
	node.initFingerTable()
	node.Predecessor = nil
	node.Successor = node.RemoteSelf
	if other == nil {
		return nil
	}
	successorNode, err := FindSuccessor_RPC(other, node.Id)
	if err != nil {
		return err
	}
	node.Successor = successorNode
	node.Predecessor = successorNode
	node.FingerTable[1].Node = node.Successor
	return nil
}

// Thread 2: Psuedocode from figure 7 of chord paper
func (node *Node) stabilize(ticker *time.Ticker) {
	for _ = range ticker.C {
		// fmt.Println("We are trying to stabilize the node", node.Id)
		if node.IsShutdown {
			fmt.Printf("[%v-stabilize] Shutting down stabilize timer\n", HashStr(node.Id))
			ticker.Stop()
			return
		}
		successorRemoteNode := node.Successor
		predecessorOfSuccessorRemoteNode, err := GetPredecessorId_RPC(successorRemoteNode)
		if err != nil {
			return
		}
		if predecessorOfSuccessorRemoteNode != nil && Between(predecessorOfSuccessorRemoteNode.Id, node.Id, successorRemoteNode.Id) {
			node.Successor = predecessorOfSuccessorRemoteNode
			node.FingerTable[1].Node = node.Successor
		}
		Notify_RPC(node.Successor, node.RemoteSelf)
	}
}

// Psuedocode from figure 7 of chord paper
func (node *Node) notify(remoteNode *RemoteNode) {
	node.dataMembersLock.Lock()
	defer node.dataMembersLock.Unlock()
	if node.Predecessor == nil || Between(remoteNode.Id, node.Predecessor.Id, node.Id) {
		node.Predecessor = remoteNode
	}
}

// Psuedocode from figure 4 of chord paper
func (node *Node) findSuccessor(id []byte) (*RemoteNode, error) {
	remoteNode, err := node.findPredecessor(id)
	if err != nil {
		return nil, err
	}
	successorNode, rpcErr := GetSuccessorId_RPC(remoteNode)
	if rpcErr != nil {
		return nil, rpcErr
	}
	return successorNode, nil
}

// Psuedocode from figure 4 of chord paper
func (node *Node) findPredecessor(id []byte) (*RemoteNode, error) {
	remoteNode := node.RemoteSelf
	for true {
		var remoteNodeSucc *RemoteNode
		var rpcErr error
		if EqualIds(remoteNode.Id, node.Id) {
			remoteNodeSucc = node.Successor
		} else {
			remoteNodeSucc, rpcErr = GetSuccessorId_RPC(remoteNode)
		}
		if rpcErr != nil {
			return nil, rpcErr
		}
		if BetweenRightIncl(id, remoteNode.Id, remoteNodeSucc.Id) {
			return remoteNode, nil
		}
		var closestPrecedingFingerTableEntry *RemoteNode
		if EqualIds(remoteNode.Id, node.Id) {
			closestPrecedingFingerTableEntry, rpcErr = node.findClosestPrecedingFinger(id)
		} else {
			closestPrecedingFingerTableEntry, rpcErr = ClosestPrecedingFinger_RPC(remoteNode, id)
		}
		if rpcErr != nil {
			return nil, rpcErr
		}
		remoteNode = closestPrecedingFingerTableEntry
	}
	return remoteNode, nil
}

func (node *Node) updateFingerTable(remoteNode *RemoteNode, index int) error {
	if BetweenLeftIncl(remoteNode.Id, node.Id, node.FingerTable[index].Node.Id) {
		node.FingerTable[index].Node = remoteNode
		predecessorNode := node.Predecessor
		if predecessorNode == nil {
			log.Fatal("WE are so screwed")
		}
		UpdateFingerTable_RPC(predecessorNode, remoteNode, index)
	}
	return nil
}

func (node *Node) findClosestPrecedingFinger(id []byte) (*RemoteNode, error) {
	// fmt.Println("Trying to get the closestPrecedingFinger", node.Id)
	for index := KEY_LENGTH; index >= 1; index-- {
		if Between(node.FingerTable[index].Node.Id, node.Id, id) {
			// fmt.Println("The closest preceding finger is", node.FingerTable[index].Node)
			return node.FingerTable[index].Node, nil
		}
	}
	// fmt.Println("The closest preceding finger is(after everything has failed)", node.RemoteSelf)
	return node.RemoteSelf, nil
}

func (node *Node) setPredecessor(remoteNode *RemoteNode) error {
	node.ftLock.Lock()
	defer node.ftLock.Unlock()
	node.Predecessor = remoteNode
	return nil
}
