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
	// consider the case when the ring is empty and not empty
	node.dataMembersLock.Lock()
	defer node.dataMembersLock.Unlock()
	if other != nil {
		node.initFingerTable()
		node.initFingerTableWithNode(other)
		node.updateOthers()
	} else {
		node.initFingerTable()
	}
	return nil
}

// Thread 2: Psuedocode from figure 7 of chord paper
func (node *Node) stabilize(ticker *time.Ticker) {
	for _ = range ticker.C {
		if node.IsShutdown {
			fmt.Printf("[%v-stabilize] Shutting down stabilize timer\n", HashStr(node.Id))
			ticker.Stop()
			return
		}
		node.dataMembersLock.Lock()
		successorRemoteNode := node.Successor
		predecessorOfSuccessorRemoteNode, err := GetPredecessorId_RPC(successorRemoteNode)
		if err != nil {
			node.dataMembersLock.Unlock()
			return
		}
		if Between(predecessorOfSuccessorRemoteNode.Id, node.Id, successorRemoteNode.Id) {
			node.Successor = predecessorOfSuccessorRemoteNode
		}
		node.dataMembersLock.Unlock()
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
		remoteNodeSucc, rpcErr := GetSuccessorId_RPC(remoteNode)
		if rpcErr != nil {
			return nil, rpcErr
		}
		if BetweenRightIncl(id, remoteNode.Id, remoteNodeSucc.Id) {
			return remoteNode, nil
		}
		closestPrecedingFingerTableEntry, rpcErr := ClosestPrecedingFinger_RPC(remoteNode, id)
		if rpcErr != nil {
			return nil, rpcErr
		}
		remoteNode = closestPrecedingFingerTableEntry
	}
	return nil, nil
}

func (node *Node) updateFingerTable(remoteNode *RemoteNode, index int) error {
	node.ftLock.Lock()
	defer node.ftLock.Unlock()
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
	for index := node.BYTE_LENGTH; index >= 1; index-- {
		if Between(node.FingerTable[index].Node.Id, node.Id, id) {
			return node.FingerTable[index].Node, nil
		}
	}
	return node.RemoteSelf, nil
}

func (node *Node) setPredecessor(remoteNode *RemoteNode) error {
	node.ftLock.Lock()
	defer node.ftLock.Unlock()
	node.Predecessor = remoteNode
}
