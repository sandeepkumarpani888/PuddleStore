/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Local Chord node functions to interact with the Chord ring.     */
/*                                                                           */

package chord

import (
	"fmt"
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
	node.ftLock.Lock()
	node.Successor = successorNode
	node.FingerTable[0].Node = successorNode
	fmt.Printf("### We found the initial successor to be %v for %v", successorNode.Id, node.Id)
	PrintFingerTable(node)
	node.ftLock.Unlock()
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
		successorRemoteNode := node.Successor
		predecessorOfSuccessorRemoteNode, err := GetPredecessorId_RPC(successorRemoteNode)
		if err != nil {
			fmt.Println("We encountered an error while going with this plan", err)
			return
		}
		if predecessorOfSuccessorRemoteNode != nil && BetweenRightIncl(predecessorOfSuccessorRemoteNode.Id, node.Id, successorRemoteNode.Id) {
			node.Successor = predecessorOfSuccessorRemoteNode
			node.FingerTable[0].Node = node.Successor
		}
		fmt.Println("We are gonna notify the node(%v): that node(%v) is predecessor", node.Successor.Id, node.RemoteSelf.Id)
		if !EqualIds(node.Successor.Id, node.RemoteSelf.Id) {
			Notify_RPC(node.Successor, node.RemoteSelf)
		}
	}
}

// Psuedocode from figure 7 of chord paper
func (node *Node) notify(remoteNode *RemoteNode) {
	node.ftLock.Lock()
	defer node.ftLock.Unlock()
	if node.Predecessor != nil {
		fmt.Println("(NOTIFY(%v)(%v))::Current predecessor of the node (%v) is (%v)", node.Id, remoteNode.Id, node.Id, node.Predecessor.Id)
	}
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
	fmt.Println("### Successor node for %v with length is %v: %v", node.Id, id, successorNode.Id)
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
		if Between(id, remoteNode.Id, remoteNodeSucc.Id) {
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

func (node *Node) findClosestPrecedingFinger(id []byte) (*RemoteNode, error) {
	// fmt.Println("Trying to get the closestPrecedingFinger", node.Id)
	for index := KEY_LENGTH - 1; index >= 0; index-- {
		if Between(node.FingerTable[index].Node.Id, node.Id, id) {
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
