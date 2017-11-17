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
	if other.Id == nil {
		node.initFingerTable()
		return nil
	}
	node.initFingerTable()
	node.fixFingerTable(other)
	node.updateOthers()
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

		remoteNode := new(RemoteNode)
		remoteNode.Addr = node.Addr
		remoteNode.Id = node.Id
		successorNode, err := GetSuccessorId_RPC(remoteNode)
		if err != nil {
			log.Printf("We have encountered an error")
		}
		predOfSucc, predErr := GetPredecessorId_RPC(successorNode)
		if predErr != nil {
			log.Printf("We have encountered another error")
		}
		if Between(predOfSucc.Id, node.Id, successorNode.Id) {
			successorNode = predOfSucc
		}
		// this will help us notify the successorNode of the currentNode if
		// we are tis predecessor
		Notify_RPC(successorNode, remoteNode)
	}
}

// Psuedocode from figure 7 of chord paper
func (node *Node) notify(remoteNode *RemoteNode) {

	if node.Predecessor == nil || Between(remoteNode.Id, node.Predecessor.Id, node.Id) {
		node.Predecessor = remoteNode
	}
}

// Psuedocode from figure 4 of chord paper
func (node *Node) findSuccessor(id []byte) (*RemoteNode, error) {
	remoteNode, err := node.findPredecessor(id)
	if err != nil {
		log.Printf("We have encounterd another error")
	}
	remoteNode, err = GetSuccessorId_RPC(remoteNode)
	if err != nil {
		return nil, err
	}
	return remoteNode, err

}

// Psuedocode from figure 4 of chord paper
func (node *Node) findPredecessor(id []byte) (*RemoteNode, error) {
	remoteNode := new(RemoteNode)
	remoteNode.Addr = node.Addr
	remoteNode.Id = node.Id
	for true {
		successorNode, _ := GetSuccessorId_RPC(remoteNode)
		if !BetweenRightIncl(id, remoteNode.Id, successorNode.Id) {
			remoteNodeNext, err := ClosestPrecedingFinger_RPC(remoteNode, id)
			if err != nil {
				log.Printf("We screwed up somewhere")
			}
			remoteNode = remoteNodeNext
		} else {
			break
		}
	}
	return remoteNode, nil
}
