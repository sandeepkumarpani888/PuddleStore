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
	return nil
	//TODO students should implement this method
}

// Thread 2: Psuedocode from figure 7 of chord paper
func (node *Node) stabilize(ticker *time.Ticker) {
	for _ = range ticker.C {
		if node.IsShutdown {
			fmt.Printf("[%v-stabilize] Shutting down stabilize timer\n", HashStr(node.Id))
			ticker.Stop()
			return
		}

		//TODO students should implement this method
		succ := node.Successor
		pred, err := node.findPredecessor(succ.Id)
		if err != nil {
			log.Fatal("findPredecessor error: " + err.Error())
		}

		if Between(pred.Id, node.Id, succ.Id) {
			node.Successor = pred
		}
		//TODO: succ.Notify(node)
	}
}

// Psuedocode from figure 7 of chord paper
func (node *Node) notify(remoteNode *RemoteNode) {

	//TODO students should implement this method
	if node.Predecessor == nil ||
		Between(remoteNode.Id, node.Predecessor.Id, node.Id) {

		node.Predecessor = remoteNode
		// TODO: transfer keys
	}
}

// Psuedocode from figure 4 of chord paper
func (node *Node) findSuccessor(id []byte) (*RemoteNode, error) {
	//TODO students should implement this method
	return nil, nil

}

// Psuedocode from figure 4 of chord paper
func (node *Node) findPredecessor(id []byte) (*RemoteNode, error) {
	//TODO students should implement this method
	return nil, nil
}
