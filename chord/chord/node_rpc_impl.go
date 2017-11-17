/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: RPC API implementation, these are the functions that actually   */
/*           get executed on a destination Chord node when a *_RPC()         */
/*           function is called.                                             */
/*                                                                           */

package chord

import (
	"bytes"
	"errors"
	"fmt"
)

/* Validate that we're executing this RPC on the intended node */
func validateRpc(node *Node, reqId []byte) error {
	if !bytes.Equal(node.Id, reqId) {
		errStr := fmt.Sprintf("Node ids do not match %v, %v", node.Id, reqId)
		return errors.New(errStr)
	}
	return nil
}

/* RPC */
func (node *Node) GetPredecessorId(req *RemoteId, reply *IdReply) error {
	if err := validateRpc(node, req.Id); err != nil {
		return err
	}
	// Predecessor may be nil, which is okay.
	if node.Predecessor == nil {
		reply.Id = nil
		reply.Addr = ""
		reply.Valid = false
	} else {
		reply.Id = node.Predecessor.Id
		reply.Addr = node.Predecessor.Addr
		reply.Valid = true
	}
	return nil
}

/* RPC */
func (node *Node) GetSuccessorId(req *RemoteId, reply *IdReply) error {
	if err := validateRpc(node, req.Id); err != nil {
		return err
	}
	if node.Successor == nil {
		reply.Id = nil
		reply.Addr = ""
		reply.Valid = false
	} else {
		reply.Id = node.Predecessor.Id
		reply.Addr = node.Predecessor.Addr
		reply.Valid = true
	}
	return nil
}

/* RPC */
func (node *Node) Notify(remoteNode *RemoteNode, reply *RpcOkay) error {
	node.ftLock.Lock()
	defer node.ftLock.Unlock()
	if node.Predecessor == nil || Between(remoteNode.Id, node.Predecessor.Id, node.Id) {
		node.Predecessor = remoteNode
		reply.Ok = true
	}
	return nil
}

/* RPC */
func (node *Node) FindSuccessor(query *RemoteQuery, reply *IdReply) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}
	remoteNode, err := node.findPredecessor(query.Id)
	if err != nil {
		reply.Addr = ""
		reply.Valid = false
		return err
	}
	reply.Addr = remoteNode.Addr
	reply.Id = remoteNode.Id
	reply.Valid = true
	return nil
}

/* RPC */
func (node *Node) ClosestPrecedingFinger(query *RemoteQuery, reply *IdReply) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}

	for n := len(query.Id); n >= 1; n-- {
		if Between(node.FingerTable[n].Node.Id, node.Id, query.Id) {
			reply.Addr = node.FingerTable[n].Node.Addr
			reply.Id = node.FingerTable[n].Node.Id
			reply.Valid = true
			break
		}
	}
	reply.Addr = node.Addr
	reply.Id = node.Id
	reply.Valid = true
	return nil
}
