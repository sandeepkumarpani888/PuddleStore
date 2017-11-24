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
	node.dataMembersLock.Lock()
	defer node.dataMembersLock.Unlock()
	if node.Successor == nil {
		reply.Id = nil
		reply.Addr = ""
		reply.Valid = false
	} else {
		reply.Id = node.Successor.Id
		reply.Addr = node.Successor.Addr
		reply.Valid = true
	}
	return nil
}

/* RPC */
func (node *Node) Notify(remoteNode *RemoteNode, reply *RpcOkay) error {
	node.dataMembersLock.Lock()
	if node.Predecessor == nil || Between(remoteNode.Id, node.Predecessor.Id, node.Id) {
		node.Predecessor = remoteNode
	}
	node.dataMembersLock.Unlock()
	return nil
}

/* RPC */
func (node *Node) FindSuccessor(query *RemoteQuery, reply *IdReply) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}
	succesorNode, err := node.findSuccessor(query.Id)
	if err != nil {
		reply.Id = nil
		reply.Addr = ""
		reply.Valid = false
		return err
	} else {
		reply.Id = succesorNode.Id
		reply.Addr = succesorNode.Addr
		reply.Valid = true
	}
	return nil
}

/* RPC */
func (node *Node) ClosestPrecedingFinger(query *RemoteQuery, reply *IdReply) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}

	remoteNode, err := node.findClosestPrecedingFinger(query.Id)
	if err != nil {
		reply.Id = nil
		reply.Addr = ""
		reply.Valid = false
		return err
	} else {
		reply.Id = remoteNode.Id
		reply.Addr = remoteNode.Addr
		reply.Valid = true
	}
	return nil
}

/* RPC */
func (node *Node) UpdateFingerTable(query *RemoteFingerEntry, reply *RpcOkay) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}
	err := node.updateFingerTable(&RemoteNode{
		Id:   query.Id,
		Addr: query.Addr,
	}, query.Index)
	return err
}

/* RPC */
func (node *Node) SetPredecessor(query *RemoteSetPredecessor, reply *RpcOkay) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}
	err := node.setPredecessor(&RemoteNode{
		Id:   query.Id,
		Addr: query.Addr,
	})
	return err
}
