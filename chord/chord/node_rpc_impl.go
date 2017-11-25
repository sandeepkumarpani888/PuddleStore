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
	if node.Predecessor == nil {
		reply.Id = nil
		reply.Addr = ""
		reply.Valid = false
	} else {
		reply.Addr = node.Predecessor.Addr
		reply.Id = node.Predecessor.Id
		reply.Valid = true
	}
	return nil
}

/* RPC */
func (node *Node) FindPredecessorGivenID(req *RemoteQuery, reply *IdReply) error {
	// fmt.Println("We are trying to get the predecessor for the given id (FindPredecessor)", node.Id, req.Id)
	if err := validateRpc(node, req.FromId); err != nil {
		return err
	}
	// Predecessor may be nil, which is okay.
	remoteNode, errFromFindPredecessor := node.findPredecessor(req.Id)
	if errFromFindPredecessor != nil {
		// fmt.Println("We got an error from finding the predecessor with the given id", errFromFindPredecessor)
		reply.Addr = ""
		reply.Id = nil
		reply.Valid = false
	} else {
		reply.Addr = remoteNode.Addr
		reply.Id = remoteNode.Id
		reply.Valid = true
	}
	// fmt.Println("We are returning the predecessor for the node with ID", node.Id, reply.Id)
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
		reply.Id = node.Successor.Id
		reply.Addr = node.Successor.Addr
		reply.Valid = true
	}
	// fmt.Println("We got the successor for the node", req.Id, reply)
	return nil
}

/* RPC */
func (node *Node) Notify(remoteNode *RemoteNode, reply *RpcOkay) error {
	if node.Predecessor == nil || Between(remoteNode.Id, node.Predecessor.Id, node.Id) {
		node.Predecessor = remoteNode
	}
	return nil
}

/* RPC */
func (node *Node) FindSuccessor(query *RemoteQuery, reply *IdReply) error {
	if err := validateRpc(node, query.FromId); err != nil {
		return err
	}
	successorNode, errSucc := node.findSuccessor(query.Id)
	if errSucc != nil {
		return errSucc
	}
	reply.Addr = successorNode.Addr
	reply.Id = successorNode.Id
	reply.Valid = true
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
	// fmt.Println("Setting the predecessor", node.RemoteSelf.Addr, node.RemoteSelf.Id, query.Addr, query.Id)
	node.Predecessor = &RemoteNode{
		Id:   query.Id,
		Addr: query.Addr,
	}
	// fmt.Println("We are updating the predecessor of the node", node.Id, query.Id)
	reply.Ok = true
	return nil
}
