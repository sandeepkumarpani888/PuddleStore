/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Finger table related functions for a given Chord node.          */
/*                                                                           */

package chord

import (
	"fmt"
	"math/big"
	"time"
)

/* A single finger table entry */
type FingerEntry struct {
	Start []byte      /* ID hash of (n + 2^i) mod (2^m)  */
	Node  *RemoteNode /* RemoteNode that Start points to */
}

/* Create initial finger table that only points to itself, will be fixed later */
func (node *Node) initFingerTable() {
	node.ftLock.Lock()
	byteLength := len(node.Id)
	remoteNode := new(RemoteNode)
	remoteNode.Addr = node.Addr
	remoteNode.Id = node.Id

	for id := 0; id <= byteLength; id++ {
		fingerTableEntry := FingerEntry{}
		fingerTableEntry.Node = remoteNode
		fingerTableEntry.Start = fingerMath(node.Id, id, byteLength)

		node.FingerTable = append(node.FingerTable, fingerTableEntry)
	}

	defer node.ftLock.Unlock()
}

func (node *Node) updateOthers() error {
	//TODO
}

func (node *Node) fixFingerTable(other *RemoteNode) error {
	node.ftLock.Lock()
	defer node.ftLock.Unlock()
	currentRemoteNode := new(RemoteNode)
	currentRemoteNode.Addr = node.Addr
	currentRemoteNode.Id = node.Id
	remoteNodeForTheFirstEntryInFingerTable, _ := FindSuccessor_RPC(other, node.FingerTable[1].Start)
	node.FingerTable[1].Node = remoteNodeForTheFirstEntryInFingerTable
	predecessorOfNode, _ := GetPredecessorId_RPC(node.FingerTable[1].Node)
	node.Predecessor = predecessorOfNode
	Notify_RPC(node.FingerTable[1].Node, currentRemoteNode)

	byteLength := len(node.Id)

	for id := 1; id < byteLength; id++ {
		if Between(node.FingerTable[id+1].Start, node.Id, node.FingerTable[id].Node.Id) {
			node.FingerTable[id+1].Node = node.FingerTable[id].Node
		} else {
			successorNode, _ := FindSuccessor_RPC(other, node.FingerTable[id+1].Start)
			node.FingerTable[id+1].Node = successorNode
		}
	}
	return nil
}

/* Called periodically (in a seperate go routine) to fix entries in our finger table. */
func (node *Node) fixNextFinger(ticker *time.Ticker) {
	for _ = range ticker.C {
		//TODO students should implement this method
	}
}

/* (n + 2^i) mod (2^m) */
func fingerMath(n []byte, i int, m int) []byte {
	nInt := big.Int{}
	nInt.SetBytes(n)

	iInt := big.NewInt(1)
	iInt.Lsh(iInt, uint(i))

	mInt := big.NewInt(1)
	mInt.Lsh(mInt, uint(m))

	res := big.Int{}
	res.Add(&nInt, iInt)
	res.Mod(&res, mInt)
	return res.Bytes()
}

/* Print contents of a node's finger table */
func PrintFingerTable(node *Node) {
	fmt.Printf("[%v] FingerTable:\n", HashStr(node.Id))
	for _, val := range node.FingerTable {
		fmt.Printf("\t{start:%v\tnodeLoc:%v %v}\n",
			HashStr(val.Start), HashStr(val.Node.Id), val.Node.Addr)
	}
}
