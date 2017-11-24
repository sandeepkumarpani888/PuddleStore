/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Finger table related functions for a given Chord node.          */
/*                                                                           */

package chord

import (
	"fmt"
	"math/big"
	"math/rand"
	"time"
)

/* A single finger table entry */
type FingerEntry struct {
	Start []byte      /* ID hash of (n + 2^i) mod (2^m)  */
	Node  *RemoteNode /* RemoteNode that Start points to */
}

/* Create initial finger table that only points to itself, will be fixed later */
// TODO: Write test for this function
func (node *Node) initFingerTable() {
	// add a 0 index to make handling array easier
	for index := 0; index <= node.BYTE_LENGTH; index++ {
		node.FingerTable = append(node.FingerTable, FingerEntry{
			Start: fingerMath(node.Id, index, node.BYTE_LENGTH),
			Node:  node.RemoteSelf,
		})
	}
	node.Predecessor = node.RemoteSelf
}

/* Already in lock */
func (node *Node) initFingerTableWithNode(other *RemoteNode) {
	successorRemoteNode, err := FindSuccessor_RPC(other, node.FingerTable[1].Start)
	if err != nil {
		return
	}
	node.ftLock.Lock()
	node.FingerTable[1].Node = successorRemoteNode
	predecessorRemoteNode, predErr := GetPredecessorId_RPC(node.FingerTable[1].Node)
	if predErr != nil {
		return
	}
	node.Predecessor = predecessorRemoteNode
	// implement this function
	SetPredecessor(predecessorRemoteNode, node.RemoteSelf)
	for index := 1; index <= node.BYTE_LENGTH-1; index++ {
		if BetweenLeftIncl(node.FingerTable[index+1].Node.Id, node.Id, node.FingerTable[index].Node.Id) {
			node.FingerTable[index+1].Node = node.FingerTable[index].Node
		} else {
			successorRemoteNode, err = FindSuccessor_RPC(other, node.FingerTable[index+1].Start)
			node.FingerTable[index+1].Node = successorRemoteNode
		}
	}
	node.ftLock.Unlock()
}

func (node *Node) updateOthers() {
	for index := 1; index <= node.BYTE_LENGTH; index++ {
		predecessorNode, err := FindPredecessor_RPC(node.RemoteSelf, fingerMathSub(node.Id, index, node.BYTE_LENGTH))
		if err != nil {
			return
		}
		UpdateFingerTable_RPC(predecessorNode, node.RemoteSelf, index)
	}
}

/* Called periodically (in a seperate go routine) to fix entries in our finger table. */
func (node *Node) fixNextFinger(ticker *time.Ticker) {
	for _ = range ticker.C {
		whichId := rand.Int() % (node.BYTE_LENGTH + 1)
		if whichId > 1 {
			succesor, err := FindSuccessor_RPC(node.RemoteSelf, node.FingerTable[whichId].Start)
			if err == nil {
				node.ftLock.Lock()
				node.FingerTable[whichId].Node = succesor
				node.ftLock.Unlock()
			}
		}
	}
}

/* (n + 2^i) mod (2^m) */
func fingerMath(n []byte, i int, m int) []byte {
	nInt := big.Int{}
	// got N
	nInt.SetBytes(n)
	powerRep := big.Int{}

	oneRep := big.NewInt(1)
	// got 2^i
	powerRep.Lsh(oneRep, uint(i))

	powerRepMod := big.Int{}
	powerRepMod.Lsh(oneRep, uint(m))

	// got 2^i + n
	powerRep.Add(&nInt, &powerRep)

	powerRepMod.Mod(&powerRep, &powerRepMod)
	return powerRepMod.Bytes()
}

/* (n - 2^i) mod (2^m) */
func fingerMathSub(n []byte, i int, m int) []byte {
	nInt := big.Int{}
	// got N
	nInt.SetBytes(n)
	powerRep := big.Int{}

	oneRep := big.NewInt(1)
	// got 2^i
	powerRep.Lsh(oneRep, uint(i))

	powerRepMod := big.Int{}
	// got 2^m
	powerRepMod.Lsh(oneRep, uint(m))

	// got n - 2^i
	powerRep.Sub(&nInt, &powerRep)
	// got n - 2^i + 2^m
	powerRep.Add(&powerRep, &powerRepMod)
	powerRepMod.Mod(&powerRep, &powerRepMod)
	return powerRepMod.Bytes()
}

/* Print contents of a node's finger table */
func PrintFingerTable(node *Node) {
	fmt.Printf("[%v] FingerTable:\n", HashStr(node.Id))
	for _, val := range node.FingerTable {
		fmt.Printf("\t{start:%v\tnodeLoc:%v %v}\n",
			HashStr(val.Start), HashStr(val.Node.Id), val.Node.Addr)
	}
}
