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
// TODO: Write test for this function
func (node *Node) initFingerTable() {
	// add a 0 index to make handling array easier
	for index := 0; index <= KEY_LENGTH; index++ {
		node.FingerTable = append(node.FingerTable, FingerEntry{
			Start: fingerMath(node.Id, index, KEY_LENGTH),
			Node:  node.RemoteSelf,
		})
	}
	PrintFingerTable(node)
	node.Successor = node.RemoteSelf
}

func (node *Node) updateOthers() {
	// fmt.Println("Start updating the other nodes(updateOthers)")
	for index := 1; index <= KEY_LENGTH; index++ {
		predecessorNode, err := node.findPredecessor(fingerMathSub(node.Id, index, KEY_LENGTH))
		// predecessorNode, err := FindPredecessor_RPC(node.RemoteSelf, fingerMathSub(node.Id, index, KEY_LENGTH))
		fmt.Println("Got the predecessor for the node(inside updateOthers)", node.Id, predecessorNode.Id, index)
		if err != nil {
			return
		}
		// fmt.Println("updating the finger table of the nodes", predecessorNode.Id, node.RemoteSelf.Id, index)
		UpdateFingerTable_RPC(predecessorNode, node.RemoteSelf, index)
	}
}

/* Called periodically (in a seperate go routine) to fix entries in our finger table. */
func (node *Node) fixNextFinger(ticker *time.Ticker) {
	for _ = range ticker.C {
		node.fixFingerIndex = 2
		succesor, err := node.findSuccessor(node.FingerTable[node.fixFingerIndex].Start)
		fmt.Println("We are fixing fingerIndex: %v for node:%v", node.fixFingerIndex, node.Id, succesor.Id)
		if err == nil {
			node.ftLock.Lock()
			node.FingerTable[node.fixFingerIndex].Node = succesor
			node.fixFingerIndex++
			if node.fixFingerIndex == KEY_LENGTH {
				node.fixFingerIndex = 1
			}
			if node.fixFingerIndex == 1 {
				node.Successor = succesor
			}
			node.ftLock.Unlock()
		}
	}
}

/* (n + 2^i) mod (2^m) */
func fingerMath(n []byte, i int, m int) []byte {
	two := &big.Int{}
	two.SetInt64(2)

	N := &big.Int{}
	N.SetBytes(n)

	// 2^i
	I := &big.Int{}
	I.SetInt64(int64(i))
	I.Exp(two, I, nil)

	// 2^m
	M := &big.Int{}
	M.SetInt64(int64(m))
	M.Exp(two, M, nil)

	result := &big.Int{}
	result.Add(N, I)
	result.Mod(result, M)

	// Big int gives an empty array if value is 0.
	// Here is a way for us to still return a 0 byte
	zero := &big.Int{}
	zero.SetInt64(0)
	if result.Cmp(zero) == 0 {
		return []byte{0}
	}

	return result.Bytes()
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
