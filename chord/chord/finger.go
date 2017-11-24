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
	for index := 0; index <= node.BYTE_LENGTH; index++ {
		node.FingerTable = append(node.FingerTable, FingerEntry{
			Start: fingerMath(node.Id, index, node.BYTE_LENGTH),
			Node:  node.RemoteSelf,
		})
	}
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

/* Print contents of a node's finger table */
func PrintFingerTable(node *Node) {
	fmt.Printf("[%v] FingerTable:\n", HashStr(node.Id))
	for _, val := range node.FingerTable {
		fmt.Printf("\t{start:%v\tnodeLoc:%v %v}\n",
			HashStr(val.Start), HashStr(val.Node.Id), val.Node.Addr)
	}
}
