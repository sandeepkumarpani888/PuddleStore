/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Utility functions to help with dealing with ID hashes in Chord. */
/*                                                                           */

package chord

import (
	"bytes"
	"crypto/sha1"
	"math/big"
)

/* Hash a string to its appropriate size */
func HashKey(key string) []byte {
	h := sha1.New()
	h.Write([]byte(key))
	v := h.Sum(nil)
	return v[:KEY_LENGTH/8]
}

/* Convert a []byte to a big.Int string, useful for debugging/logging */
func HashStr(keyHash []byte) string {
	keyInt := big.Int{}
	keyInt.SetBytes(keyHash)
	return keyInt.String()
}

func EqualIds(a, b []byte) bool {
	return bytes.Equal(a, b)
}

/* Example of how to do math operations on []byte IDs, you may not need this function. */
func AddIds(a, b []byte) []byte {
	aInt := big.Int{}
	aInt.SetBytes(a)

	bInt := big.Int{}
	bInt.SetBytes(b)

	sum := big.Int{}
	sum.Add(&aInt, &bInt)
	return sum.Bytes()
}

/* On this crude ascii Chord ring, X is between (A : B)
   ___
  /   \-A
 |     |
B-\   /-X
   ---
*/
func Between(nodeX, nodeA, nodeB []byte) bool {

	aInt := big.Int{}
	aInt.SetBytes(nodeA)

	bInt := big.Int{}
	bInt.SetBytes(nodeB)

	xInt := big.Int{}
	xInt.SetBytes(nodeX)

	var reply bool
	reply = false
	if bInt.Cmp(&aInt) == 0 {
		reply = true
	}
	if bInt.Cmp(&aInt) == 1 && xInt.Cmp(&aInt) == 1 && bInt.Cmp(&xInt) == 1 {
		reply = true
	}
	if bInt.Cmp(&aInt) == -1 && xInt.Cmp(&aInt) == xInt.Cmp(&bInt) {
		reply = true
	}
	// fmt.Println("Doing operation between: %v %v %v: %v", nodeX, nodeA, nodeB, reply, bInt.Cmp(&aInt), xInt.Cmp(&aInt), xInt.Cmp(&bInt))
	return reply
}

/* Is X between (A : B] */
func BetweenRightIncl(nodeX, nodeA, nodeB []byte) bool {

	aInt := big.Int{}
	aInt.SetBytes(nodeA)

	bInt := big.Int{}
	bInt.SetBytes(nodeB)

	xInt := big.Int{}
	xInt.SetBytes(nodeX)
	var reply bool
	reply = false
	if aInt.Cmp(&bInt) == 0 {
		reply = true
	}
	if bInt.Cmp(&aInt) == 1 && xInt.Cmp(&aInt) == 1 && bInt.Cmp(&xInt) <= 0 {
		reply = true
	}
	if bInt.Cmp(&aInt) == -1 && xInt.Cmp(&aInt) == xInt.Cmp(&bInt) {
		reply = true
	}
	if bInt.Cmp(&aInt) == -1 && xInt.Cmp(&bInt) == 0 {
		reply = true
	}
	// fmt.Println("Doing operation betweenRightIncl: %v %v %v: %v", nodeX, nodeA, nodeB, reply)
	return reply
}

/* Is X between [A: B) */
func BetweenLeftIncl(nodeX, nodeA, nodeB []byte) bool {
	aInt := big.Int{}
	aInt.SetBytes(nodeA)

	bInt := big.Int{}
	bInt.SetBytes(nodeB)

	xInt := big.Int{}
	xInt.SetBytes(nodeX)

	if xInt.Cmp(&aInt) <= 0 && bInt.Cmp(&aInt) == 1 && bInt.Cmp(&xInt) == 1 {
		return true
	}
	if bInt.Cmp(&aInt) == -1 && xInt.Cmp(&aInt) <= 0 && bInt.Cmp(&xInt) == 1 {
		return true
	}
	if bInt.Cmp(&aInt) == -1 && xInt.Cmp(&aInt) <= 1 && xInt.Cmp(&bInt) == -1 {
		return true
	}
	return false
}
