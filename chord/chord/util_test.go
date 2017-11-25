/*                                                                           */
/*  Brown University, CS138, Spring 2015                                     */
/*                                                                           */
/*  Purpose: Utility functions to help with dealing with ID hashes in Chord. */
/*                                                                           */

package chord

import "testing"

func TestBetweenRightIncl(t *testing.T) {
	type args struct {
		nodeX []byte
		nodeA []byte
		nodeB []byte
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
	// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := BetweenRightIncl(tt.args.nodeX, tt.args.nodeA, tt.args.nodeB); got != tt.want {
				t.Errorf("BetweenRightIncl() = %v, want %v", got, tt.want)
			}
		})
	}
}
