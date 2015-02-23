package tapestry

import (
	//	"errors"
	"fmt"
	"time"
)

/*
   Implementation of the local tapestry node.  There are three kinds of methods defined in this file

       1.  Methods that can be invoked remotely via RPC by other Tapestry nodes (eg AddBackpointer, RemoveBackpointer)
       2.  Methods that are invoked by clients (eg Publish, Lookup)
       3.  Common utility methods that you can use in your implementation (eg findRoot)

   For RPC methods, to invoke the equivalent method on a remote node, see the methods defined in tapestry-remote.go
*/

/*
   The ID and location information for a node in the tapestry
*/
type Node struct {
	Id      ID
	Address string
}

/*
   The main struct for the local tapestry node.

   Methods can be invoked locally on this struct.

   Methods on remote TapestryNode instances can be called on the 'tapestry' member
*/
type TapestryNode struct {
	node         Node          // the ID and address of this node
	table        *RoutingTable // the routing table
	backpointers *Backpointers // backpointers to keep track of other nodes that point to us
	store        *ObjectStore  // stores keys for which this node is the root
	tapestry     *Tapestry     // api to the tapestry mesh
}

/*
   Called in tapestry initialization to create a tapestry node struct
*/
func newTapestryNode(node Node, tapestry *Tapestry) *TapestryNode {
	n := new(TapestryNode)
	n.node = node
	n.table = NewRoutingTable(node)
	n.backpointers = NewBackpointers(node)
	n.store = NewObjectStore()
	n.tapestry = tapestry
	return n
}

/*
   Invoked when starting the local node, if we are connecting to an existing Tapestry.

   *    Find the root for our node's ID
   *    Call AddNode on our root to initiate the multicast and receive our initial neighbour set
   *    Iteratively get backpointers from the neighbour set and populate routing table
*/
func (local *TapestryNode) Join(otherNode Node) error {
	Debug.Printf("Joining\n", otherNode)

	// Route to our root
	root, err := local.findRoot(otherNode, local.node.Id)
	if err != nil {
		return fmt.Errorf("Error joining existing tapestry node %v, reason: %v", otherNode, err)
	}

	// Add ourselves to our root by invoking AddNode on the remote node
	neighbours, err := local.tapestry.addNode(root, local.node)
	if err != nil {
		return fmt.Errorf("Error adding ourselves to root node %v, reason: %v", root, err)
	}

	fmt.Printf("neighbours %v\n", len(neighbours))
	// Add the neighbours to our local routing table.
	//for _, n := range neighbours {
	//	fmt.Printf("id: %v\n", n.Id)
	//	local.addRoute(n)
	//}

	// TODO: Students should implement the backpointer traversal portion of Join

	/*
		"The nodes returned by AddNodeMulticast() will go into the
		Joining node's routing table, but all these nodes are of length n
		and greater. This means that rows 0 through n-1 of the node's
		routing table still need to be filled via backpointer traversal."
	*/

	level := SharedPrefixLength(local.node.Id, root.Id)
	for {

		for _, n := range neighbours {
			local.addRoute(n)
		}

		if level > 0 {
			nextNeighbours := make([]Node, 0)
			for _, neighbour := range neighbours {
				fmt.Printf("ENTRE con %v nivel %v\n", local.node.Id, level)
				result, err := local.tapestry.getBackpointers(neighbour, local.node, level-1)

				if err != nil {
					// TODO handle this
				}

				nextNeighbours = append(nextNeighbours, result...)
				nextNeighbours = append(nextNeighbours, neighbours...)
			}

			neighbours = nextNeighbours
		} else {
			break
		}

		level--
	}

	return err // TODO
}

/*
   Client API function to gracefully exit the Tapestry mesh

   *    Notify the nodes in our backpointers that we are leaving by calling NotifyLeave
   *    If possible, give each backpointer a suitable alternative node from our routing table
*/
func (local *TapestryNode) Leave() (err error) {
	// TODO: Students should implement this

	sets := local.backpointers.sets // [DIGITS]*NodeSet
	for _, set := range sets {
		set.mutex.Lock()
		for node, _ := range set.data {
			//TODO: Add suitable replacement from our routing table
			err := local.tapestry.notifyLeave(node, local.node, nil)
			if err != nil {
				//TODO
			}
		}
		set.mutex.Unlock()
	}
	return
}

/*
   Client API.  Publishes the key to tapestry.

   *    Route to the root node for the key
   *    Register our local node on the root
   *    Start periodically republishing the key
   *    Return a channel for cancelling the publish
*/
func (local *TapestryNode) Publish(key string) (done chan bool, err error) {
	// TODO: Students should implement this
	done = make(chan bool)

	root, err := local.findRoot(local.node, Hash(key))
	local.tapestry.register(root, local.node, key)

	//Periodically checking
	go func() {
		for {
			select {
			case <-done:
				return
			case <-time.After(time.Second * 1):
				//republish the key every second.
				root, _ := local.findRoot(local.node, Hash(key))
				local.tapestry.register(root, local.node, key)
			}
		}
	}()

	return done, err //TODO: ?
}

/*
   Client API.  Look up the Tapestry nodes that are storing the blob for the specified key.

   *    Find the root node for the key
   *    Fetch the replicas from the root's object store
   *    Attempt up to RETRIES many times
*/
func (local *TapestryNode) Lookup(key string) (nodes []Node, err error) {
	// TODO: Students should implement this
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   We are the root node for some new node joining the tapestry.
   *    Begin the acknowledged multicast
   *    Return the neighbourset from the multicast
*/
func (local *TapestryNode) AddNode(node Node) (neighbourset []Node, err error) {
	return local.AddNodeMulticast(node, SharedPrefixLength(node.Id, local.node.Id))
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   A new node is joining the tapestry, and we are a need-to-know node participating in the multicast.
   *    Propagate the multicast to the specified row in our routing table
   *    Await multicast response and return the neighbourset
   *    Begin transfer of appropriate replica info to the new node
*/
func (local *TapestryNode) AddNodeMulticast(newnode Node, level int) (neighbours []Node, err error) {
	Debug.Printf("Add node multicast %v at level %v\n", newnode, level)
	fmt.Printf("Add node multicast %v at level %v\n", newnode, level)
	// TODO: Students should implement this
	neighbours = local.table.GetLevel(level)
	fmt.Printf("%v: Neighbours for node %v\n", local.node.Id, neighbours)
	results := make([]Node, 0)
	for _, target := range neighbours {
		result, err := local.tapestry.addNodeMulticast(
			target, newnode, level+1)
		results = append(results, result...)

		if err != nil {
			// TODO
		}
	}
	err = local.tapestry.transfer(newnode, local.node, nil)
	if err != nil {
		// TODO
	}

	neighbours = append(neighbours, local.node)
	neighbours = append(neighbours, results...)
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   Another node is informing us of a graceful exit.
   *    Remove references to the from node from our routing table and backpointers
   *    If replacement is not nil, add replacement to our routing table
*/
func (local *TapestryNode) NotifyLeave(from Node, replacement *Node) (err error) {
	Debug.Printf("Received leave notification from %v with replacement node %v\n", from, replacement)

	// TODO: Students should implement this
	local.table.Remove(from)
	local.RemoveBackpointer(from)

	if replacement != nil {
		local.addRoute(*replacement)
	}
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Returns the best candidate from our routing table for routing to the provided ID
*/
func (local *TapestryNode) GetNextHop(id ID) (morehops bool, nexthop Node, err error) {
	// TODO: Students should implement this

	// Call routingtable.go method
	nexthop = local.table.GetNextHop(id)

	// If all digits match (aka is equal), no more hops are needed.
	sharedDigits := SharedPrefixLength(local.node.Id, nexthop.Id)
	morehops = DIGITS != sharedDigits

	// If calling nexthop is worse than the current one, it errors out.
	// TODO: Is this the potential erorr?
	// if id.BetterChoice(local.node.Id, nexthop.Id) {
	// 	err = errors.New("Next hop was not better than the previous")
	// }
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Add the from node to our backpointers
   *    Possibly add the node to our routing table, if appropriate
*/
func (local *TapestryNode) AddBackpointer(from Node) (err error) {
	if local.backpointers.Add(from) {
		Debug.Printf("Added backpointer %v\n", from)
	}
	local.addRoute(from)
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Remove the from node from our backpointers
*/
func (local *TapestryNode) RemoveBackpointer(from Node) (err error) {
	if local.backpointers.Remove(from) {
		Debug.Printf("Removed backpointer %v\n", from)
	}
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Get all backpointers at the level specified
   *    Possibly add the node to our routing table, if appropriate
*/
func (local *TapestryNode) GetBackpointers(from Node, level int) (backpointers []Node, err error) {
	Debug.Printf("Sending level %v backpointers to %v\n", level, from)
	backpointers = local.backpointers.Get(level)
	local.addRoute(from)
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   The provided nodes are bad and we should discard them
   *    Remove each node from our routing table
   *    Remove each node from our set of backpointers
*/
func (local *TapestryNode) RemoveBadNodes(badnodes []Node) (err error) {
	for _, badnode := range badnodes {
		if local.table.Remove(badnode) {
			Debug.Printf("Removed bad node %v\n", badnode)
		}
		if local.backpointers.Remove(badnode) {
			Debug.Printf("Removed bad node backpointer %v\n", badnode)
		}
	}
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.
   Register the specified node as an advertiser of the specified key.

   *    Check that we are the root node for the key
   *    Add the node to the object store
   *    Kick off a timer to remove the node if it's not advertised again after a set amount of time
*/
func (local *TapestryNode) Register(key string, replica Node) (isRoot bool, err error) {
	// TODO: Students should implement this
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes.

   *    Check that we are the root node for the requested key
   *    Return all nodes that are registered in the local object store for this key
*/
func (local *TapestryNode) Fetch(key string) (isRoot bool, replicas []Node, err error) {
	// TODO: Students should implement this
	return
}

/*
   This method is invoked over RPC by other Tapestry nodes

   *    Register all of the provided objects in the local object store
   *    If appropriate, add the from node to our local routing table
*/
func (local *TapestryNode) Transfer(from Node, replicamap map[string][]Node) error {
	// TODO: Students should implement this
	local.store.RegisterAll(replicamap, TIMEOUT)

	// TODO: when is it appropiate to add to local table?
	err := local.addRoute(from)
	return err
}

/*
   Utility function that adds a node to our routing table

   *    Adds the provided node to the routing table, if appropriate.
   *    If the node was added to the routing table, notify the node of a backpointer
   *    If an old node was removed from the routing table, notify the old node of a removed backpointer
*/

func (local *TapestryNode) addRoute(node Node) (err error) {
	// TODO: Students should implement this
	added, prev := local.table.Add(node)

	if added {
		local.tapestry.addBackpointer(node, local.node)
		// TODO notify
	}

	if prev != nil {
		local.tapestry.removeBackpointer(node, local.node)
		// TODO notify old node
	}

	return
}

/*
   Utility function for iteratively contacting nodes to get the root node for the provided ID

   *    Starting from the specified node, iteratively contact nodes calling getNextHop until we reach the root node
   *    Also keep track of any bad nodes that errored during lookup
   *    At each step, notify the next-hop node of all of the bad nodes we have encountered along the way
*/
func (local *TapestryNode) findRoot(start Node, id ID) (Node, error) {
	Debug.Printf("Routing to %v\n", id)
	// TODO: Students should implement this

	// I hate Go's scoping </3
	next := start
	var current Node
	var err error
	var hasNext bool

	for { // No do whiles in Go </3
		current = next
		hasNext, next, err = local.tapestry.getNextHop(current, id)

		fmt.Printf("I am %v, next hop %v\n", current.Id, next.Id)

		if err != nil {
			// TODO: Notify who and what? If I got a node back then whats the problem?
		}

		if !hasNext {
			break
		}
	}
	fmt.Printf("returned %v\n", current.Id)

	// TODO: Again, what error goes here?
	return current, nil
}
