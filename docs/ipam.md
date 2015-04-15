See the [requirements](https://github.com/zettio/weave/wiki/IP-allocation-requirements).

At its highest level, the idea is that we start with a certain IP
address space, known to all nodes, and divide it up between the
nodes. This allows nodes to allocate and free individual IPs locally,
until they run out.

We use a CRDT to represent shared knowledge about the space,
transmitted over the Weave Gossip mechanism, together with
point-to-point messages for one peer to request more space from
another.

The allocator running at each node also has an http interface which
the container infrastructure (e.g. the Weave script, or a Docker
plugin) uses to request IP addresses.

![Schematic of IP allocation in operation](https://docs.google.com/drawings/d/1-EUIRKYxwfKTpBJ7v_LMcdvSpodIMSz4lT3wgEfWKl4/pub?w=701&h=310)

## Commands

The commands supported by the allocator are:

- Allocate: request one IP address for a container
- Delete records for one container: all IP addresses allocated to that
  container are freed.
- Claim: request a specific IP address for a container (e.g. because
  it is already using that address)
- Free: return an IP address that is currently allocated

## Definitions

1. Allocations. We use the word 'allocation' to refer to a specific
   IP address being assigned, e.g. so it can be given to a container.

2. Range. Most of the time, instead of dealing with individual IP
   addresses, we operate on them in contiguous groups, for which we
   use the word "range".  A range has a start address and a length.

3. Universe. The address space from which all ranges are
   allocated. This is configured at start-up and cannot be changed
   during the run-time of the system.

### The Ring

We consider the universe as a ring, and place tokens at the start of
each range owned by a node.  The node owns every address from the
start token up to but not including the next token which denotes
another owned range. Ranges wrap around the end of the universe.

This ring is a CRDT.  Nodes only ever make changes in ranges that they
own (except under administrator command - see later). This makes the
data structure inherently convergent.

In more detail:
- The mapping is from token -> {peer name, version, tombstone flag}
- A token is an IP address.
- The mapping is sorted by token.
- Peer names are taken from Weave: they are unique and survive across restarts.
- A host owns the ranges indicated by the tokens it owns.
- A token can only be inserted by the host owning the range it is inserted into.
- Entries in the map can only be updated by the owning host, and when
  this is done the version is incremented
- The map is always gossiped in its entirety
- The merge operation when a host receives a map is:
  - Disjoint tokens are just copied into the combined map
  - For entries with the same token, pick the highest version number
- If a token maps to a tombstone, this indicates that the previous
  owning host that has left the network.
  - For the purpose of range ownership, tombstones are ignored - ie
    ranges extend past tombstones.
  - Tombstones are only inserted by an administrative action (see below)
- The data gossiped about the ring also includes the amount of free
  space in each range: this is not essential but it improves the
  selection of nodes to ask for space.

### The allocator

- The allocator can allocate freely to containers on your machine from ranges you own
  - This data does not need to be persisted (assumed to have the same failure domain)
- If the allocator runs out of space (all owned ranges are full), it
  will ask another host for space
  - we pick a host to ask at random, weighted by the amount of space
    owned by each host
  - if the target host decides to give up space, it unicasts a message
    back to the asker with the newly-updated ring.
  - we will continue to ask for space until we receive some via the
    gossip mechanism, or our copy of the ring tells us all nodes are
    full.

- When hosts are asked for space, there are 4 scenarios:
  1. We have an empty range; we can change the host associated with
  the token at the beginning of the range, increment the version and
  gossip that
  2. We have a range which can be subdivided by a single token to form
  a free range.  We insert said token, mapping to the host requesting
  space and gossip that.
  3. We have a 'hole' in the middle of a range; an empty range can be
  created by inserting two tokens, one at the beginning of the hole
  mapping to the host requesting the space, and one at the end of the
  hole mapping to us.
  4. We have no space.

## Initialisation

Nodes are told the universe - the IP range from which all allocations
are made - when starting up.  Each node must be given the same range.

We deal with concurrent start-up through a process of leader election.
In essence, the node with the highest id claims the entire universe for
itself, and then other nodes can begin to request ranges from it.

When a node starts up, it initialises the CRDT to just an empty
ring. It then waits for ring updates and/or commands.  Until a node
receives a command to allocate or claim an IP address, it does not
care what else is going on, but it does need to keep track of it.

When a node first receives such a command, it consults its map. If it
sees any other nodes that *do* claim to have some tokens, then it
proceeds with normal allocation [see above]. Otherwise, if no such
update has been received (i.e. we've either received no update or only
updates with an empty ring), then the node starts a leader election.
If this node has the highest id, then the node claims the entire
universe for itself, inserting one token at the beginning of the ring,
and broadcasts the ring.

If it sees that another node has the highest ID, it sends a message to
that node proposing that it be leader.  A node receiving such a
message proceeds as above: only if it sees no other nodes with tokens
and it sees itself as the node with the highest ID does it take
charge.

Note that the chosen leader can be a node other than the one with the
overall highest id. This happens in the event a node with a higher id
starts up just as another node has made the decision to become
leader. That is ok, as long as the new node hasn't already run its own
leader election.

Failures:
- two nodes that haven't communicated with each other yet can each
  decide to be leader
  -> this is a fundamental problem: if you don't know about someone
     else then you cannot make allowances for them.
     [To improve matters, nodes could wait for a bit to see if more
     peers arrive before running an election.  Also, we could look at
     the command-line parameters supplied to Weave, and wait longer in
     the case that we have been asked to connect to someone specific.]
- prospective leader dies before sending map
  -> This failure will be detected by the underlying Weave peer
     topology. The node requiring space will re-try, re-running the
     leadership election across all connected peers.
     [Currently, re-try is triggered by gossip arriving, or another
     command.  It is possible for neither to happen.]

## Node shutdown

When a node leaves (a `weave reset` command), it updates all its own
tokens to be tombstones, then broadcasts the updated ring.  This
causes its space to be inherited by the owner of the previous tokens
on the ring, for each range.

After sending the message, the node terminates - it does not wait for
any response.

Failures:
- message lost
  - the space will be unusable by other nodes because it will still be
    seen as owned.

To cope with the situation where a node has left or died without
managing to tell its peers, an administrator may go to any other node
and command that it mark the dead node's tokens as tombstones (with
the `weave rmpeer` command).  This information will then be gossipped
out to the network.
