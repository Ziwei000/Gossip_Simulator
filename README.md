# Gossip_Simulator
# Gossip Algorithm
  The Gossip algorithm involves the following:

• Starting: A participant(actor) it told/sent a roumor(fact) by the main process

• Step: Each actor selects a random neighbor and tells it the rumor

• Termination: Each actor keeps track of rumors and how many times it has heard the rumor. It stops transmitting once it has heard the rumor 10 times (10 is arbitrary, you can select other values).

# Push-Sum algorithm for sum computation

• State: Each actor Ai maintains two quantities: s and w. Initially, s = xi = i (that is actor number i has value i, play with other distribution if you so desire) and w = 1

• Starting: Ask one of the actors to start from the main process.

• Receive: Messages sent and received are pairs of the form (s, w). Upon receive, an actor should add received pair to its own corresponding val- ues. Upon receive, each actor selects a random neighboor and sends it a message.

• Send: When sending a message to another actor, half of s and w is kept by the sending actor and half is placed in the message.

• Sum estimate: At any given moment of time, the sum estimate is ws where s and w are the current values of an actor.

• Termination: If an actors ratio ws did not change more than 10−10 in 3 consecutive rounds the actor terminates. WARNING: the values s and w independently never converge, only the ratio does.

# Topologies 
The actual network topology plays a critical role in the dissemi- nation speed of Gossip protocols. As part of this project you have to experiment with various topologies. The topology determines who is considered a neighboor in the above algorithms.

• Full Network Every actor is a neighbor of all other actors. That is, every actor can talk directly to any other actor.

• 2D Grid: Actors form a 2D grid. The actors can only talk to the grid neighbors.

• Line: Actors are arranged in a line. Each actor has only 2 neighbors (one left and one right, unless you are the first or last actor).

• Imperfect 2D Grid: Grid arrangement but one random other neighbor is selected from the list of all actors (4+1 neighbors).

To start, run dotnet fsi project2.fsx numNodes topology algorithm

e.g.

dotnet fsi project2.fsx 2000 full gossip
