#load "Reference.fsx"
#time "on"
open System
open System.Collections.Generic
open Akka
open Akka.Actor
open Akka.FSharp
open Akka.Configuration
open Akka.TestKit
open System.Diagnostics
let system = System.create "system" (Configuration.defaultConfig())
let sleep (ms : int) = System.Threading.Thread.Sleep(ms)
let timer = Stopwatch()
//timer.Start()
type Command =
    | BOSS_Start of List<string>
    | ACTList of List<string> * int
    | Gossip
    | SW of double * double
    | Continue
    | G_Conv
    | G_Rec
    | SW_Conv
    | SW_Rec
let In_numNodes = fsi.CommandLineArgs.[1] |> int
let In_topo = fsi.CommandLineArgs.[2] |> string
let In_algo = fsi.CommandLineArgs.[3] |> string

let gossipactor (mailbox:Actor<_>) =
    let neighbor_list:List<string> = List<string>()
    let mutable boss = ""
    let mutable state = 0
    let mutable g_counter = 0
    let mutable myid = 0
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        match msg with
        | ACTList(sendlist,ith) -> for i in sendlist do
                                      neighbor_list.Add(i)
                                   myid <- ith
                                   boss <- string(sender.Path)
                                   //printfn "received %A %s"neighbor_list boss
                                   return! loop()
        | Gossip -> if string(sender.Path) <> boss then
                        sender <! Continue
                    if state = 0 then
                        mailbox.Context.ActorSelection(boss).Tell(G_Rec)
                        state <- 1     
                    g_counter <- g_counter + 1
                    if g_counter < 10 then
                        let r_node = Random()
                        let ii = r_node.Next(neighbor_list.Count)
                        mailbox.Context.ActorSelection(neighbor_list.[ii]).Tell(Gossip)
                        return! loop()
                    else
                       mailbox.Context.ActorSelection(boss).Tell(G_Conv)
                      // printfn "worker%i has heard 10 gossip, stop transmitting..."myid
        | Continue ->  let r_node = Random()
                       let ii = r_node.Next(neighbor_list.Count)
                       mailbox.Context.ActorSelection(neighbor_list.[ii]).Tell(Gossip)
                       return! loop()
        | _ -> return! loop()        
    }
    loop ()
let pushsumactor (mailbox:Actor<_>) =
    let neighbor_list:List<string> = List<string>()
    let mutable boss = ""
    let mutable s : double = 1.0
    let mutable w : double = 1.0
    let mutable swratio : double = 0.0
    let mutable unchange = 0
    let mutable myid = 0
    let mutable state = 0
    let rec loop() = actor{
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()
        match msg with
        | ACTList(neighbor,ith) -> for i in neighbor do
                                      neighbor_list.Add(i)
                                     // printfn "%s" i
                                   myid <- ith + 1
                                   boss <- string(sender.Path)
                                   s <- double(myid)
                                   swratio <- s/w
                                  // printfn "received %A %s"neighbor_list boss
                                   return! loop()
        | SW(x , y) -> //if state = 0 then
                         // mailbox.Context.ActorSelection(boss).Tell(SW_Rec)
                       if string(sender.Path) <> boss then
                          sender <! Continue
                       else  //if x = 0.0 && y = 0.0 then
                          //sw from boss 
                          let r_node = Random()
                          let ii = r_node.Next(neighbor_list.Count)
                          state <- 1
                          mailbox.Context.ActorSelection(neighbor_list.[ii]).Tell(SW(s/2.0 , w/2.0))
                          s <- s / 2.0
                          w <- w / 2.0
                          return! loop() 
                       if state = 0 then
                           state <- 1
                       s <- s + x
                       w <- w + y
                       let tmp = swratio
                       swratio <- s/w
                       let diff = (swratio-tmp)/swratio
                       if diff < 10e-10 && diff > -10e-10 then
                           if unchange = 0 then
                               mailbox.Context.ActorSelection(boss).Tell(SW_Rec)
                               printfn "worker%i 's ratio unchanged..."myid
                           unchange <- unchange + 1
                       if unchange < 3 then
                           let r_node = Random()
                           let ii = r_node.Next(neighbor_list.Count)
                           mailbox.Context.ActorSelection(neighbor_list.[ii]).Tell(SW(s/2.0 , w/2.0))
                           s <- s / 2.0
                           w <- w / 2.0
                           //sleep(1)
                           return! loop()
                       else
                          mailbox.Context.ActorSelection(boss).Tell(SW_Conv)
                          //printfn "worker%i has stay unchanged for 3 times, stop transmitting..."myid
        | Continue -> let r_node = Random()
                      let ii = r_node.Next(neighbor_list.Count)
                      mailbox.Context.ActorSelection(neighbor_list.[ii]).Tell(SW(s/2.0 , w/2.0))
                      s <- s / 2.0
                      w <- w / 2.0
                      return! loop()             
        | _ -> return! loop()
    }
    loop()

let getActorList numnodes algo =
    let actorList = List<string>()
    for i = 0 to numnodes - 1 do
        let w_props = "worker" + string(i)
        if algo = "gossip" then
            let g_actor = spawn system w_props <| gossipactor
            let g_path = string(g_actor.Path)
            actorList.Add(g_path)
        else 
            let ps_actor = spawn system w_props <| pushsumactor
            let ps_path = string(ps_actor.Path)
            actorList.Add(ps_path)
    actorList
    
let getNeighbor topo actornode (actorList:List<string>) =
    let n_list = List<string>()
    let numnodes = actorList.Count
    match topo with
    | "full" -> for i in 0..numnodes-1 do
                    if i <> actornode then
                        n_list.Add(actorList.[i])
    | "line" -> if actornode + 1 <= numnodes - 1 then
                    n_list.Add(actorList.[actornode + 1])
                if actornode - 1 >= 0 then
                    n_list.Add(actorList.[actornode - 1])
    | "2D" -> let n_row = int(floor(sqrt(float(numnodes))))
              if actornode >= n_row then
                  let up = actornode - n_row
                  n_list.Add(actorList.[up])
              if actornode <= numnodes - n_row - 1 then
                  let bot = actornode + n_row
                  n_list.Add(actorList.[bot])
              if actornode % n_row > 0 then
                  let left = actornode - 1
                  n_list.Add(actorList.[left])
              if (actornode + 1) % n_row > 0 && actornode < numnodes - 2 then
                  let right = actornode + 1
                  n_list.Add(actorList.[right])        
    | "imp2D" -> let n_row = int(floor(sqrt(float(numnodes))))
                 let mutable up = -1
                 let mutable bot = -1
                 let mutable left = -1
                 let mutable right = -1
                 if actornode >= n_row then
                    up <- actornode - n_row
                    n_list.Add(actorList.[up])
                 if actornode <= numnodes - n_row - 1 then
                    bot <- actornode + n_row
                    n_list.Add(actorList.[bot])
                 if actornode % n_row > 0 then
                    left <- actornode - 1
                    n_list.Add(actorList.[left])
                 if (actornode + 1) % n_row > 0 && actornode < numnodes - 2 then
                    right <- actornode + 1
                    n_list.Add(actorList.[right])
                 let r_node = Random()
                 let mutable ii = r_node.Next(numnodes)
                 while (ii = up) || (ii = bot) || (ii = left) || (ii = right) do
                     ii <- r_node.Next(numnodes)
                 n_list.Add(actorList.[ii])
    | _ -> ()
    n_list
let boss (mailbox: Actor<_>) =
    let mutable workerNum = In_numNodes
    let mutable coef = 1.0
    let mutable workerConv = workerNum// int(ceil(float(workerNum) * coef))
    let mutable workerConv0 = workerNum
    let mutable actorlist:List<string> = List<string>()
    let rec loop() = actor {
        let! msg = mailbox.Receive()
        let sender = mailbox.Sender()        
        match msg with
        | BOSS_Start(al)-> timer.Start()
                           actorlist <- al
                           for i in 0..actorlist.Count - 1 do
                               let neighborlist = getNeighbor In_topo i actorlist
                               mailbox.Context.ActorSelection(actorlist.[i]).Tell(ACTList(neighborlist , i))
                           //sleep(1)
                           let r_node = Random()
                           let ii = r_node.Next(0 , actorlist.Count - 1)
                           if In_algo = "gossip" then
                               mailbox.Context.ActorSelection(actorlist.[ii]).Tell(Gossip)
                               if In_topo = "line" then coef <- 0.2
                               else
                                   coef <- 0.9
                               workerConv <- int(ceil(float(workerNum) * coef))
                               return! loop()
                           else// pushsum
                               mailbox.Context.ActorSelection(actorlist.[ii]).Tell(SW(0.0 , 0.0))
                               coef <- 0.05
                               workerConv <- int(ceil(float(workerNum) * coef))
                               workerConv0 <- workerConv
                               if workerNum >= 400 then
                                   workerConv0 <- 5
                               elif workerNum < 400 && workerNum >= 50 then
                                   workerConv0 <- 3
                               else
                                   workerConv0 <- 1
                               return! loop()
        | G_Rec -> workerNum <- workerNum - 1
                    //printfn"now there are %i workers haven't heard the rumor" workerNum
                   if workerNum < 1 then
                        printfn "all people have heard gossip, gossip*converged.... %d ms" timer.ElapsedMilliseconds
                   else
                        return! loop()
        | G_Conv -> workerConv <- workerConv - 1
                    if workerConv < 1  then
                        printfn "gossip converged ... %d ms" timer.ElapsedMilliseconds
                    else
                        return! loop()          
        | SW_Rec -> workerConv0 <- workerConv0 - 1
                    if workerConv0 < 1 then
                        printfn "some worker start to have unchanged ratio, pushsum*converged.... %d ms" timer.ElapsedMilliseconds
                    else
                        return! loop()
        | SW_Conv -> workerConv <- workerConv - 1
                     if workerConv < 1 then
                         printfn "pushsum converged ... %d ms" timer.ElapsedMilliseconds
                     else
                         return! loop()
                     
        
        | _ -> return! loop()        
    }
    loop()
let AL = getActorList In_numNodes In_algo
let Boss = spawn system "Boss" boss
Boss <! BOSS_Start(AL)
//system.WhenTerminated.Wait()
Console.ReadLine() |> ignore