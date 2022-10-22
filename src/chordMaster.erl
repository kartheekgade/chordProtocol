-module(chordMaster).

%% API

-export([start/2, createNodes/4, wait_for_all_nodes/1, startRequests/2, fixFinger/2]).

start(NumNodes,NumRequests) ->
  if
    NumNodes == 1 ->
      io:fwrite("Total number of hops is 0");
    NumNodes < 1 ->
      io:fwrite("Total number of hops is 0");
    NumRequests < 1 ->
      io:fwrite("Total number of hops is 0");
    NumNodes > 1 ->
      M = 128,

      gobal:register_name(master,self()),
      ets:new(counter, [set, public, named_table]),
      ets:insert(counter,{"counter", 0}),
      createNodes(NumNodes, NumNodes, NumRequests, M),
      chord_logic(NumNodes, M, NumRequests)

  end.

%%---------------- Create Nodes ---------------%%
createNodes(1, NumNodes, NumRequests, M) ->
  SelfID = getHashedID(1, M),
  Pid = spawn(chordNodes, start_link, [SelfID, NumNodes, NumRequests, M, #{}, []]),
  register(SelfID, Pid),
 done;

createNodes(Count, NumNodes, NumRequests, M) when Count>1->
  SelfID = getHashedID(Count, M),
  Pid = spawn(chordNodes, start_link, [SelfID, NumNodes, NumRequests, M, #{}, []]),
  register(SelfID, Pid),
  createNodes(Count-1, NumNodes, NumRequests, M).
%%-------------------------------------------------%%

%%--------------- chord_logic -------------------%%
%%-- This function schedules the various task sequentially that the node has to perform --%%
chord_logic(NumNodes, M, NumRequests) ->
  wait_for_all_nodes(NumNodes),
  fixFinger(NumNodes, M),
  wait_for_all_nodes(NumNodes),
  startRequests(M, NumNodes),
%%times_received = NumNodes * NumRequests,
  wait_for_all_nodes(NumNodes*NumRequests),
  calculateAvgHops(NumNodes, NumRequests).
%%--------------------------------------------------%%%

%%--------------- Hashed key ---------------------%%
getHashedID(I,M) ->
  KeyGen = integer_to_list(I),
  httpd_util:hexlist_to_integer(io_lib:format("~128.16.0b", [binary:decode_unsigned(crypto:hash(sha256, KeyGen))])) rem math:pow(2,M).
%%-------------------------------------------------%%

%%--------------- This function makes the master wait till all the nodes send 'done' ---------------%%
wait_for_all_nodes(1)->
  receive
    {done,_} -> nil
  end,
  done;

wait_for_all_nodes(NumNodes) when NumNodes > 1 ->
  receive
    {done, _} -> nil
  end,
  wait_for_all_nodes(NumNodes - 1).
%%-----------------------------------------%%

%%----- This function gives the command to the nodes to fill their finger tables ------%%
fixFinger(1, M) ->
  NodeID = getHashedID(0, M),
  Node_pid = whereis(NodeID),
  node_pid ! {Node_pid, fixFingerTable},
  done;
fixFinger(NumNodes, M) when NumNodes > 1 ->
  NodeID = getHashedID(NumNodes, M),
  Node_pid = whereis(NodeID),
  node_pid ! {Node_pid, fixFingerTable},
  fixFinger(NumNodes - 1, M).
%%----------------------------------------------%%

%%---- This function issues message to the nodes to start sending requests to other nodes -----%%
startRequests(M, 1) ->
  NodeID = getHashedID(1, M),
  Node_pid = whereis(NodeID),
  Node_pid ! {wait_for_startrequest},
  done;
startRequests(M, NumNodes) when NumNodes > 1->
  NodeID = getHashedID(1, M),
  Node_pid = whereis(NodeID),
  Node_pid ! {wait_for_startrequest},
  startRequests(M, NumNodes-1).
%%----------------------------------------------%%

%%----- This function takes the total hops value from the counter and calculates the average hops for the whole network ---%%
calculateAvgHops(NumNodes, NumRequests) ->
  io:format("calculating the average hops for the whole network"),
  Val = match_spec:match(counter, {"$1", "$2"}),
  [Head | _] = Val,
  [_, Count] = Head,
  TotAvgHop = Count / (NumNodes * NumRequests),
  io:format("The total average hops taken by all the nodes in the network is ~p", TotAvgHop).
%%----------------------------------------------%%







