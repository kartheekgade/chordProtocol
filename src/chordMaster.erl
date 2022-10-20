%%%-------------------------------------------------------------------
%%% @author kartheek
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 15. Oct 2022 8:26 pm
%%%-------------------------------------------------------------------
-module(chordMaster).
-author("kartheek").

%% API
-export([start/2, createNodes/4, createNodes/4, fillLookUpTable/3, wait_for_all_nodes/1, startRequests/2, fixFinger/2]).

start(NumNodes,NumRequests) ->
  if
    NumNodes == 1 ->
      io:fwrite("Total number of hops is 0");
    NumNodes < 1 ->
      io:fwrite("Total number of hops is 0");
    NumRequests < 1 ->
      io:fwrite("Total number of hops is 0");
    NumNodes > 1 ->
      m = 128,

      gobal:register_name(master,self()),
      ets:new(counter, [set, public, named_table]),
      ets:insert(counter,{"counter", 0}),
      createNodes(NumNodes, NumNodes, NumRequests, m),
      chord_logic(NumNodes, m, NumRequests)

  end.

%%---------------- Create Nodes ---------------%%
createNodes(1, NumNodes, NumRequests, m) ->
  selfID = getHashedID(count, m),
  Pid = spawn(chordMaster, fillLookUpTable, [1,NumNodes,NumRequests,m]),
  register(selfID, Pid),
 done;

createNodes(Count, NumNodes, NumRequests, m) when Count>1->
  selfID = getHashedID(count, m),
  Pid = spawn(chordMaster, fillLookUpTable, [NumNodes,#{},m]),
  register(selfID, Pid),
  createNodes(Count-1, NumNodes, NumRequests, m).
%%-------------------------------------------------%%

%%--------------- chord_logic -------------------%%
%%-- This function schedules the various task sequentially that the node has to perform --%%
chord_logic(NumNodes, m, NumRequests) ->
  wait_for_all_nodes(NumNodes),
  fixFinger(NumNodes, m),
  wait_for_all_nodes(NumNodes),
  startRequests(m, NumNodes),
%%times_received = NumNodes * NumRequests,
  wait_for_all_nodes(NumNodes*NumRequests),
  calculateAvgHops(NumNodes, NumRequests).
%%--------------------------------------------------%%%

%%--------------- Hashed key ---------------------%%
getHashedID(i,m) ->
  KeyGen = integer_to_list(i),
  hexlist_to_integer(io_lib:format("~128.16.0b", [binary:decode_unsigned(crypto:hash(sha256, KeyGen))])) rem pow(2,m).
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
fixFinger(1, m) ->
  NodeID = getHashedID(0, m),
  Node_pid = whereis(NodeID),
  node_pid ! {Node_pid, fixFingerTable},
  done;
fixFinger(NumNodes, m) when NumNodes > 1 ->
  NodeID = getHashedID(NumNodes, m),
  Node_pid = whereis(NodeID),
  node_pid ! {Node_pid, fixFingerTable},
  fixFinger(NumNodes - 1, m).
%%----------------------------------------------%%

%%---- This function issues message to the nodes to start sending requests to other nodes -----%%
startRequests(m, 1) ->
  NodeID = getHashedID(1, m),
  Node_pid = whereis(NodeID),
  Node_pid ! {wait_for_startrequest},
  done;
startRequests(m, NumNodes) when NumNodes > 1->
  NodeID = getHashedID(1, m),
  Node_pid = whereis(NodeID),
  Node_pid ! {wait_for_startrequest},
  startRequests(m, NumNodes-1).
%%----------------------------------------------%%

%%----- This function takes the total hops value from the counter and calculates the average hops for the whole network ---%%
calculateAvgHops(NumNodes, NumRequests) ->
  io:format("calculating the average hops for the whole network"),
  Val = match(counter, {"$1", "$2"}),
  [Head | _] = Val,
  [_, Count] = Head,
  TotAvgHop = Count / (NumNodes * NumRequests),
  io:format("The total average hops taken by all the nodes in the network is ~p", TotAvgHop).
%%----------------------------------------------%%

%%----------------fillLookUpTable will fill LookUp Table which is used for finger Table---------------%%%
fillLookUpTable(NumNodes,LookUpTable,m) ->
  LookUpTable:foreach(fun(N)->
    Node = getHashedID(N,m),
    LookUpTable ++ Node
    end, lists:seq(1,NumNodes)).
%%----------------------------------------------------------------------------------------------------%%%





