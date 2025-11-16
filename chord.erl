-module(chord).
-export([create_p2p_network/2, node/4, listen_task_completion/2, main/2]).


randomNode(Node_id, []) -> Node_id;
randomNode(_, ExistingNodes) -> lists:nth(rand:uniform(length(ExistingNodes)), ExistingNodes).


get_forward_distance(Key, Key, _, Distance) ->
    Distance;
get_forward_distance(Key, NodeId, M, Distance) ->
    get_forward_distance(Key, (NodeId + 1) rem trunc(math:pow(2, M)), M, Distance + 1)
.

get_closest(_, [], MinimumDistNode, _, _) ->
    MinimumDistNode;
get_closest(Key, FingerNodeIds, MinimumDistNode, MinimumVal, State) ->
    [First| Rest] = FingerNodeIds,
    Distance = get_forward_distance(Key, First, dict:fetch(m, State), 0),
    if
        Distance < MinimumVal ->
            get_closest(Key, Rest, First, Distance, State);
        true -> 
            get_closest(Key, Rest, MinimumDistNode, MinimumVal, State)
    end
.

get_closest_node(Key, FingerNodeIds, State) ->
    case lists:member(Key, FingerNodeIds) of
        true -> Key;
        _ -> get_closest(Key, FingerNodeIds, -1, 10000000, State)
    end

.

listen_on_node(NodeState) ->
    Hash = dict:fetch(id, NodeState),
    receive
        {finger_table, FingerTable} -> 
            % io:format("Received Finger for ~p ~p", [Hash, FingerTable]),
            UpdatedState = dict:store(finger_table, FingerTable, NodeState);
            
        {lookup, Id, Key, HopsCount, _} ->

                NodeVal = get_closest_node(Key, dict:fetch_keys(dict:fetch(finger_table ,NodeState)), NodeState),
                UpdatedState = NodeState,
                %io:format("Lookup::: ~p  For Key ~p  ClosestNode ~p ~n", [Hash, Key, NodeVal]),
                if 
                    
                    (Hash == Key) -> 
                        taskcompletionmonitor ! {completed, Hash, HopsCount, Key};
                    (NodeVal == Key) and (Hash =/= Key) -> 
                        taskcompletionmonitor ! {completed, Hash, HopsCount, Key};
                    
                    true ->
                        dict:fetch(NodeVal, dict:fetch(finger_table, NodeState)) ! {lookup, Id, Key, HopsCount + 1, self()}
                end
                ;
        {kill} ->
            UpdatedState = NodeState,
            exit("received exit signal");
        {state, Pid} -> Pid ! NodeState,
                        UpdatedState = NodeState;
        {stabilize, _} -> 
                                UpdatedState = NodeState
    end, 
    listen_on_node(UpdatedState).

node(Hash, M, ChordNodes, _) -> 
    io:format("Node is spawned with hash ~p",[Hash]),
    FingerTable = lists:duplicate(M, randomNode(Hash, ChordNodes)),
    NodeStateUpdated = dict:from_list([{id, Hash}, {predecessor, nil}, {finger_table, FingerTable}, {next, 0}, {m, M}]),
    listen_on_node(NodeStateUpdated)        
.


get_m(NumberOfNodes) ->
    trunc(math:ceil(math:log2(NumberOfNodes)))
.

get_node_pid(Hash, NetworkState) -> 
    case dict:find(Hash, NetworkState) of
        error -> nil;
        _ -> dict:fetch(Hash, NetworkState)
    end
.

add_node_to_chord(ChordNodes, TotalNodes, M, NetworkState) ->
    RemainingHashes = lists:seq(0, TotalNodes - 1, 1) -- ChordNodes,
    Hash = lists:nth(rand:uniform(length(RemainingHashes)), RemainingHashes),
    Pid = spawn(chord, node, [Hash, M, ChordNodes, dict:new()]),
    io:format("~n ~p ~p ~n", [Hash, Pid]),
    [Hash, dict:store(Hash, Pid, NetworkState)]
.


listen_task_completion(0, HopsCount) ->
    main ! {totalhops, HopsCount}
;

listen_task_completion(NumberOfRequestss, HopsCount) ->
    receive 
        {completed, _, HopsCountForTask, _} ->
            listen_task_completion(NumberOfRequestss - 1, HopsCount + HopsCountForTask)
    end
.

send_message_to_node(_, [], _) ->
    ok;
send_message_to_node(Key, ChordNodes, NetworkState) ->
    [First | Rest] = ChordNodes,
    Pid = get_node_pid(First, NetworkState),
    Pid ! {lookup, First, Key, 0, self()},
    send_message_to_node(Key, Rest, NetworkState)
.


send_messages_all_nodes(_, 0, _, _) ->
    ok;
send_messages_all_nodes(ChordNodes, NumberOfRequests, M, NetworkState) ->
    timer:sleep(1000),
    Key = lists:nth(rand:uniform(length(ChordNodes)), ChordNodes),
    send_message_to_node(Key, ChordNodes, NetworkState),
    send_messages_all_nodes(ChordNodes, NumberOfRequests - 1, M, NetworkState)
.

kill_all_nodes([], _) ->
    ok;
kill_all_nodes(ChordNodes, NetworkState) -> 
    [First | Rest] = ChordNodes,
    get_node_pid(First, NetworkState) ! {kill},
    kill_all_nodes(Rest, NetworkState).

getTotalHops() ->
    receive
        {totalhops, HopsCount} ->
            HopsCount
        end.


send_messages_and_kill(ChordNodes, NumberOfNodes, NumberOfRequests, M, NetworkState) ->
    register(taskcompletionmonitor, spawn(chord, listen_task_completion, [NumberOfNodes * NumberOfRequests, 0])),

    send_messages_all_nodes(ChordNodes, NumberOfRequests, M, NetworkState),

    TotalHops = getTotalHops(),

    io:format("~n Average Hops = ~p  Total hops = ~p  Total requests = ~p ~n", [TotalHops/(NumberOfNodes * NumberOfRequests), TotalHops, NumberOfNodes * NumberOfRequests]),
    kill_all_nodes(ChordNodes, NetworkState)
.

stabilize_network(ChordNodes, NetworkState) ->
    Pid = get_node_pid(lists:nth(rand:uniform(length(ChordNodes)), ChordNodes), NetworkState),

    case Pid of
        nil -> stabilize_network(ChordNodes, NetworkState);
        _ -> Pid ! {stabilize, NetworkState}
    end
.


create_network_nodes(ChordNodes, _, _, 0, NetworkState) -> 
    [ChordNodes, NetworkState];
create_network_nodes(ChordNodes, TotalNodes, M, NumberOfNodes, NetworkState) ->
    [Hash, NewNetworkState] = add_node_to_chord(ChordNodes, TotalNodes,  M, NetworkState),
    create_network_nodes(lists:append(ChordNodes, [Hash]), TotalNodes, M, NumberOfNodes - 1, NewNetworkState)
.


get_ith_successor(_, _, I , I, CurID, _) ->
    CurID;

get_ith_successor(Hash, NetworkState, I, Cur, CurID, M) -> 
    case dict:find((CurID + 1) rem trunc(math:pow(2, M)), NetworkState) of
        error ->
             get_ith_successor(Hash, NetworkState, I, Cur, (CurID + 1) rem trunc(math:pow(2, M)),M);
        _ -> get_ith_successor(Hash, NetworkState, I, Cur + 1, (CurID + 1) rem trunc(math:pow(2, M)),M)
    end
.

get_finger_table(_, _, M, M,FingerList) ->
    FingerList;
get_finger_table(Node, NetworkState, M, I, FingerList) ->
    Hash = element(1, Node),
    Ith_succesor = get_ith_successor(Hash, NetworkState, trunc(math:pow(2, I)), 0, Hash, M),
    get_finger_table(Node, NetworkState, M, I + 1, FingerList ++ [{Ith_succesor, dict:fetch(Ith_succesor, NetworkState)}] )
.


collectfingertables(_, [], FTDict,_) ->
    FTDict;

collectfingertables(NetworkState, NetList, FTDict,M) ->
    [First | Rest] = NetList,
    FingerTables = get_finger_table(First, NetworkState,M, 0,[]),
    collectfingertables(NetworkState, Rest, dict:store(element(1, First), FingerTables, FTDict), M)
.



send_finger_tables_nodes([], _, _) ->
    ok;
send_finger_tables_nodes(NodesToSend, NetworkState, FingerTables) ->
    [First|Rest] = NodesToSend,
    Pid = dict:fetch(First ,NetworkState),
    Pid ! {finger_table, dict:from_list(dict:fetch(First, FingerTables))},
    send_finger_tables_nodes(Rest, NetworkState, FingerTables)
.

send_finger_tables(NetworkState,M) ->
    FingerTables = collectfingertables(NetworkState, dict:to_list(NetworkState), dict:new(),M),
    io:format("~n~p~n", [FingerTables]),
    send_finger_tables_nodes(dict:fetch_keys(FingerTables), NetworkState, FingerTables)
.

create_p2p_network(NumberOfNodes, NumberOfRequests) ->
    M = get_m(NumberOfNodes),
    [ChordNodes, NetworkState] = create_network_nodes([], round(math:pow(2, M)), M, NumberOfNodes, dict:new()),
    
    send_finger_tables(NetworkState,M),
    
    stabilize_network(ChordNodes, NetworkState),
    send_messages_and_kill(ChordNodes, NumberOfNodes, NumberOfRequests, M, NetworkState)
.



main(NumberOfNodes, NumberOfRequests) ->
    register(main, spawn(chord, create_p2p_network, [NumberOfNodes, NumberOfRequests]))
.


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
      ets:new(counter, [set, public, named_table]),
      ets:insert(counter,{"counter", 0}),
      createNodes(NumNodes, NumNodes, NumRequests, M)

  end.

getHashedID(I,M) ->
  KeyGen = integer_to_list(I),
  httpd_util:hexlist_to_integer(io_lib:format("~64.16.0b", [binary:decode_unsigned(crypto:hash(sha256, KeyGen))])) rem trunc(math:pow(2,M)).

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

  findSucPred(SelfID, LookUpTable, LowNode, HighNode) ->
      if 
        SelfID == HighNode -> Temp = lists:filter(LookUpTable, fun(Z) -> Z < SelfID end),
          Temp1 = lists:reverse(Temp),
          [Head | _] = Temp1,
          [LowNode, Head];
        SelfID == LowNode -> Temp = lists:filter(LookUpTable, fun(Z) -> Z > SelfID end),
          [Head | _] = Temp,
          [Head, HighNode];
        true -> Temp = lists:filter(LookUpTable, fun(Z) -> Z > SelfID end),
          [Head | _] = Temp,
          Temp1 = lists:filter(LookUpTable, fun(Z) -> Z < SelfID end),
          Temp2 = lists:reverse(Temp1),
          [Head1 | _] = Temp2,
          [Head, Head1]
      end.