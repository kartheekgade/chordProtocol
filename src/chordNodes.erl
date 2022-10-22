-module(chordNodes).

-behaviour(gen_server).

%% API
-export([start_link/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3, startRequests/6]).

-define(SERVER, ?MODULE).

-record(chordNodes_state, {}).

start_link(SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable], []).

init([SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable]) ->
  LookUpTable = fillLookUpTable(NumNodes, LookUpTable, M),
  chordMaster:whereis(SelfID) ! {done, SelfID},
  {ok, [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable]}.

%%----------------fillLookUpTable will fill LookUp Table which is used for finger Table---------------%%%
fillLookUpTable(NumNodes, LookUpTable, M) ->
  LookUpTable:foreach(fun(N) ->
    Node = getHashedID(N, M),
    LookUpTable ++ Node
                      end, lists:seq(1, NumNodes)),
  LookUpTable = lists:sort(LookUpTable).

fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, 1) ->
  Num = (SelfID + math:pow(2, M - 1)) rem trunc(math:pow(2, M)),
  FingerTable =
    if
      Num > HighNode ->
        Temp = lists:filter(fun(Z) -> math:pow(2, M) + Z - Num >= 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - 1, Head);

      Num =< HighNode ->
        Temp = lists:filter(fun(Z) -> (Z - Num) > 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - 1, Head)
    end,
  FingerTable,
  done;

fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, Count) when Count > 1 ->
  Num = (SelfID + trunc(math:pow(2, M - Count))) rem trunc(math:pow(2, M)),
  FingerTable =
    if
      Num > HighNode ->
        Temp = lists:filter(fun(Z) -> math:pow(2, M) + Z - Num >= 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - Count, Head);

      Num =< HighNode ->
        Temp = lists:filter(fun(Z) -> (Z - Num) > 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - Count, Head)
    end,

  fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, Count - 1).

handle_call(_Request, _From, State = #chordNodes_state{}) ->
  {reply, ok, State}.

handle_cast({find_this_node, Rand_node}, State = #chordNodes_state{}) ->
  [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable] = State,
  [LowNode, HighNode] = findInfo(LookUpTable),
  [Successor, Predecessor] = findSucPred(SelfID, LookUpTable, LowNode, HighNode),
  InRange =
    if
      SelfID == LowNode ->
        if Rand_node > HighNode ->
          1;
         Rand_node =< LowNode ->
          1;
          true ->
            0
        end;
      true ->
        if
          Rand_node > Predecessor ->
         if Rand_node =< SelfID ->
            1
          end;
          true ->
            0
        end
  end,
  if
    InRange == 1 ->
%%# IO.puts "#{rand_node} is in range of #{selfID}, sending done"
      chordMaster:whereis(SelfID) ! {done, SelfID};
%%# startRequests(selfID, numNodes, numRequests-1, m, fingerTable, lookUpTable)
    true ->
      case lists:member(Rand_node, maps:iterator(FingerTable)) of
        true -> Node_pid = chordNodes:whereis(Rand_node)
      end,

      case lists:any(maps:filter(FingerTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z < Rand_node end) of
        true ->
          case lists:any(lists:filter(LookUpTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z > SelfID end) of
            true ->
              Temp = lists:filter(lists:sort(maps:values(FingerTable)), fun(Z) -> Z - Rand_node < 0 end),
              Temp1 = lists:reverse(Temp),
              [Head | _] = Temp1,
              Node_pid = chordNodes:whereis(Head);
            false ->
              Node_pid = chordMaster:whereis(Successor)
          end
      end,
      ets:update_counter(counter, "counter", {2, 1}),
      gen_Server:cast(Node_pid, {find_this_node, Rand_node})
  end,
  {noreply, State}.

handle_info({_, fixFingerTable}, State = #chordNodes_state{}) ->

  [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable] = State,
  [LowNode, HighNode] = findInfo(LookUpTable),
  FingerTable = fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, M),

  State = setnth(4, State, FingerTable),
  {noreply, State};

handle_info({wait_for_startrequest}, State) ->
[SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable] = State,
startRequests(SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable),
{noreply, State}.

%%####################################################################################
%%# This function returns the successor and predecessor for the node that is passed in
%%# input: selfID, lookUpTable, lowNode, highNode
%%# calls: none
%%####################################################################################

findSucPred(SelfID, LookUpTable, LowNode, HighNode) ->
  [Successor, Predecessor] =
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
%%---------------------------------------------
setnth(1, [_ | Rest], New) -> [New | Rest],
  done;
setnth(I, [E | Rest], New) when I > 1 -> [E | setnth(I - 1, Rest, New)].

%%---------------------------------------------
findInfo(LookUpTable) ->
  [LowNode | _] = LookUpTable,
  LookUpTable = lists:reverse(LookUpTable),
  [HighNode | _] = LookUpTable,
  [LowNode, HighNode].
%%-----------------------------------------

%%###################################################################
%%# This function starts the nodes requests to other nodes
%%# input: selfID, numNodes, numRequests, m, fingerTable, lookUpTable
%%# calls: findInfo, findSucPred, getHashedID, sendDone, itself
%%###################################################################

startRequests(SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable) when NumRequests =< 1 ->
%%# IO.inspect fingerTable, label: "Node #{selfID}'s FT:"
%%# IO.inspect lookUpTable, label: "Node #{selfID}'s LT:"
  [LowNode, HighNode] = findInfo(LookUpTable),
  [Successor, Predecessor] = findSucPred(SelfID, LookUpTable, LowNode, HighNode),
  Rand_node = rand:uniform(math:pow(2, M)),
%%# IO.puts "node: #{selfID} -> random node: #{rand_node}"
  InRange =
    if
      SelfID == LowNode ->
        if 
          Rand_node > HighNode ->
            1;
            Rand_node =< LowNode ->
          1;
          true ->
            0
        end;
      true ->
        if
          Rand_node > Predecessor ->
            if Rand_node =< SelfID ->
            1
            end;
          true ->
            0
        end
    end,
  if
    InRange == 1 ->
%%# IO.puts "#{rand_node} is in range of #{selfID}, sending done"
      chordMaster:whereis(SelfID) ! {done, SelfID};
%%# startRequests(selfID, numNodes, numRequests-1, m, fingerTable, lookUpTable)
    true ->
      case lists:member(Rand_node, maps:iterator(FingerTable)) of
        true -> Node_pid = chordNodes:whereis(Rand_node)
      end,

      case lists:any(maps:filter(FingerTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z < Rand_node end) of
        true ->
          case lists:any(lists:filter(LookUpTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z > SelfID end) of
            true ->
              Temp = lists:filter(lists:sort(maps:values(FingerTable)), fun(Z) -> Z - Rand_node < 0 end),
              Temp1 = lists:reverse(Temp),
              [Head | _] = Temp1,
              Node_pid = chordNodes:whereis(Head);
            false ->
              Node_pid = chordMaster:whereis(Successor)
          end
      end,
      ets:update_counter(counter, "counter", {2, 1}),
      gen_Server:cast(Node_pid, {find_this_node, Rand_node})
  end,
  done;


startRequests(SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable) ->
%%# IO.inspect fingerTable, label: "Node #{selfID}'s FT:"
%%# IO.inspect lookUpTable, label: "Node #{selfID}'s LT:"
  [LowNode, HighNode] = findInfo(LookUpTable),
  [Successor, Predecessor] = findSucPred(SelfID, LookUpTable, LowNode, HighNode),
  Rand_node = rand:uniform(math:pow(2, M)),
%%# IO.puts "node: #{selfID} -> random node: #{rand_node}"
  InRange =
    if
      SelfID == LowNode ->
        if Rand_node > HighNode -> 
          1;
          Rand_node =< LowNode ->
          1;
          true ->
            0
        end;
      true ->
        if
          Rand_node > Predecessor ->
            if  Rand_node =< SelfID ->
            1
            end;
          true ->
            0
        end
    end,
  if
    InRange == 1 ->
%%# IO.puts "#{rand_node} is in range of #{selfID}, sending done"
      chordMaster:whereis(SelfID) ! {done, SelfID};
%%# startRequests(selfID, numNodes, numRequests-1, m, fingerTable, lookUpTable)
    true ->
      case lists:member(Rand_node, maps:iterator(FingerTable)) of
        true -> Node_pid = chordNodes:whereis(Rand_node)
      end,

      case lists:any(lists:filter(FingerTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z < Rand_node end) of
        true ->
          case lists:any(lists:filter(LookUpTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z > SelfID end) of
            true ->
              Temp = lists:filter(lists:sort(map:values(FingerTable)), fun(Z) -> Z - Rand_node < 0 end),
              Temp1 = lists:reverse(Temp),
              [Head | _] = Temp1,
              Node_pid = chordNodes:whereis(Head);
            false ->
              Node_pid = chordMaster:whereis(Successor)
          end
      end,
      ets:update_counter(counter, "counter", {2, 1}),
      gen_Server:cast(Node_pid, {find_this_node, Rand_node}),
      startRequests(SelfID, NumNodes, NumRequests - 1, M, FingerTable, LookUpTable)
  end.
%%----------------------------------
%%--------------- Hashed key ---------------------%%
getHashedID(I,M) ->
  KeyGen = integer_to_list(I),
  httpd_util:hexlist_to_integer(io_lib:format("~64.16.0b", [binary:decode_unsigned(crypto:hash(sha256, KeyGen))])) rem trunc(math:pow(2,M)).

terminate(_Reason, _State = #chordNodes_state{}) ->
  ok.

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #chordNodes_state{},
    Extra :: term()) ->
  {ok, NewState :: #chordNodes_state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #chordNodes_state{}, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
