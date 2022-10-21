%%%-------------------------------------------------------------------
%%% @author kartheek
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 19. Oct 2022 9:00 pm
%%%-------------------------------------------------------------------
-module(chordNodes).
-author("kartheek").

-behaviour(gen_server).

%% API
-export([start_link/6]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
  code_change/3, startRequests/6]).

-define(SERVER, ?MODULE).

-record(chordNodes_state, {}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
  {ok, State :: #chordNodes_state{}} | {ok, State :: #chordNodes_state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable]) ->
  LookUpTable = fillLookUpTable(NumNodes, LookUpTable, M),
  {ok, [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable]}.

%%----------------fillLookUpTable will fill LookUp Table which is used for finger Table---------------%%%
fillLookUpTable(NumNodes, LookUpTable, M) ->
  LookUpTable:foreach(fun(N) ->
    Node = getHashedID(N, M),
    LookUpTable ++ Node
                      end, lists:seq(1, NumNodes)),
  LookUpTable = sort(LookUpTable).
%%----------------------------------------------------------------------------------------------------%%%
%%%%-----------------------
%%# This function fills the finger table of the node by referring the lookUpTable
%%# Input: selfID, numRequests, m, fingerTable, lookUpTable, lowNode, highNode, count
%%# calls: itself
%%%%--------------------------

fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, 1) ->
  Num = (SelfID + pow(2, M - 1)) rem pow(2, M),
  fingerTable =
    if
      Num > HighNode ->
        Temp = filter(fun(Z) -> pow(2, M) + Z - Num >= 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - 1, Head);

      Num =< HighNode ->
        Temp = filter(fun(Z) -> (Z - Num) > 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - 1, Head)
    end,
  fingerTable,
  done;

fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, Count) when Count > 1 ->
  Num = (SelfID + pow(2, M - Count)) rem pow(2, M),
  fingerTable =
    if
      Num > HighNode ->
        Temp = filter(fun(Z) -> pow(2, M) + Z - Num >= 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - Count, Head);

      Num =< HighNode ->
        Temp = filter(fun(Z) -> (Z - Num) > 0, LookUpTable end),
        [Head | _] = Temp,
        maps:put(FingerTable, M - Count, Head)
    end,

  fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, Count - 1).
%%------------------------------------------------------------------------------------%%%

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #chordNodes_state{}) ->
  {reply, Reply :: term(), NewState :: #chordNodes_state{}} |
  {reply, Reply :: term(), NewState :: #chordNodes_state{}, timeout() | hibernate} |
  {noreply, NewState :: #chordNodes_state{}} |
  {noreply, NewState :: #chordNodes_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #chordNodes_state{}} |
  {stop, Reason :: term(), NewState :: #chordNodes_state{}}).
handle_call(_Request, _From, State = #chordNodes_state{}) ->
  {reply, ok, State}.
%%------------------------------------------------
%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #chordNodes_state{}) ->
  {noreply, NewState :: #chordNodes_state{}} |
  {noreply, NewState :: #chordNodes_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #chordNodes_state{}}).
handle_cast({find_this_node, Rand_node}, State = #chordNodes_state{}) ->
  [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable] = State,
  [LowNode, HighNode] = findInfo(LookUpTable),
  [Successor, Predecessor] = findSucPred(SelfID, LookUpTable, LowNode, HighNode),
  InRange =
    if
      SelfID == LowNode ->
        if Rand_node > HighNode or Rand_node =< LowNode ->
          1;
          true ->
            0
        end;
      true ->
        if
          Rand_node > Predecessor and Rand_node =< SelfID ->
            1;
          true ->
            0
        end
    end,
  if
    InRange == 1 ->
%%# IO.puts "#{rand_node} is in range of #{selfID}, sending done"
      sendDone(selfID);
%%# startRequests(selfID, numNodes, numRequests-1, m, fingerTable, lookUpTable)
    true ->
      case member(Rand_node, maps:iterator(FingerTable)) of
        true -> Node_pid = chordNodes:whereis(Rand_node)
      end,

      case any(filter(FingerTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z < Rand_node end) of
        true ->
          case any(filter(LookUpTable, fun(Z) -> Z < Rand_node end), fun(z) -> z > selfID end) of
            true ->
              Temp = filter(sort(values(FingerTable)), fun(Z) -> Z - Rand_node < 0 end),
              Temp1 = reverse(Temp),
              [Head | _] = Temp1,
              Node_pid = chordNodes:whereis(Head);
            false ->
              Node_pid = chordMaster:whereis(successor)
          end
      end,
      update_counter(counter, "counter", {2, 1}),
      gen_Server:cast(node_pid, {find_this_node, rand_node})
  end,
  {noreply, State}.

%%----------------------------------------------
%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #chordNodes_state{}) ->
  {noreply, NewState :: #chordNodes_state{}} |
  {noreply, NewState :: #chordNodes_state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #chordNodes_state{}}).
handle_info({wait_for_startrequest}, State = #chordNodes_state{}) ->

  [SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable] = State,
  [LowNode, HighNode] = findInfo(LookUpTable),
  fingerTable = fixFingerTable(SelfID, NumRequests, M, FingerTable, LookUpTable, LowNode, HighNode, M),
  State = setnth(4, State, FingerTable),
  {noreply, State}.

%%####################################################################################
%%# This function returns the successor and predecessor for the node that is passed in
%%# input: selfID, lookUpTable, lowNode, highNode
%%# calls: none
%%####################################################################################

findSucPred(SelfID, LookUpTable, LowNode, HighNode) ->
  [Successor, Predecessor] =
    if
      SelfID == HighNode -> Temp = filter(LookUpTable, fun(Z) -> Z < SelfID end),
        Temp1 = reverse(Temp),
        [Head | _] = Temp1,
        [LowNode, Head];
      SelfID == LowNode -> Temp = filter(LookUpTable, fun(Z) -> Z > SelfID end),
        [Head | _] = Temp,
        [Head, HighNode];
      true -> Temp = filter(LookUpTable, fun(Z) -> Z > SelfID end),
        [Head | _] = Temp,
        Temp1 = filter(LookUpTable, fun(Z) -> Z < SelfID end),
        Temp2 = reverse(Temp1),
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
  LookUpTable = reverse(LookUpTable),
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
  Rand_node = rand:uniform(pow(2, M)),
%%# IO.puts "node: #{selfID} -> random node: #{rand_node}"
  InRange =
    if
      SelfID == LowNode ->
        if Rand_node > HighNode or Rand_node =< LowNode ->
          1;
          true ->
            0
        end;
      true ->
        if
          Rand_node > Predecessor and Rand_node =< SelfID ->
            1;
          true ->
            0
        end
    end,
  if
    InRange == 1 ->
%%# IO.puts "#{rand_node} is in range of #{selfID}, sending done"
      sendDone(selfID);
%%# startRequests(selfID, numNodes, numRequests-1, m, fingerTable, lookUpTable)
    true ->
      case member(Rand_node, maps:iterator(FingerTable)) of
        true -> Node_pid = chordNodes:whereis(Rand_node)
      end,

      case any(filter(FingerTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z < Rand_node end) of
        true ->
          case any(filter(LookUpTable, fun(Z) -> Z < Rand_node end), fun(z) -> z > selfID end) of
            true ->
              Temp = filter(sort(values(FingerTable)), fun(Z) -> Z - Rand_node < 0 end),
              Temp1 = reverse(Temp),
              [Head | _] = Temp1,
              Node_pid = chordNodes:whereis(Head);
            false ->
              Node_pid = chordMaster:whereis(successor)
          end
      end,
      update_counter(counter, "counter", {2, 1}),
      gen_Server:cast(node_pid, {find_this_node, rand_node})
  end,
  done;


startRequests(SelfID, NumNodes, NumRequests, M, FingerTable, LookUpTable) ->
%%# IO.inspect fingerTable, label: "Node #{selfID}'s FT:"
%%# IO.inspect lookUpTable, label: "Node #{selfID}'s LT:"
  [LowNode, HighNode] = findInfo(LookUpTable),
  [Successor, Predecessor] = findSucPred(SelfID, LookUpTable, LowNode, HighNode),
  Rand_node = rand:uniform(pow(2, M)),
%%# IO.puts "node: #{selfID} -> random node: #{rand_node}"
  InRange =
    if
      SelfID == LowNode ->
        if Rand_node > HighNode or Rand_node =< LowNode ->
          1;
          true ->
            0
        end;
      true ->
        if
          Rand_node > Predecessor and Rand_node =< SelfID ->
            1;
          true ->
            0
        end
    end,
  if
    InRange == 1 ->
%%# IO.puts "#{rand_node} is in range of #{selfID}, sending done"
      sendDone(selfID);
%%# startRequests(selfID, numNodes, numRequests-1, m, fingerTable, lookUpTable)
    true ->
      case member(Rand_node, maps:iterator(FingerTable)) of
        true -> Node_pid = chordNodes:whereis(Rand_node)
      end,

      case any(filter(FingerTable, fun(Z) -> Z < Rand_node end), fun(Z) -> Z < Rand_node end) of
        true ->
          case any(filter(LookUpTable, fun(Z) -> Z < Rand_node end), fun(z) -> z > selfID end) of
            true ->
              Temp = filter(sort(values(FingerTable)), fun(Z) -> Z - Rand_node < 0 end),
              Temp1 = reverse(Temp),
              [Head | _] = Temp1,
              Node_pid = chordNodes:whereis(Head);
            false ->
              Node_pid = chordMaster:whereis(successor)
          end
      end,
      update_counter(counter, "counter", {2, 1}),
      gen_Server:cast(node_pid, {find_this_node, rand_node}),
      startRequests(SelfID, NumNodes, NumRequests - 1, M, FingerTable, LookUpTable)
  end.
%%----------------------------------

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #chordNodes_state{}) -> term()).
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
