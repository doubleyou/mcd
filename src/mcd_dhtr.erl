-module(mcd_dhtr).
-behavior(gen_server).
-export([
    do/2,
    start_link/2,
    start_link/3
]).
-export([
    code_change/3,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    init/1,
    terminate/2
]).

-record(state, {
    index_size,
    ring
}).

-define(DEFAULT_REPLICATION, 1).

start_link(Name, Peers) ->
    start_link(Name, Peers, ?DEFAULT_REPLICATION).
start_link(Name, Peers, ReplicationFactor) ->
    gen_server:start_link({local, Name}, ?MODULE, [Peers, ReplicationFactor], []).

do(Name, Command) ->
    gen_server:call(Name, {do, Command}).

init([Peers, ReplicationFactor]) ->
    RawRing = Peers ++ Peers,
    Ring = array:from_list(
        [lists:sublist(RawRing, I, ReplicationFactor)
            || I <- lists:seq(1, length(Peers))]
    ),
    State = #state{
        index_size = length(Peers),
        ring = Ring
    },
    {ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call({do, Command}, _From, State = #state{ index_size = IndexSize, ring = Ring }) ->
    [_, Key | _] = tuple_to_list(Command),
    Nodes = array:get(index(Key, IndexSize), Ring),
    Result = [rpc:call(Node, mcd, do, [localmcd, Command]) || Node <- Nodes],
    {reply, reduce(Result), State}.

handle_info(_Info, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

index(Key, Size) ->
    erlang:phash2(Key, Size).

reduce([]) ->
    {error, no_valid_response};
reduce([{error, _} | Rest]) ->
    reduce(Rest);
reduce([Result | _]) ->
    Result.
