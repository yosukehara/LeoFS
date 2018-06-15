%%======================================================================
%%
%% Leo Gateway
%%
%% Copyright (c) 2012-2018 Rakuten, Inc.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%======================================================================
-module(leo_gateway_rpc_handler).

-include_lib("leo_commons/include/leo_commons.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-undef(MAX_RETRY_TIMES).
-include_lib("leo_statistics/include/leo_statistics.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("leo_http.hrl").
-include("leo_gateway.hrl").

-export([head/1,
         get/1,
         get/2,
         get/3,
         get_dir_meta/1,
         get_bucket/4,
         delete/1,
         put/1
        ]).

-record(rpc_params, {req_id = 0 :: non_neg_integer(),
                     timestamp = 0 :: non_neg_integer(),
                     addr_id = 0 :: non_neg_integer(),
                     redundancies = [] :: [#redundant_node{}]
                    }).

-define(MOD_STORAGE_OBJ_HANDLER, 'leo_storage_handler_object').
-define(MOD_STORAGE_DIR_HANDLER, 'leo_storage_handler_directory').


%% @doc Retrieve a metadata from the storage-cluster
-spec(head(Key) ->
             {ok, #?METADATA{}}|{error, any()} when Key::binary()).
head(Key) ->
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                req_id = ReqId} = get_request_parameters(?HTTP_HEAD_ATOM, Key),
    invoke(Reds, ?MOD_STORAGE_OBJ_HANDLER, head, #request{method = ?HTTP_HEAD_ATOM,
                                                          addr_id = AddrId,
                                                          key = Key,
                                                          req_id = ReqId}).


%% @doc Retrieve an object from the storage-cluster
-spec(get(Key) ->
             {ok, #?METADATA{}, binary()}|{error, any()} when Key::binary()).
get(Key) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                req_id = ReqId} = get_request_parameters(?HTTP_GET_ATOM, Key),
    invoke(Reds, ?MOD_STORAGE_OBJ_HANDLER, get, #request{method = ?HTTP_GET_ATOM,
                                                         addr_id = AddrId,
                                                         key = Key,
                                                         req_id = ReqId}).

-spec(get(Key, ETag) ->
             {ok, match}|{ok, #?METADATA{}, binary()}|{error, any()} when Key::binary(),
                                                                          ETag::non_neg_integer()).
get(Key, ETag) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                req_id = ReqId} = get_request_parameters(?HTTP_GET_ATOM, Key),
    invoke(Reds, ?MOD_STORAGE_OBJ_HANDLER, get, #request{method = ?HTTP_GET_ATOM,
                                                         addr_id = AddrId,
                                                         key = Key,
                                                         checksum = ETag,
                                                         req_id = ReqId}).

-spec(get(Key, StartPos, EndPos) ->
             {ok, #?METADATA{}, binary()}|{error, any()} when Key::binary(),
                                                              StartPos::integer(),
                                                              EndPos::integer()).
get(Key, StartPos, EndPos) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_GET),
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                req_id = ReqId} = get_request_parameters(?HTTP_GET_ATOM, Key),
    invoke(Reds, ?MOD_STORAGE_OBJ_HANDLER, get, #request{method = ?HTTP_GET_ATOM,
                                                         addr_id = AddrId,
                                                         key = Key,
                                                         start_pos = StartPos, %% @TODO
                                                         end_pos = EndPos,     %% @TODO
                                                         req_id = ReqId}).


%% @doc Retrieve a directory metadata encoded into binary from the storage-cluster
-spec(get_dir_meta(Key) ->
             {ok, binary()}|{error, any()} when Key::binary()).
get_dir_meta(Key) ->
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                req_id = ReqId} = get_request_parameters(?HTTP_GET_ATOM, Key),
    invoke(Reds, ?MOD_STORAGE_DIR_HANDLER,
           find_by_parent_dir, #request{method = ?HTTP_GET_ATOM,
                                        addr_id = AddrId,
                                        key = Key,
                                        req_id = ReqId}).


%% @doc Retrieve a list of metadata
-spec(get_bucket(Key, Delimitar, Marker, MaxKeys) ->
             {ok, MetadataL} | {error, any()} when Key::binary(),
                                                   Delimitar::binary(),
                                                   Marker::binary(),
                                                   MaxKeys::non_neg_integer(),
                                                   MetadataL::[#?METADATA{}]).
get_bucket(Key, Delimitar, Marker, MaxKeys) ->
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                req_id = ReqId} = get_request_parameters(?HTTP_GET_ATOM, Key),
    invoke(Reds, ?MOD_STORAGE_DIR_HANDLER,
           find_by_parent_dir, #request{method = ?HTTP_GET_ATOM,
                                        addr_id = AddrId,
                                        key = Key,
                                        delimitar = Delimitar,
                                        marker = Marker,
                                        max_keys = MaxKeys,
                                        req_id = ReqId}).


%% @doc Remove an object from storage-cluster
-spec(delete(Key) ->
             ok|{error, any()} when Key::binary()).
delete(Key) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_DEL),
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                timestamp = Timestamp,
                req_id = ReqId} = get_request_parameters(?HTTP_DELETE_ATOM, Key),
    invoke(Reds, ?MOD_STORAGE_OBJ_HANDLER, delete, #request{method = ?HTTP_DELETE_ATOM,
                                                            addr_id = AddrId,
                                                            key = Key,
                                                            timestamp = Timestamp,
                                                            req_id = ReqId}).


%% @doc Insert an object into the storage-cluster (regular-case)
-spec(put(ReqParams) ->
             ok|{ok, pos_integer()}|{error, any()} when ReqParams::#request{}).
put(#request{key = Key} = Req) ->
    ok = leo_metrics_req:notify(?STAT_COUNT_PUT),
    #rpc_params{redundancies = Reds,
                addr_id = AddrId,
                timestamp = Timestamp,
                req_id = ReqId} = get_request_parameters(put, Key),
    %% @DEBUG:
    %% #request{ssec_algorithm = SSEC_Algorithm,
    %%          ssec_key = SSEC_Key,
    %%          ssec_key_hash = SSEC_KeyHash,
    %%          ssec_algorithm_cp_src = _SSEC_CP_Algorithm,
    %%          ssec_key_cp_src = _SSEC_CP_Key,
    %%          ssec_key_hash_cp_src = _SSEC_CP_Hash} = Req,
    %% ?debugVal(SSEC_Algorithm),
    %% ?debugVal(SSEC_Key),
    %% ?debugVal(SSEC_KeyHash),
    invoke(Reds, ?MOD_STORAGE_OBJ_HANDLER, put, Req#request{method = ?HTTP_PUT_ATOM,
                                                            addr_id = AddrId,
                                                            timestamp = Timestamp,
                                                            req_id = ReqId}).


%% @doc Do invoke rpc calls with handling retries
%% @private
-spec(invoke(Reds, Mod, Method, ReqParams) ->
             ok|{ok, any()}|{ok, #?METADATA{}, Bin}|{error, any()} when Reds::[#redundant_node{}],
                                                                        Mod::module(),
                                                                        Method::http_verb_atom(),
                                                                        ReqParams::#request{},
                                                                        Bin::binary()).
invoke(Reds, Mod, Method, ReqParams) ->
    invoke(Reds, Mod, Method, ReqParams, []).

%% @private
invoke([],_,_,_,Errors) ->
    {error, error_filter(Errors)};
invoke([#redundant_node{available = false}|T], Mod, Method, ReqParams, Errors) ->
    invoke(T, Mod, Method, ReqParams, [?ERR_TYPE_INTERNAL_ERROR|Errors]);

invoke([#redundant_node{node = Node,
                        available = true}|T], Mod, Method, ReqParams, Errors) ->
    %% #request{method = Method} = ReqParams,
    Timeout = timeout(Method, [ReqParams]),
    case rpc:call(Node, Mod, Method, [ReqParams], Timeout) of
        %% is_dir
        Ret when is_boolean(Ret) ->
            Ret;
        %% delete
        ok = Ret ->
            Ret;
        %% put
        {ok, {etag, ETag}} ->
            {ok, ETag};
        %% get-1
        {ok, Meta, Bin} ->
            case leo_object_storage_transformer:transform_metadata(Meta) of
                {error,_} ->
                    {ok, Meta, Bin};
                Meta_1 ->
                    {ok, Meta_1, Bin}
            end;
        %% get-2
        {ok, match} = Ret ->
            Ret;
        %% find_by_parent_dir
        {ok, MetaL} when is_list(MetaL) ->
            TMetaL = lists:foldl(
                       fun(Meta, Acc) ->
                               Meta_2 = case leo_object_storage_transformer:transform_metadata(Meta) of
                                            {error,_} ->
                                                Meta;
                                            Meta_1 ->
                                                Meta_1
                                        end,
                               [Meta_2 | Acc]
                       end, [], MetaL),
            {ok, lists:reverse(TMetaL)};
        %% head/get_dir_meta
        {ok, Meta} ->
            case leo_object_storage_transformer:transform_metadata(Meta) of
                {error,_} ->
                    {ok, Meta};
                Meta_1 ->
                    {ok, Meta_1}
            end;
        {badrpc,_Cause} = Error ->
            E = handle_error(Node, Mod, Method, ReqParams, Error),
            invoke(T, Mod, Method, ReqParams, [E|Errors]);
        Error ->
            {error, handle_error(Node, Mod, Method, ReqParams, Error)}
    end.


%% @doc Get request parameters
-spec(get_request_parameters(atom(), binary()) ->
             #rpc_params{}).
get_request_parameters(Method, Key) ->
    {ok, #redundancies{id = Id,
                       nodes = Redundancies}} =
        leo_redundant_manager_api:get_redundancies_by_key(Method, Key),

    UnivDateTime = erlang:universaltime(),
    {_,_,NowPart} = os:timestamp(),
    {{Y,MO,D},{H,MI,S}} = UnivDateTime,

    ReqId = erlang:phash2([Y,MO,D,H,MI,S, erlang:node(), Key, NowPart]),
    Timestamp = calendar:datetime_to_gregorian_seconds(UnivDateTime),

    #rpc_params{addr_id = Id,
                redundancies = Redundancies,
                req_id = ReqId,
                timestamp = Timestamp}.


%% @doc Error messeage filtering
error_filter([not_found|_])          -> not_found;
error_filter([{error, not_found}|_]) -> not_found;
error_filter([H|T])                  -> error_filter(T, H).
error_filter([],             Prev)   -> Prev;
error_filter([not_found|_T],_Prev)   -> not_found;
error_filter([{error,not_found}|_],_Prev) -> not_found;
error_filter([_H|T],                Prev) -> error_filter(T, Prev).


%% @doc Handle an error response
handle_error(_Node,_Mod,_Method,_Args, {error, not_found = Error}) ->
    Error;
handle_error(_Node,_Mod,_Method,_Args, {error, unavailable = Error}) ->
    Error;
handle_error(Node, Mod, Method,_Args, {error, Cause}) ->
    ?warn("handle_error/5",
          [{node, Node}, {mod, Mod},
           {method, Method}, {cause, Cause}]),
    ?ERR_TYPE_INTERNAL_ERROR;
handle_error(Node, Mod, Method,_Args, {badrpc, Cause}) ->
    ?warn("handle_error/5",
          [{node, Node}, {mod, Mod},
           {method, Method}, {cause, Cause}]),
    ?ERR_TYPE_INTERNAL_ERROR;
handle_error(Node, Mod, Method, _Args, timeout = Cause) ->
    ?warn("handle_error/5",
          [{node, Node}, {mod, Mod},
           {method, Method}, {cause, Cause}]),
    Cause.


%% @doc Timeout depends on length of an object
timeout(Len) when ?TIMEOUT_L1_LEN > Len -> ?env_timeout_level_1();
timeout(Len) when ?TIMEOUT_L2_LEN > Len -> ?env_timeout_level_2();
timeout(Len) when ?TIMEOUT_L3_LEN > Len -> ?env_timeout_level_3();
timeout(Len) when ?TIMEOUT_L4_LEN > Len -> ?env_timeout_level_4();
timeout(_)                              -> ?env_timeout_level_5().

timeout(put, [#request{dsize = DSize}]) ->
    timeout(DSize);
timeout(get,_) -> ?env_timeout_for_get();
timeout(find_by_parent_dir,_) -> ?env_timeout_for_ls();
timeout(_,_) ->
    ?DEF_REQ_TIMEOUT.
