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
-module(leo_gateway_http_commons).

-include("leo_gateway.hrl").
-include("leo_http.hrl").
-include_lib("leo_logger/include/leo_logger.hrl").
-include_lib("leo_object_storage/include/leo_object_storage.hrl").
-include_lib("leo_s3_libs/include/leo_s3_bucket.hrl").
-include_lib("leo_redundant_manager/include/leo_redundant_manager.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([start/1, start/2]).
-export([onrequest/2, onresponse/2]).
-export([get_object/3, get_object_with_cache/4,
         put_object/3, put_small_object/3, put_large_object/4, move_large_object/3,
         delete_object/3, head_object/3,
         range_object/3,
         do_health_check/0]).
-export([reload_http_header_conf/0, validate_http_header_conf/0]).

-record(req_large_obj, {handler :: pid(),
                        bucket_name = <<>> :: binary(),
                        bucket_info = #?BUCKET{} :: #?BUCKET{},
                        key = <<>> :: binary(),
                        meta = <<>> :: binary(),
                        length :: pos_integer(),
                        timeout_for_body = 0 :: non_neg_integer(),
                        chunked_size = 0 :: non_neg_integer(),
                        reading_chunked_size = 0 :: non_neg_integer(),
                        transfer_decode_fun :: function(),
                        transfer_decode_state :: #aws_chunk_decode_state{}|undefined,
                        begin_time = 0 :: non_neg_integer()
                       }).


%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
-spec(start(#http_options{}) ->
             ok).
start(#http_options{handler = Handler,
                    port = Port,
                    ssl_port = SSLPort,
                    ssl_certfile = SSLCertFile,
                    ssl_keyfile = SSLKeyFile,
                    num_of_acceptors = NumOfAcceptors,
                    max_keepalive = MaxKeepAlive,
                    headers_config_file = CustomHeaderConf,
                    timeout_for_header = Timeout4Header,
                    sending_chunked_obj_len = SendChunkLen,
                    cache_method = CacheMethod,
                    cache_expire = CacheExpire,
                    cache_max_content_len = CacheMaxContentLen,
                    cachable_content_type = CachableContentTypes,
                    cachable_path_pattern = CachablePathPatterns} = Props) ->
    CustomHeaderSettings = case leo_nginx_conf_parser:parse(CustomHeaderConf) of
                               {ok, Ret} ->
                                   Ret;
                               not_found ->
                                   undefined;
                               {error, enoent} ->
                                   undefined;
                               {error, Reason} ->
                                   ?error("start/1",
                                          [{simple_cause, "reading http custom header file failed"},
                                           {cause, Reason}]),
                                   undefined
                           end,
    InternalCache = (CacheMethod == 'inner'),
    Dispatch      = cowboy_router:compile(
                      [{'_', [{'_', Handler,
                               [?env_layer_of_dirs(), InternalCache,
                                CustomHeaderSettings, Props]}]}]),

    Config = case InternalCache of
                 %% Using inner-cache
                 true ->
                     [{env, [{dispatch, Dispatch}]},
                      {max_keepalive, MaxKeepAlive},
                      {timeout, Timeout4Header}];
                 %% Using http-cache (like a varnish/squid)
                 false ->
                     CacheCondition = #cache_condition{expire = CacheExpire,
                                                       max_content_len = CacheMaxContentLen,
                                                       content_types = CachableContentTypes,
                                                       path_patterns = CachablePathPatterns,
                                                       sending_chunked_obj_len = SendChunkLen},
                     [{env, [{dispatch, Dispatch}]},
                      {max_keepalive, MaxKeepAlive},
                      {onrequest, Handler:onrequest(CacheCondition)},
                      {onresponse, Handler:onresponse(CacheCondition)},
                      {timeout, Timeout4Header}]
             end,

    {ok,_Pid1} = cowboy:start_http(Handler, NumOfAcceptors,
                                   [{port, Port}], Config),
    {ok,_Pid2} = cowboy:start_https(list_to_atom(lists:append([atom_to_list(Handler), "_ssl"])),
                                    NumOfAcceptors,
                                    [{port, SSLPort},
                                     {certfile, SSLCertFile},
                                     {keyfile, SSLKeyFile}],
                                    Config),
    ok.

%% @doc Launch http handler
-spec(start(atom(), #http_options{}) ->
             ok).
start(Sup, Options) ->
    %% launch Cowboy
    ChildSpec1 = {cowboy_sup,
                  {cowboy_sup, start_link, []},
                  permanent, ?SHUTDOWN_WAITING_TIME, supervisor, [cowboy_sup]},
    {ok,_} = supervisor:start_child(Sup, ChildSpec1),

    %% launch http-handler(s)
    start(Options).


%% @doc Reload HTTP header conf
-spec(reload_http_header_conf() -> ok | {error, any()}).
reload_http_header_conf() ->
    Ret = validate_http_header_conf(),
    reload_http_header_conf(Ret).

reload_http_header_conf({error, _Reason} = Err) ->
    Err;
reload_http_header_conf({ok, HttpOpts, CustomHeaderSettings}) ->
    InternalCache = (HttpOpts#http_options.cache_method == 'inner'),
    Dispatch      = cowboy_router:compile(
                      [{'_', [{'_', HttpOpts#http_options.handler,
                               [?env_layer_of_dirs(), InternalCache,
                                CustomHeaderSettings, HttpOpts]}]}]),
    ok = cowboy:set_env(HttpOpts#http_options.handler, dispatch, Dispatch).


%% @doc Validate HTTP header conf
-spec(validate_http_header_conf() -> {ok, #http_options{}, any()} | {error, any()}).
validate_http_header_conf() ->

    [{_Key, HttpOpts}] = ets:lookup(?ETS_HTTP_OPTION_TBL, ?ETS_HTTP_OPTION_KEY),
    case leo_nginx_conf_parser:parse(HttpOpts#http_options.headers_config_file) of
        {ok, CustomHeaderSettings} ->
            {ok, HttpOpts, CustomHeaderSettings};
        not_found ->
            {error, not_found};
        {error, enoent} ->
            {error, enoent};
        {error, Reason} ->
            ?error("validate_http_header_conf/0",
                   [{simple_cause, "reading http custom header file failed"},
                    {cause, Reason}]),
            {error, Reason}
    end.


%% @doc Handle request
-spec(onrequest(#cache_condition{}, function()) ->
             any()).
onrequest(#cache_condition{expire = Expire, sending_chunked_obj_len = SendChunkLen}, FunGenKey) ->
    fun(Req) ->
            Method = cowboy_req:get(method, Req),
            onrequest_1(Method, Req, Expire, FunGenKey, SendChunkLen)
    end.

%% @private
onrequest_1(?HTTP_GET, Req, Expire, FunGenKey, SendChunkLen) ->
    {_Bucket, Key} = FunGenKey(Req),
    Ret = (catch leo_cache_api:get(Key)),
    onrequest_2(Req, Expire, Key, Ret, SendChunkLen);
onrequest_1(_, Req,_,_,_) ->
    Req.

%% @private
onrequest_2(Req,_Expire,_Key, not_found, _) ->
    Req;
onrequest_2(Req,_Expire,_Key, {'EXIT', _Cause}, _) ->
    Req;
onrequest_2(Req, Expire, Key, {ok, CachedObj}, SendChunkLen) ->
    #cache{mtime = MTime,
           content_type = ContentType,
           etag = Checksum,
           body = Body,
           cmeta = CMetaBin,
           size = Size} = binary_to_term(CachedObj),
    Now = leo_date:now(),
    Diff = Now - MTime,

    case (Diff > Expire) of
        true ->
            _ = (catch leo_cache_api:delete(Key)),
            Req;
        false ->
            LastModified = leo_http:rfc1123_date(MTime),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, LastModified},
                       {?HTTP_HEADER_RESP_CONTENT_TYPE,  ContentType},
                       {?HTTP_HEADER_RESP_AGE, integer_to_list(Diff)},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(Checksum)},
                       {?HTTP_HEADER_RESP_CACHE_CTRL, ?httP_cache_ctl(Expire)}],
            Headers2 = case CMetaBin of
                           <<>> ->
                               Headers;
                           _ ->
                               CMeta = binary_to_term(CMetaBin),
                               CMeta ++ Headers
                       end,

            IMSSec = case cowboy_req:parse_header(?HTTP_HEADER_IF_MODIFIED_SINCE, Req) of
                         {ok, undefined,_} ->
                             0;
                         {ok, IMSDateTime,_} ->
                             calendar:datetime_to_gregorian_seconds(IMSDateTime)
                     end,
            case IMSSec of
                MTime ->
                    {ok, Req2} = ?reply_not_modified(Headers, Req),
                    Req2;
                _ ->
                    BodyFunc = fun(Socket, Transport) ->
                                       leo_net:chunked_send(
                                         Transport, Socket, Body, SendChunkLen)
                               end,
                    {ok, Req2} = ?reply_ok(Headers2, {Size, BodyFunc}, Req),
                    Req2
            end
    end.


%% @doc Handle response
-spec(onresponse(#cache_condition{}, function()) ->
             any()).
onresponse(#cache_condition{expire = Expire} = Config, FunGenKey) ->
    fun(Status, Header1, Body, Req) when 100 > (Status - ?HTTP_ST_OK) andalso
                                         (Status - ?HTTP_ST_OK) >= 0 ->
            %% for 20x
            case cowboy_req:get(method, Req) of
                ?HTTP_GET ->
                    {_Bucket, Key} = FunGenKey(Req),

                    case lists:all(fun(Fun) ->
                                           Fun(Key, Config, Header1, Body)
                                   end, [fun is_cachable_req1/4,
                                         fun is_cachable_req2/4,
                                         fun is_cachable_req3/4]) of
                        true ->
                            Now = leo_date:now(),
                            MetaList = lists:foldl(fun(Ele, Acc) ->
                                                           case Ele of
                                                               {<<"x-amz-meta-", _>>, _} ->
                                                                   Acc ++ Ele;
                                                               _ ->
                                                                   Acc
                                                           end
                                                   end, [], Header1),
                            CMetaBin = case MetaList of
                                           [] ->
                                               <<>>;
                                           _ ->
                                               term_to_binary(MetaList)
                                       end,
                            Bin = term_to_binary(
                                    #cache{mtime = Now,
                                           etag = leo_hex:raw_binary_to_integer(crypto:hash(md5, Body)),
                                           size = byte_size(Body),
                                           body = Body,
                                           cmeta = CMetaBin,
                                           msize = byte_size(CMetaBin),
                                           content_type = ?http_content_type(Header1)}),
                            catch leo_cache_api:put(Key, Bin),
                            Header2 = lists:keydelete(?HTTP_HEADER_LAST_MODIFIED, 1, Header1),
                            Header3 = [{?HTTP_HEADER_RESP_CACHE_CTRL, ?httP_cache_ctl(Expire)},
                                       {?HTTP_HEADER_RESP_LAST_MODIFIED, leo_http:rfc1123_date(Now)}
                                       |Header2],
                            {ok, Req2} = ?reply_ok(Header3, Req),
                            Req2;
                        false ->
                            cowboy_req:set_resp_body(<<>>, Req)
                    end;
                _ ->
                    cowboy_req:set_resp_body(<<>>, Req)
            end;
       (_Status, _Header1, _Body, Req) ->
            %% for other status like 40x, 50x
            cowboy_req:set_resp_body(<<>>, Req)
    end.


%%--------------------------------------------------------------------
%% Commons Request Handlers
%%--------------------------------------------------------------------
%%% @doc Do health check by net_adm:ping to storage nodes
%%      Return true if at least one storage responds pong
%%      Otherwise false.
-spec(do_health_check() -> boolean()).
do_health_check() ->
    case leo_redundant_manager_api:get_members_by_status(?STATE_RUNNING) of
        {ok, Members} ->
            do_health_check(Members);
        _ ->
            false
    end.
do_health_check([]) ->
    false;
do_health_check([#member{node = Node}|Rest]) ->
    case net_adm:ping(Node) of
        pong ->
            true;
        pang ->
            do_health_check(Rest)
    end.


%% @doc GET an object
-spec(get_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
get_object(Req, Key, #req_params{bucket_name = BucketName,
                                 custom_header_settings = CustomHeaderSettings,
                                 has_inner_cache = HasInnerCache,
                                 has_disk_cache = HasDiskCache,
                                 sending_chunked_obj_len = SendChunkLen,
                                 begin_time = BeginTime}) ->
    IMSSec = case cowboy_req:parse_header(?HTTP_HEADER_IF_MODIFIED_SINCE, Req) of
                 {ok, undefined,_} ->
                     0;
                 {ok, IMSDateTime,_} ->
                     calendar:datetime_to_gregorian_seconds(IMSDateTime)
             end,
    case leo_gateway_rpc_handler:get(Key) of
        %% For the case If-Modified-Since matches timestamp in metadata
        {ok, #?METADATA{timestamp = IMSSec}, _Resp} ->
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            ?reply_not_modified([?SERVER_HEADER] ++ CustomHeaders, Req);
        %% For regular case (NOT a chunked object)
        {ok, #?METADATA{cnumber = 0,
                        meta = CMetaBin} = Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),

            case HasInnerCache of
                true ->
                    Val = term_to_binary(#cache{etag = Meta#?METADATA.checksum,
                                                mtime = Meta#?METADATA.timestamp,
                                                content_type = Mime,
                                                body = RespObject,
                                                cmeta = CMetaBin,
                                                msize = byte_size(CMetaBin),
                                                size = byte_size(RespObject)}),
                    catch leo_cache_api:put(Key, Val);
                false ->
                    void
            end,

            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_CONTENT_TYPE, Mime},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = case CMetaBin of
                           <<>> ->
                               Headers ++ CustomHeaders;
                           _ ->
                               CMeta = binary_to_term(CMetaBin),
                               CMeta ++ Headers ++ CustomHeaders
                       end,

            BodyFunc = fun(Socket, Transport) ->
                               leo_net:chunked_send(
                                 Transport, Socket, RespObject, SendChunkLen),
                               ?access_log_get(BucketName, Key, Meta#?METADATA.dsize, ?HTTP_ST_OK, BeginTime)
                       end,

            ?reply_ok(Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);

        %% For a chunked object.
        {ok, #?METADATA{cnumber = TotalChunkedObjs,
                        dsize = ObjLen,
                        meta = CMetaBin} = Meta, _RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_CONTENT_TYPE, Mime},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = case CMetaBin of
                           <<>> ->
                               Headers ++ CustomHeaders;
                           _ ->
                               CMeta = binary_to_term(CMetaBin),
                               CMeta ++ Headers ++ CustomHeaders
                       end,
            BodyFunc = fun(Socket, Transport) ->
                               {ok, Pid} = leo_large_object_get_handler:start_link(
                                             {Key, #transport_record{transport = Transport,
                                                                     socket = Socket,
                                                                     sending_chunked_obj_len = SendChunkLen},
                                              HasDiskCache}),
                               try
                                   Ret = leo_large_object_get_handler:get(
                                           Pid, TotalChunkedObjs, Req, Meta),
                                   reply_fun(Ret, get, BucketName, Key, ObjLen, BeginTime),
                                   ok
                               after
                                   ?access_log_get(BucketName, Key, Meta#?METADATA.dsize, 0, BeginTime),
                                   catch leo_large_object_get_handler:stop(Pid)
                               end
                       end,
            cowboy_req:reply(?HTTP_ST_OK, Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);
        {error, Cause} ->
            reply_fun({error, Cause}, get, BucketName, Key, 0, Req, BeginTime)
    end.


%% @doc GET an object with Etag
-spec(get_object_with_cache(cowboy_req:req(), binary(), #cache{}, #req_params{}) ->
             {ok, cowboy_req:req()}).
get_object_with_cache(Req, Key, CacheObj, #req_params{bucket_name = BucketName,
                                                      custom_header_settings = CustomHeaderSettings,
                                                      sending_chunked_obj_len = SendChunkLen,
                                                      begin_time = BeginTime}) ->
    IMSSec = case cowboy_req:parse_header(?HTTP_HEADER_IF_MODIFIED_SINCE, Req) of
                 {ok, undefined,_} ->
                     0;
                 {ok, IMSDateTime,_} ->
                     calendar:datetime_to_gregorian_seconds(IMSDateTime)
             end,
    Path = CacheObj#cache.file_path,
    HasDiskCache = case Path of
                       [] ->
                           false;
                       _ ->
                           filelib:is_file(Path)
                   end,

    case leo_gateway_rpc_handler:get(Key, CacheObj#cache.etag) of
        %% HIT: For the case If-Modified-Since matches mtime in cache
        {ok, match} when CacheObj#cache.mtime == IMSSec ->
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            ?reply_not_modified([?SERVER_HEADER] ++ CustomHeaders, Req);

        %% HIT: get an object from disc-cache
        {ok, match} when Path /= []
                         andalso HasDiskCache ->
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_CONTENT_TYPE, CacheObj#cache.content_type},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(CacheObj#cache.etag)},
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, leo_http:rfc1123_date(CacheObj#cache.mtime)},
                       {?HTTP_HEADER_X_FROM_CACHE, <<"True/via disk">>}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),

            case leo_gateway_rpc_handler:head(Key) of
                {ok, #?METADATA{meta = CMetaBin}} ->
                    Headers2 = case CMetaBin of
                                   <<>> ->
                                       Headers ++ CustomHeaders;
                                   _ ->
                                       CMeta = binary_to_term(CMetaBin),
                                       CMeta ++ Headers ++ CustomHeaders
                               end,

                    case file:open(Path, [raw, read]) of
                        {ok, Fd} ->
                            BodyFunc = fun(Socket,_Transport) ->
                                               case file:sendfile(Fd, Socket, 0, 0,
                                                                  [{chunk_size, SendChunkLen}]) of
                                                   {ok,_} ->
                                                       ?access_log_get(BucketName, Key,
                                                                       CacheObj#cache.size,
                                                                       ?HTTP_ST_OK,
                                                                       BeginTime, "hit:disk-cache");
                                                   {error, Cause} ->
                                                       ?access_log_get(BucketName, Key,
                                                                       CacheObj#cache.size,
                                                                       ?HTTP_ST_OK,
                                                                       BeginTime, "err:disk-cache"),
                                                       ?warn("get_object_with_cache/4",
                                                             [{key, Path},
                                                              {summary, ?ERROR_COULD_NOT_SEND_DISK_CACHE},
                                                              {cause, Cause}])
                                               end,
                                               _ = file:close(Fd),
                                               ok
                                       end,

                            cowboy_req:reply(?HTTP_ST_OK, Headers2,
                                             {CacheObj#cache.size, BodyFunc}, Req);
                        {error, Reason} ->
                            catch leo_cache_api:delete(Key),
                            ?warn("get_object_with_cache/4",
                                  [{key, Path},
                                   {summary, ?ERROR_COULD_NOT_OPEN_DISK_CACHE},
                                   {cause, Reason}]),

                            ?access_log_get(BucketName, Key, 0,
                                            ?HTTP_ST_INTERNAL_ERROR,
                                            BeginTime, "hit:disk-cache"),
                            ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req)
                    end;
                {error, Reason} ->
                    catch leo_cache_api:delete(Key),
                    ?warn("get_object_with_cache/4",
                          [{key, Path},
                           {summary, ?ERROR_COULD_NOT_OPEN_DISK_CACHE},
                           {cause, Reason}]),

                    ?access_log_get(BucketName, Key, 0, ?HTTP_ST_INTERNAL_ERROR, BeginTime),
                    ?reply_internal_error([?SERVER_HEADER], Key, <<>>, Req)
            end;

        %% HIT: get an object from memory-cache
        {ok, match} when Path == [] ->
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_CONTENT_TYPE, CacheObj#cache.content_type},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(CacheObj#cache.etag)},
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, leo_http:rfc1123_date(CacheObj#cache.mtime)},
                       {?HTTP_HEADER_X_FROM_CACHE, <<"True/via memory">>}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(Key, CustomHeaderSettings),
            Headers2 = case CacheObj#cache.cmeta of
                           <<>> ->
                               Headers ++ CustomHeaders;
                           CMetaBin ->
                               CMeta = binary_to_term(CMetaBin),
                               CMeta ++ Headers ++ CustomHeaders
                       end,

            BodyFunc = fun(Socket, Transport) ->
                               leo_net:chunked_send(
                                 Transport, Socket, CacheObj#cache.body, SendChunkLen),
                               ?access_log_get(BucketName, Key,
                                               CacheObj#cache.size,
                                               ?HTTP_ST_OK, BeginTime, "hit:mem-cache")
                       end,

            ?reply_ok(Headers2, {CacheObj#cache.size, BodyFunc}, Req);

        %% MISS: For the case If-Modified-Since matches timestamp in metadata
        {ok, #?METADATA{timestamp = IMSSec}, _Resp} ->
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(
                                    Key, CustomHeaderSettings),
            ?reply_not_modified([?SERVER_HEADER] ++ CustomHeaders, Req);

        %% MISS: get an object from storage (small-size)
        {ok, #?METADATA{cnumber = 0,
                        meta = CMetaBin} = Meta, RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            Val = term_to_binary(#cache{etag = Meta#?METADATA.checksum,
                                        mtime = Meta#?METADATA.timestamp,
                                        content_type = Mime,
                                        body = RespObject,
                                        cmeta = CMetaBin,
                                        msize = byte_size(CMetaBin),
                                        size = byte_size(RespObject)}),
            catch leo_cache_api:put(Key, Val),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_CONTENT_TYPE, Mime},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(
                                    Key, CustomHeaderSettings),
            Headers2 = case CMetaBin of
                           <<>> ->
                               Headers ++ CustomHeaders;
                           _ ->
                               CMeta = binary_to_term(CMetaBin),
                               CMeta ++ Headers ++ CustomHeaders
                       end,
            BodyFunc = fun(Socket, Transport) ->
                               leo_net:chunked_send(
                                 Transport, Socket, RespObject, SendChunkLen)
                       end,

            ?access_log_get(BucketName, Key, Meta#?METADATA.dsize, ?HTTP_ST_OK, BeginTime),
            ?reply_ok(Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);

        %% MISS: get an object from storage (large-size)
        {ok, #?METADATA{cnumber = TotalChunkedObjs,
                        dsize = ObjLen,
                        meta = CMetaBin} = Meta, _RespObject} ->
            Mime = leo_mime:guess_mime(Key),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_CONTENT_TYPE,  Mime},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(Meta#?METADATA.checksum)},
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, ?http_date(Meta#?METADATA.timestamp)}],
            {ok, CustomHeaders} = leo_nginx_conf_parser:get_custom_headers(
                                    Key, CustomHeaderSettings),
            Headers2 = case CMetaBin of
                           <<>> ->
                               Headers ++ CustomHeaders;
                           _ ->
                               CMeta = binary_to_term(CMetaBin),
                               CMeta ++ Headers ++ CustomHeaders
                       end,
            BodyFunc = fun(Socket, Transport) ->
                               {ok, Pid} = leo_large_object_get_handler:start_link(
                                             {Key, #transport_record{
                                                      transport = Transport,
                                                      socket = Socket,
                                                      sending_chunked_obj_len = SendChunkLen},
                                              HasDiskCache}),
                               try
                                   Ret = leo_large_object_get_handler:get(
                                           Pid, TotalChunkedObjs, Req, Meta),
                                   reply_fun(Ret, get, BucketName, Key, ObjLen, BeginTime),
                                   ok
                               after
                                   ?access_log_get(BucketName, Key,
                                                   Meta#?METADATA.dsize, 0, BeginTime),
                                   catch leo_large_object_get_handler:stop(Pid)
                               end
                       end,
            cowboy_req:reply(?HTTP_ST_OK, Headers2, {Meta#?METADATA.dsize, BodyFunc}, Req);
        {error, Cause} ->
            reply_fun({error, Cause}, get, BucketName, Key, 0, Req, BeginTime)
    end.


%% @doc MOVE/COPY an object
-spec(move_large_object(#?METADATA{}, binary(), #req_params{}) ->
             ok | {error, any()}).
move_large_object(#?METADATA{key = Key, cnumber = TotalChunkedObjs} = SrcMeta, DestKey, Params) ->
    {ok, ReadHandler} = leo_large_object_move_handler:start_link(Key, 0, TotalChunkedObjs),
    try
        move_large_object(SrcMeta, DestKey, Params, ReadHandler)
    after
        catch leo_large_object_move_handler:stop(ReadHandler)
    end.

move_large_object(#?METADATA{dsize = Size}, DestKey,
                  #req_params{chunked_obj_len = ChunkedSize,
                              custom_metadata = CMeta,
                              bucket_name = BucketName,
                              bucket_info = BucketInfo,
                              begin_time = BeginTime}, ReadHandler) ->
    {ok, WriteHandler} =
        leo_large_object_put_handler:start_link(
          BucketInfo, DestKey, ChunkedSize),
    try
        case move_large_object_1(
               leo_large_object_move_handler:get_chunk_obj(ReadHandler),
               #req_large_obj{handler = WriteHandler,
                              bucket_name = BucketName,
                              bucket_info = BucketInfo,
                              key = DestKey,
                              meta = CMeta,
                              length = Size,
                              chunked_size = ChunkedSize,
                              begin_time = BeginTime}, ReadHandler) of
            ok ->
                ok;
            {error, Cause} ->
                ok = leo_large_object_put_handler:rollback(WriteHandler),
                {error, Cause}
        end
    after
        catch leo_large_object_put_handler:stop(WriteHandler)
    end.

%% @private
move_large_object_1({ok, Data},
                    #req_large_obj{key = Key,
                                   handler = WriteHandler} = ReqLargeObj, ReadHandler) ->
    case catch leo_large_object_put_handler:put(WriteHandler, Data) of
        ok ->
            move_large_object_1(
              leo_large_object_move_handler:get_chunk_obj(ReadHandler),
              ReqLargeObj, ReadHandler);
        {'EXIT', Cause} ->
            ?error("move_large_object_1/3",
                   [{key, binary_to_list(Key)}, {cause, Cause}]),
            {error, ?ERROR_FAIL_PUT_OBJ};
        {error, Cause} ->
            ?error("move_large_object_1/3",
                   [{key, binary_to_list(Key)}, {cause, Cause}]),
            {error, ?ERROR_FAIL_PUT_OBJ}
    end;
move_large_object_1({error, Cause},
                    #req_large_obj{key = Key},_ReadHandler) ->
    ?error("move_large_object_1/3",
           [{key, binary_to_list(Key)}, {cause, Cause}]),
    {error, ?ERROR_FAIL_RETRIEVE_OBJ};
move_large_object_1(done, #req_large_obj{handler = WriteHandler,
                                         bucket_name = BucketName,
                                         bucket_info = BucketInfo,
                                         key = Key,
                                         meta = CMeta,
                                         length = Size,
                                         chunked_size = ChunkedSize,
                                         begin_time = BeginTime},_ReadHandler) ->
    case catch leo_large_object_put_handler:result(WriteHandler) of
        {ok, #large_obj_info{length = TotalSize,
                             num_of_chunks = TotalChunks,
                             md5_context = Digest}} when Size == TotalSize ->
            Digest_1 = leo_hex:raw_binary_to_integer(Digest),
            %% @TODO Object Encryption (2018-05-16)
            case leo_gateway_rpc_handler:put(#put_req_params{
                                                path = Key,
                                                body = ?BIN_EMPTY,
                                                meta = CMeta,
                                                msize = byte_size(CMeta),
                                                dsize = Size,
                                                total_chunks = TotalChunks,
                                                cindex = 0,
                                                csize = ChunkedSize,
                                                digest = Digest_1,
                                                bucket_info = BucketInfo}) of
                {ok, _ETag} ->
                    ?access_log_put(BucketName, Key, Size, ?HTTP_ST_OK, BeginTime),
                    ok;
                {error, timeout = Cause} ->
                    {error, Cause};
                {error, unavailable} ->
                    {error, unavailable};
                {error,_Cause} ->
                    {error, ?ERROR_FAIL_PUT_OBJ}
            end;
        {ok, _} ->
            {error, ?ERROR_NOT_MATCH_LENGTH};
        {_,_Cause} ->
            {error, ?ERROR_FAIL_PUT_OBJ}
    end.


%% @doc PUT an object
-spec(put_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
put_object(Req, Key, #req_params{bucket_name = BucketName,
                                 is_upload = IsUpload,
                                 timeout_for_body = Timeout4Body,
                                 max_len_of_obj = MaxLenForObj,
                                 threshold_of_chunk_len = ThresholdObjLen,
                                 transfer_decode_fun = TransferDecodeFun,
                                 transfer_decode_state = TransferDecodeState,
                                 begin_time = BeginTime} = Params) ->
    {Size, _} = cowboy_req:body_length(Req),
    ?debug("put_object/3", "Object Size: ~p", [Size]),

    case (Size >= ThresholdObjLen) of
        true when Size >= MaxLenForObj ->
            ?access_log_put(BucketName, Key, 0, ?HTTP_ST_BAD_REQ, BeginTime),
            ?reply_bad_request([?SERVER_HEADER],
                               ?XML_ERROR_CODE_EntityTooLarge,
                               ?XML_ERROR_MSG_EntityTooLarge,
                               Key, <<>>, Req);

        true when IsUpload == false ->
            put_large_object(Req, Key, Size, Params);
        false ->
            Ret = case cowboy_req:has_body(Req) of
                      true ->
                          BodyOpts = case TransferDecodeFun of
                                         undefined ->
                                             [{read_timeout, Timeout4Body}];
                                         _ ->
                                             [{read_timeout, Timeout4Body},
                                              {transfer_decode,
                                               TransferDecodeFun, TransferDecodeState}]
                                     end,
                          case cowboy_req:body(Req, BodyOpts) of
                              {ok, Bin0, Req0} ->
                                  {ok, {Size, Bin0, Req0}};
                              {error, Cause} ->
                                  {error, Cause}
                          end;
                      false ->
                          {ok, {0, ?BIN_EMPTY, Req}}
                  end,
            put_small_object(Ret, Key, Params)
    end.


%% @doc check if a specified binary contains a character
%% @private
binary_is_contained(<<>>, _Char) ->
    false;
binary_is_contained(<<C:8, Rest/binary>>, Char) ->
    case C of
        Char ->
            true;
        _ ->
            binary_is_contained(Rest, Char)
    end.


%% @doc Put a small object
%% @private
-spec(put_small_object({ok, any()}|{error, any()}, binary(), #req_params{}) ->
             {ok, any()}).
put_small_object({error, Cause},_,_) ->
    {error, Cause};
put_small_object({ok, {Size, Bin, Req}}, Key, #req_params{bucket_name = BucketName,
                                                          custom_metadata = CMeta,
                                                          upload_part_num = UploadPartNum,
                                                          has_inner_cache = HasInnerCache,
                                                          bucket_info = BucketInfo,
                                                          begin_time = BeginTime}) ->
    case leo_gateway_rpc_handler:put(
           #put_req_params{path = Key,
                           body = Bin,
                           meta = CMeta,
                           msize = byte_size(CMeta),
                           dsize = Size,
                           cindex = UploadPartNum,
                           bucket_info = BucketInfo}) of
        {ok, ETag} ->
            case (HasInnerCache
                  andalso binary_is_contained(Key, 10) == false) of
                true  ->
                    %% Stores an object into the cache
                    Mime = leo_mime:guess_mime(Key),
                    RetCMeta =
                        case CMeta of
                            <<>> ->
                                {ok, CMeta};
                            _ ->
                                case catch leo_misc:get_value(
                                             ?PROP_CMETA_UDM, binary_to_term(CMeta)) of
                                    {'EXIT', Reason} ->
                                        ?error("put_small_object/3",
                                               [{key, binary_to_list(Key)},
                                                {simple_cause, "Invalid metadata"},
                                                {cause, Reason}]),
                                        {error, Reason};
                                    undefined ->
                                        {ok, <<>>};
                                    UDM ->
                                        case catch term_to_binary(UDM) of
                                            {'EXIT', Why} ->
                                                ?error("put_small_object/3",
                                                       [{key, binary_to_list(Key)},
                                                        {simple_cause, "Invalid metadata"},
                                                        {cause, Why}]),
                                                {error, Why};
                                            CMeta_1 ->
                                                {ok, CMeta_1}
                                        end
                                end
                        end,

                    case RetCMeta of
                        {ok, CMeta_2} ->
                            Val = term_to_binary(#cache{etag = ETag,
                                                        mtime = leo_date:now(),
                                                        content_type = Mime,
                                                        body = Bin,
                                                        cmeta = CMeta_2,
                                                        msize = byte_size(CMeta_2),
                                                        size = byte_size(Bin)
                                                       }),
                            catch leo_cache_api:put(Key, Val);
                        _ ->
                            void
                    end;
                false ->
                    void
            end,

            Header = [?SERVER_HEADER,
                      {?HTTP_HEADER_RESP_ETAG, ?http_etag(ETag)}],
            ?access_log_put(BucketName, Key, Size, ?HTTP_ST_OK, BeginTime),
            ?reply_ok(Header, Req);
        {error, Cause} ->
            reply_fun({error, Cause}, put, BucketName, Key, 0, Req, BeginTime)
    end.


%% @doc Put a large-object
%% @private
-spec(put_large_object(cowboy_req:req(), binary(), pos_integer(), #req_params{}) ->
             {ok, cowboy_req:req()}).
put_large_object(Req, Key, Size, #req_params{bucket_name = BucketName,
                                             bucket_info = BucketInfo,
                                             custom_metadata = CMeta,
                                             timeout_for_body = Timeout4Body,
                                             chunked_obj_len = ChunkedSize,
                                             reading_chunked_obj_len = ReadingChunkedSize,
                                             transfer_decode_fun = TransferDecodeFun,
                                             transfer_decode_state = TransferDecodeState,
                                             begin_time = BeginTime})->
    %% launch 'large_object_handler'
    {ok, Handler} =
        leo_large_object_put_handler:start_link(BucketInfo, Key, ChunkedSize),

    %% remove a registered object with 'touch-command'
    %% from the cache
    catch leo_cache_api:delete(Key),

    %% retrieve an object from the stream,
    %% then put it to the storage-cluster
    BodyOpts = [{length, ReadingChunkedSize},
                {read_timeout, Timeout4Body},
                {read_length, ReadingChunkedSize}
               ],
    BodyOpts_1 = case TransferDecodeFun of
                     undefined ->
                         BodyOpts;
                     _ ->
                         [{transfer_decode, TransferDecodeFun, TransferDecodeState} | BodyOpts]
                 end,
    Reply = case put_large_object_1(cowboy_req:body(Req, BodyOpts_1),
                                    #req_large_obj{handler = Handler,
                                                   key = Key,
                                                   meta = CMeta,
                                                   length = Size,
                                                   timeout_for_body = Timeout4Body,
                                                   chunked_size = ChunkedSize,
                                                   reading_chunked_size = ReadingChunkedSize,
                                                   transfer_decode_fun = TransferDecodeFun,
                                                   transfer_decode_state = TransferDecodeState,
                                                   begin_time = BeginTime}) of
                {error, ErrorRet} ->
                    ok = leo_large_object_put_handler:rollback(Handler),
                    {Req_1, Cause} = case ErrorRet of
                                         {_, _} ->
                                             ErrorRet;
                                         _ ->
                                             {Req, ErrorRet}
                                     end,
                    reply_fun({error, Cause}, put, BucketName, Key, Size, Req_1, BeginTime);
                Ret ->
                    ?access_log_put(BucketName, Key, Size, ?HTTP_ST_OK, BeginTime),
                    Ret
            end,
    catch leo_large_object_put_handler:stop(Handler),
    Reply.

%% @private
put_large_object_1({more, Data, Req},
                   #req_large_obj{key = Key,
                                  handler = Handler,
                                  timeout_for_body = Timeout4Body,
                                  reading_chunked_size = ReadingChunkedSize,
                                  transfer_decode_fun = TransferDecodeFun,
                                  transfer_decode_state = TransferDecodeState
                                 } = ReqLargeObj) ->
    case catch leo_large_object_put_handler:put(Handler, Data) of
        ok ->
            BodyOpts = [{length, ReadingChunkedSize},
                        {read_timeout, Timeout4Body},
                        {read_length, ReadingChunkedSize}
                       ],
            BodyOpts_1 = case TransferDecodeFun of
                             undefined ->
                                 BodyOpts;
                             _ ->
                                 [{transfer_decode,
                                   TransferDecodeFun, TransferDecodeState} | BodyOpts]
                         end,
            put_large_object_1(cowboy_req:body(Req, BodyOpts_1), ReqLargeObj);
        {'EXIT', Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}};
        {error, Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
    end;

%% An error occurred while reading the body, connection is gone.
%% @private
put_large_object_1({error, Cause}, #req_large_obj{key = Key}) ->
    ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                    {cause, Cause}]),
    {error, ?ERROR_FAIL_RETRIEVE_OBJ};

%% @private
put_large_object_1({ok, Data, Req}, #req_large_obj{handler = Handler,
                                                   bucket_info = BucketInfo,
                                                   key = Key,
                                                   meta = CMeta,
                                                   length = Size,
                                                   chunked_size = ChunkedSize}) ->
    case catch leo_large_object_put_handler:put(Handler, Data) of
        ok ->
            case catch leo_large_object_put_handler:result(Handler) of
                {ok, #large_obj_info{length = TotalSize,
                                     num_of_chunks = TotalChunks,
                                     md5_context = Digest}} when Size == TotalSize ->
                    Digest_1 = leo_hex:raw_binary_to_integer(Digest),
                    case leo_gateway_rpc_handler:put(#put_req_params{
                                                        path = Key,
                                                        body = ?BIN_EMPTY,
                                                        meta = CMeta,
                                                        msize = byte_size(CMeta),
                                                        dsize = Size,
                                                        total_chunks = TotalChunks,
                                                        csize = ChunkedSize,
                                                        digest = Digest_1,
                                                        bucket_info = BucketInfo}) of
                        {ok, _ETag} ->
                            Header = [?SERVER_HEADER,
                                      {?HTTP_HEADER_RESP_ETAG, ?http_etag(Digest_1)}],
                            ?reply_ok(Header, Req);
                        {error, timeout = Cause} ->
                            {error, {Req, Cause}};
                        {error, unavailable} ->
                            {error, {Req, unavailable}};
                        {error,_Cause} ->
                            {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
                    end;
                {ok, #large_obj_info{length = TotalSize}} ->
                    ?error("put_large_object_1/2", [{total_size, TotalSize}, {size, Size},
                                                    {cause, "Length Not Match"}]),
                    {error, {Req, ?ERROR_NOT_MATCH_LENGTH}};
                {_,_Cause} ->
                    {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
            end;
        {'EXIT', Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}};
        {error, Cause} ->
            ?error("put_large_object_1/2", [{key, binary_to_list(Key)},
                                            {cause, Cause}]),
            {error, {Req, ?ERROR_FAIL_PUT_OBJ}}
    end.


%% @doc DELETE an object
-spec(delete_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
delete_object(Req, Key, #req_params{bucket_name = BucketName}) ->
    BeginTime = leo_date:clock(),
    Size1 = case leo_gateway_rpc_handler:head(Key) of
                {ok, #?METADATA{del = 0, dsize = Size}} ->
                    Size;
                _ ->
                    0
            end,

    case leo_gateway_rpc_handler:delete(Key) of
        ok ->
            ?access_log_delete(BucketName, Key, Size1, ?HTTP_ST_NO_CONTENT, BeginTime),
            ?reply_no_content([?SERVER_HEADER], Req);
        {error, Cause} ->
            reply_fun({error, Cause}, delete, BucketName, Key, 0, Req, BeginTime)
    end.


%% @doc HEAD an object
-spec(head_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
head_object(Req, Key, #req_params{bucket_name = BucketName}) ->
    BeginTime = leo_date:clock(),
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{del = 0,
                        meta = CMetaBin} = Meta} ->
            Timestamp = leo_http:rfc1123_date(Meta#?METADATA.timestamp),
            Headers = [?SERVER_HEADER,
                       {?HTTP_HEADER_RESP_CONTENT_TYPE, leo_mime:guess_mime(Key)},
                       {?HTTP_HEADER_RESP_ETAG, ?http_etag(Meta#?METADATA.checksum)},
                       %% [NOTE]:
                       %% - https://github.com/leo-project/leofs/issues/489
                       %% We used Camel Case for response headers
                       %% as old version boto(s) only took care Camel Cased headers.
                       %% but the latest(also maybe last) stable release seems to
                       %% handle headers with caseinsensitive mannear.
                       %% so I changed to the lower case one from the Camel Cased
                       %% in order to cope with cowboy_req:merge_headers which only take care
                       %% lower case ones.
                       {?HTTP_HEADER_CONTENT_LENGTH, erlang:integer_to_list(Meta#?METADATA.dsize)},
                       {?HTTP_HEADER_RESP_LAST_MODIFIED, Timestamp}],
            Headers2 = case CMetaBin of
                           <<>> ->
                               Headers;
                           _ ->
                               CMeta = binary_to_term(CMetaBin),
                               CMeta ++ Headers
                       end,
            ?access_log_head(BucketName, Key, ?HTTP_ST_OK, BeginTime),
            cowboy_req:reply(?HTTP_ST_OK, Headers2, <<>>, Req);
        {ok, #?METADATA{del = 1}} ->
            ?access_log_head(BucketName, Key, ?HTTP_ST_NOT_FOUND, BeginTime),
            ?reply_not_found_without_body([?SERVER_HEADER], Req);
        {error, Cause} ->
            reply_fun({error, Cause}, head, BucketName, Key, 0, Req, BeginTime)
    end.


%% @doc Retrieve a part of an object
-spec(range_object(cowboy_req:req(), binary(), #req_params{}) ->
             {ok, cowboy_req:req()}).
range_object(Req, Key, #req_params{bucket_name = BucketName,
                                   range_header = RangeHeader,
                                   sending_chunked_obj_len = SendChunkLen}) ->
    BeginTime = leo_date:clock(),
    Range = cowboy_http:range(RangeHeader),
    get_range_object(Req, BucketName, Key, Range, SendChunkLen, BeginTime).


%% @private
%% @TODO: Handle Multiple Part Range Request with "multipart/byteranges"
%% RFC: 7233, https://tools.ietf.org/html/rfc7233#appendix-A
get_range_object(Req, BucketName, Key, {error, badarg}, _, BeginTime) ->
    ?access_log_get(BucketName, Key, 0, ?HTTP_ST_BAD_RANGE, BeginTime),
    ?reply_bad_range([?SERVER_HEADER], Key, <<>>, Req);
get_range_object(Req, BucketName, Key, {_Unit, Range}, SendChunkLen, BeginTime) when is_list(Range) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{del = 0,
                        dsize = ObjectSize,
                        meta = CMetaBin} = Meta}->
            Range_2 = fix_range_end(Range, ObjectSize),
            case get_body_length(ObjectSize, Range_2) of
                {ok, Length} ->
                    Timestamp = leo_http:rfc1123_date(Meta#?METADATA.timestamp),
                    Headers = [?SERVER_HEADER,
                               {?HTTP_HEADER_RESP_CONTENT_TYPE, leo_mime:guess_mime(Key)},
                               {?HTTP_HEADER_RESP_LAST_MODIFIED, Timestamp}],
                    Headers2 = case CMetaBin of
                                   <<>> ->
                                       Headers;
                                   _ ->
                                       CMeta = binary_to_term(CMetaBin),
                                       CMeta ++ Headers
                               end,
                    %% Reply with Content Range
                    %% RFC: 7233, https://tools.ietf.org/html/rfc7233#section-4.2
                    Headers3 = case length(Range) of
                                   1 ->
                                       ContentRangeBin = range_to_binary(Range, ObjectSize),
                                       ObjectSizeBin = integer_to_binary(ObjectSize),
                                       [{?HTTP_HEADER_RESP_CONTENT_RANGE,
                                         <<"bytes ", ContentRangeBin/binary, "/", ObjectSizeBin/binary>>}] ++ Headers2;
                                   _ ->
                                       Headers2
                               end,
                    Req2 = cowboy_req:set_resp_body_fun(
                             Length,
                             fun(Socket, Transport) ->
                                     get_range_object_1(Req, BucketName, Key, Range_2, undefined,
                                                        #transport_record{transport = Transport,
                                                                          socket = Socket,
                                                                          sending_chunked_obj_len = SendChunkLen})
                             end,
                             Req),
                    ?reply_partial_content(Headers3, Req2);
                {error, bad_range} ->
                    ?access_log_get(BucketName, Key, 0, ?HTTP_ST_BAD_RANGE, BeginTime),
                    ?reply_bad_range([?SERVER_HEADER], Key, <<>>, Req)
            end;
        {ok, #?METADATA{del = 1}} ->
            ?access_log_get(BucketName, Key, 0, ?HTTP_ST_NOT_FOUND, BeginTime),
            ?reply_not_found_without_body([?SERVER_HEADER], Req);
        {error, Cause} ->
            reply_fun({error, Cause}, get, BucketName, Key, 0, Req, BeginTime)
    end.

%% @private
get_range_object_1(_Req,_BucketName,_Key,_, {error,_Reason}, #transport_record{socket = Socket,
                                                                               transport = Transport}) ->
    %% @TODO:
    %%    Transport:close(Socket),
    %%    case Reason of
    %%        unavailable ->
    %%            ?reply_service_unavailable_error([?SERVER_HEADER], Key, <<>>, Req);
    %%        not_found ->
    %%            ?reply_not_found([?SERVER_HEADER], Key, <<>>, Req);
    %%        _ ->
    %%            ?reply_internal_error_without_body([?SERVER_HEADER], Req)
    %%    end;
    Transport:close(Socket);
get_range_object_1(Req,_BucketName,_Key, [],_,_TransportRec) ->
    {ok, Req};
get_range_object_1(Req, BucketName, Key, [{Start, End}|Rest],_, TransportRec) ->
    Ret = get_range_object_2(Req, BucketName, Key, Start, End, TransportRec),
    get_range_object_1(Req, BucketName, Key, Rest, Ret, TransportRec);
get_range_object_1(Req, BucketName, Key, [End|Rest], _, TransportRec) ->
    Ret = get_range_object_2(Req, BucketName, Key, 0, End, TransportRec),
    get_range_object_1(Req, BucketName, Key, Rest, Ret, TransportRec).

%% @private
get_range_object_2(Req, BucketName, Key, Start, End, TransportRec) ->
    case leo_gateway_rpc_handler:head(Key) of
        {ok, #?METADATA{del = 0,
                        cnumber = 0}} ->
            get_range_object_small(Req, BucketName, Key, Start, End, TransportRec);
        {ok, #?METADATA{del = 0,
                        cnumber = N,
                        dsize = ObjectSize,
                        csize = CS}} ->
            %% Retrieve start and end position of the object
            {NewStartPos, NewEndPos} = calc_pos(Start, End, ObjectSize),

            %% Retrieve the grand-child's metadata
            %% to get collect chunk size of the object
            {CurPos, Index} = move_current_pos_to_head(NewStartPos, CS, 0, 0),

            IndexBin = list_to_binary(integer_to_list(1)),
            Key_1 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,
            {CurPos_1, Index_1} =
                case leo_gateway_rpc_handler:head(Key_1) of
                    {ok, #?METADATA{del = 0,
                                    dsize = ChildObjSize}} ->
                        move_current_pos_to_head(NewStartPos, ChildObjSize, 0, 0);
                    _ ->
                        {CurPos, Index}
                end,
            get_range_object_large(Req, BucketName, Key,
                                   NewStartPos, NewEndPos, N, Index_1, CurPos_1,
                                   TransportRec);
        Error ->
            Error
    end.


%% @doc Retrieve the small object
%% @private
get_range_object_small(_Req, BucketName, Key, Start, End,
                       #transport_record{transport = Transport,
                                         socket = Socket,
                                         sending_chunked_obj_len = SendChunkLen}) ->
    BeginTime = leo_date:clock(),
    case leo_gateway_rpc_handler:get(Key, Start, End) of
        {ok, _Meta, <<>>} ->
            ?access_log_get(BucketName, Key, 0, ?HTTP_ST_OK, BeginTime),
            ok;
        {ok, _Meta, Bin} ->
            ?access_log_get(BucketName, Key, byte_size(Bin), ?HTTP_ST_OK, BeginTime),
            case leo_net:chunked_send(Transport, Socket, Bin, SendChunkLen) of
                ok ->
                    ok;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%% @private
%% @doc Fix last-byte-pos when it is larger than or equal to object size
%% Quote from RFC-7233#2.1
%% If the last-byte-pos value is absent, or if the value is greater than
%% or equal to the current length of the representation data, the byte
%% range is interpreted as the remainder of the representation (i.e., the
%% server replaces the value of last-byte-pos with a value that is one
%% less than the current length of the selected representation).
fix_range_end(Range, ObjectSize) ->
    fix_range_end_1(Range, ObjectSize, []).

fix_range_end_1([], _, Acc) ->
    lists:reverse(Acc);
fix_range_end_1([{Start, End}|Rest], ObjectSize, Acc) when is_integer(End),
                                                           End >= ObjectSize ->
    fix_range_end_1(Rest, ObjectSize, [{Start, ObjectSize - 1} | Acc]);
fix_range_end_1([Head|Rest], ObjectSize, Acc) ->
    fix_range_end_1(Rest, ObjectSize, [Head|Acc]).


%% @private
%% @doc Convert Range List to Content-Range Format
range_to_binary(List, ObjectSize) ->
    range_to_binary(List, ObjectSize, <<>>).

range_to_binary([], _, Acc) ->
    Acc;
range_to_binary([{Start, infinity}|Rest], OS, Acc) ->
    range_to_binary([{Start, OS - 1}|Rest], OS, Acc);
range_to_binary([{Start, End}|Rest], OS, <<>>) ->
    SB = integer_to_binary(Start),
    EB = integer_to_binary(End),
    range_to_binary(Rest, OS, <<SB/binary, "-", EB/binary>>);
range_to_binary([{Start, End}|Rest], OS, Acc) ->
    SB = integer_to_binary(Start),
    EB = integer_to_binary(End),
    range_to_binary(Rest, OS, <<Acc/binary, ",", SB/binary, "-", EB/binary>>);
range_to_binary([End|Rest], OS, Acc) ->
    range_to_binary([{OS + End, OS - 1}|Rest], OS, Acc).


%% @private
get_body_length(ObjectSize, Range) ->
    get_body_length_1(Range, ObjectSize, 0).

%% @private
get_body_length_1([], _ObjectSize, Acc) ->
    {ok, Acc};
get_body_length_1([{Start, infinity}|Rest], ObjectSize, Acc) ->
    get_body_length_1(Rest, ObjectSize, Acc + ObjectSize - Start);
get_body_length_1([{Start, End}|Rest], ObjectSize, Acc) when End < 0 ->
    get_body_length_1(Rest, ObjectSize, Acc + ObjectSize - Start);
get_body_length_1([{Start, End}|Rest], ObjectSize, Acc) when End < ObjectSize ->
    get_body_length_1(Rest, ObjectSize, Acc + End - Start + 1);
get_body_length_1([End|Rest], ObjectSize, Acc) when End < 0 ->
    get_body_length_1(Rest, ObjectSize, Acc + ObjectSize);
get_body_length_1([End|Rest], ObjectSize, Acc) when End < ObjectSize ->
    get_body_length_1(Rest, ObjectSize, Acc + End + 1);
get_body_length_1(_, _, _) ->
    {error, bad_range}.


%% @doc
%% @private
move_current_pos_to_head(Start, ChunkedSize, CurPos, Idx)
  when (CurPos + ChunkedSize - 1) < Start ->
    move_current_pos_to_head(Start, ChunkedSize, CurPos + ChunkedSize, Idx + 1);
move_current_pos_to_head(_Start, _ChunkedSize, CurPos, Idx) ->
    {CurPos, Idx}.


%% @doc
%% @private
calc_pos(StartPos, infinity, ObjectSize) ->
    {StartPos, ObjectSize - 1};
calc_pos(_StartPos, EndPos, ObjectSize) when EndPos < 0 ->
    NewStartPos = ObjectSize + EndPos,
    NewEndPos = ObjectSize - 1,
    {NewStartPos, NewEndPos};
calc_pos(StartPos, 0, ObjectSize) when StartPos > 0 ->
    {StartPos, ObjectSize - 1};
calc_pos(StartPos, EndPos, _ObjectSize) ->
    {StartPos, EndPos}.


%% @doc Retrieve the large object
%% @private
get_range_object_large(_Req,_BucketName,_Key,_Start,_End,
                       _Total, _Index, {error, _} = Error, _TransportRec) ->
    Error;
get_range_object_large(_Req,_BucketName,_Key,_Start,_End,
                       Total, Total, CurPos, _TransportRec) ->
    {ok, CurPos};
get_range_object_large(_Req,_BucketName,_Key,_Start, End,
                       _Total,_Index, CurPos, _TransportRec) when CurPos > End ->
    {ok, CurPos};
get_range_object_large( Req, BucketName, Key, Start, End,
                        Total, Index, CurPos, TransportRec) ->
    IndexBin = list_to_binary(integer_to_list(Index + 1)),
    Key2 = << Key/binary, ?DEF_SEPARATOR/binary, IndexBin/binary >>,

    case leo_gateway_rpc_handler:head(Key2) of
        {ok, #?METADATA{cnumber = 0,
                        dsize = CS}} ->
            %% get and chunk an object
            NewPos = send_chunk(Req, BucketName, Key2, Start, End, CurPos, CS, TransportRec),
            get_range_object_large(Req, BucketName, Key, Start, End,
                                   Total, Index + 1, NewPos, TransportRec);

        {ok, #?METADATA{cnumber = GrandChildNum}} ->
            case get_range_object_large(Req, BucketName, Key2, Start, End,
                                        GrandChildNum, 0, CurPos, TransportRec) of
                {ok, NewPos} ->
                    get_range_object_large(Req, BucketName, Key, Start, End,
                                           Total, Index + 1, NewPos, TransportRec);
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Sending a chunk to the client
%% @private
send_chunk(_Req,_,_Key, Start,_End, CurPos, ChunkSize, _TransportRec)
  when (CurPos + ChunkSize - 1) < Start ->
    %% skip proc
    CurPos + ChunkSize;
send_chunk(_Req,_BucketName, Key, Start, End, CurPos, ChunkSize,
           #transport_record{transport = Transport,
                             socket    = Socket,
                             sending_chunked_obj_len = SendChunkLen})
  when CurPos >= Start andalso
       (CurPos + ChunkSize - 1) =< End ->
    %% whole get
    case leo_gateway_rpc_handler:get(Key) of
        {ok, _Meta, Bin} ->
            %% @FIXME current impl can't handle a file which consist of grand children
            %% ?access_log_get(BucketName, Key, ChunkSize, ?HTTP_ST_OK),
            case leo_net:chunked_send(Transport, Socket, Bin, SendChunkLen) of
                ok ->
                    CurPos + ChunkSize;
                {error, Cause} ->
                    {error, Cause}
            end;
        Error ->
            Error
    end;

send_chunk(_Req,_BucketName, Key, Start, End, CurPos, ChunkSize,
           #transport_record{transport = Transport,
                             socket = Socket,
                             sending_chunked_obj_len = SendChunkLen}) ->
    %% partial get
    StartPos = case Start =< CurPos of
                   true -> 0;
                   false -> Start - CurPos
               end,
    EndPos = case (CurPos + ChunkSize - 1) =< End of
                 true -> ChunkSize - 1;
                 false -> End - CurPos
             end,
    case leo_gateway_rpc_handler:get(Key, StartPos, EndPos) of
        {ok, _Meta, <<>>} ->
            CurPos + ChunkSize;
        {ok, _Meta, Bin} ->
            %% @FIXME current impl can't handle a file which consist of grand childs
            %% ?access_log_get(BucketName, Key, ChunkSize, ?HTTP_ST_OK),
            case leo_net:chunked_send(Transport, Socket, Bin, SendChunkLen) of
                ok ->
                    CurPos + ChunkSize;
                {error, Cause} ->
                    {error, Cause}
            end;
        {error, Cause} ->
            {error, Cause}
    end.


%% @doc Judge cachable request
%% @private
is_cachable_req1(_Key, #cache_condition{max_content_len = MaxLen}, Headers, Body) ->
    HasNOTCacheControl = (false == lists:keyfind(?HTTP_HEADER_CACHE_CTRL, 1, Headers)),
    HasNOTCacheControl  andalso
        is_binary(Body) andalso
        size(Body) > 0  andalso
        size(Body) < MaxLen.

is_cachable_req2(_Key, #cache_condition{path_patterns = []},       _Headers, _Body) -> true;
is_cachable_req2(_Key, #cache_condition{path_patterns = undefined},_Headers, _Body) -> true;
is_cachable_req2( Key, #cache_condition{path_patterns = Patterns}, _Headers, _Body) ->
    Res = lists:any(fun(Path) ->
                            nomatch /= re:run(Key, Path)
                    end, Patterns),
    Res.

is_cachable_req3(_, #cache_condition{content_types = []},       _Headers, _Body) -> true;
is_cachable_req3(_, #cache_condition{content_types = undefined},_Headers, _Body) -> true;
is_cachable_req3(_Key, #cache_condition{content_types = CTypes}, Headers, _Body) ->
    case lists:keyfind(?HTTP_HEADER_CONTENT_TYPE, 1, Headers) of
        false ->
            false;
        {_, ContentType} ->
            lists:member(ContentType, CTypes)
    end.


%% @doc Reply to a request
%% @private
reply_fun(ok, get, Bucket, Key, ObjLen, BeginTime) ->
    ?access_log_get(Bucket, Key, ObjLen, ?HTTP_ST_OK, BeginTime);
reply_fun({ok,_}, get, Bucket, Key, ObjLen, BeginTime) ->
    ?access_log_get(Bucket, Key, ObjLen, ?HTTP_ST_OK, BeginTime);
reply_fun({error, Cause}, Method, Bucket, Key, ObjLen, BeginTime) ->
    ?reply_fun(Cause, Method, Bucket, Key, ObjLen, BeginTime).
reply_fun({error, Cause}, Method, Bucket, Key, ObjLen, Req, BeginTime) ->
    ?reply_fun(Cause, Method, Bucket, Key, ObjLen, Req, BeginTime).


%%====================================================================
%% TEST
%%====================================================================
-ifdef(TEST).
fix_range_end_test() ->
    ?debugMsg("Testing Fix Range End"),
    [{0,3}] = fix_range_end([{0,4}], 4),
    [{0,2}] = fix_range_end([{0,2}], 4),
    [{0,1}, {0,3}] = fix_range_end([{0,1}, {0,4}], 4),
    [{-1}, {0,2}] = fix_range_end([{-1}, {0,2}], 4),
    ok.

range_to_binary_test() ->
    ?debugMsg("Testing Range to Binary"),
    <<"0-99">> = range_to_binary([{0,99}],200),
    <<"100-199">> = range_to_binary([-100],200),
    <<"0-99,100-199">> = range_to_binary([{0,99},{100,199}],200),
    <<"50-199">> = range_to_binary([{50,infinity}],200),
    ok.
-endif.
