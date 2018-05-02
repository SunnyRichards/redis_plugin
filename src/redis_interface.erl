%%%-------------------------------------------------------------------
%%% @author sunnyrichards
%%% @copyright (C) 2018, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 14. Mar 2018 6:54 PM
%%%-------------------------------------------------------------------
-module(redis_interface).
-author("sunnyrichards").

-record(alert_info, {
  alert_initiation_status = true :: boolean(),
  low_battery_alert = false :: boolean(),
  critical_battery_alert = false :: boolean(),
  geo_source_alert = true :: boolean(),
  geo_in_transit_alert = true :: boolean(),
  geo_destination_alert = true :: boolean(),
  geo_forbidden_alert = false :: boolean(),
  idle_state_alert = false :: boolean(),
  alarm_report_alert = false :: boolean(),
  status_report_alert = false :: boolean(),
  device_lock_alert = true :: boolean()
}).

%% API
-export([connect/0]).
-export([create_alert/1]).

-export([insert_single_pair/2,read_single_pair/1,delete_single_pair/1]).
-export([insert_multiple_pair/1,insert_multiple_key_values/2,read_multiple_pair/1,delete_multiple_pair/1]).

-export([insert_hash/2,read_entire_hash/1,update_single_hash_field/3,delete_hash_fields/2]).
-export([read_single_hash_field/2, read_multiple_hash_field/2, read_all_keys/1, read_all_values/1]).

-export([delete_entire_hash/1,read_multiple_pair/0]).


connect() ->
  {ok, Connection} = eredis:start_link(),
  Connection.


%%%---------------------------------------------------------------------------------------------------------------------
%%% SINGLE PAIR
%%%---------------------------------------------------------------------------------------------------------------------

insert_single_pair(Key,Value) ->
  {ok, Connection} = eredis:start_link(),
  eredis:q(Connection, ["SET" ,Key,Value]).

read_single_pair(Key) ->
  {ok, Connection} = eredis:start_link(),
  eredis:q(Connection, ["GET" ,Key]).

delete_single_pair(Key) ->
  {ok, Connection} = eredis:start_link(),
  eredis:q(Connection, ["DEL" ,Key]).

%%%---------------------------------------------------------------------------------------------------------------------
%%%  MULTIPLE PAIR
%%%---------------------------------------------------------------------------------------------------------------------

insert_multiple_pair(KeyValuePairs)  when is_list(KeyValuePairs)->
  {ok, Connection} = eredis:start_link(),
  eredis:q(Connection, ["MSET" | KeyValuePairs]).

insert_multiple_key_values(Keys,Values) when is_list(Keys) and is_list(Values) and length(Keys)=:=length(Values) ->
  KeyValuePairs = make_pairs(Keys,Values,[]),
  insert_multiple_pair(KeyValuePairs).

read_multiple_pair(KeyList)  when is_list(KeyList)->
  {ok, Connection} = eredis:start_link(),
  eredis:q(Connection, ["MGET" | KeyList]).

read_multiple_pair() ->
  {ok, Connection} = eredis:start_link(),
  eredis:q(Connection, ["KEYS", <<$*>>]).

delete_multiple_pair(KeyList)  when is_list(KeyList)->
  {ok, Connection} = eredis:start_link(),
  eredis:q(Connection, ["DEL" | KeyList]).

%%%---------------------------------------------------------------------------------------------------------------------
%%% HASH MAPS
%%%---------------------------------------------------------------------------------------------------------------------

insert_hash(Key,ValuePairs)  when is_list(ValuePairs)->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HMSET", Key | ValuePairs]).

read_single_hash_field(Key,Field) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HGET", Key, Field]).

read_multiple_hash_field(Key,FieldList) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HMGET", Key | FieldList]).

read_entire_hash(Key) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HGETALL", Key]).

read_all_keys(Key) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HKEYS", Key]).

read_all_values(Key) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HVALS", Key]).

update_single_hash_field(Key,Field,Value) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HSET", Key, Field, Value]).

delete_hash_fields(Key,FieldList) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["HDEL", Key | FieldList]).

delete_entire_hash(Key) ->
  {ok, Connection} = eredis:start_link(localhost,6379),
  eredis:q(Connection, ["DEL", Key]).


%%%---------------------------------------------------------------------------------------------------------------------
%%% Internal Functions
%%%---------------------------------------------------------------------------------------------------------------------

create_alert(Key) ->
{ok, Connection} = eredis:start_link("13.127.111.209",6379),
FieldList = record_info(fields, alert_info),
[_RecordName|ValueList] = tuple_to_list(#alert_info{}),
ValuePairs = make_pairs(FieldList,ValueList,[]),
eredis:q(Connection, ["HMSET", Key | ValuePairs]).

make_pairs([],[],Result) -> Result;

make_pairs([Field|FList],[Value|VList],Acc) when is_float(Value) ->
  make_pairs(FList,VList,Acc ++ [Field,float_to_list(Value)]);

make_pairs([Field|FList],[Value|VList],Acc) ->
  make_pairs(FList,VList,Acc ++ [Field,Value]).

