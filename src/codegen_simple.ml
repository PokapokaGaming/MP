(* Simplified code generator that faithfully reproduces mpfrp-original architecture *)
(* Focus: 3-tier architecture with dummy input nodes *)

open Syntax
open Util

(* === Core Helper Functions === *)

(* Import erlang_of_expr and related functions from codegen.ml *)
module Codegen = struct
  include Codegen
end

(* Generate party actor (same as original) *)
let gen_party_actor party_id leader_inst periodic_ms =
  let party_name = "party_" ^ String.lowercase_ascii party_id in
  let leader_name = String.lowercase_ascii leader_inst in
  Printf.sprintf
    "%s(Ver) ->\n\
    \    timer:sleep(%d),\n\
    \    %s ! {%s, Ver},\n\
    \    %s(Ver + 1).\n"
    party_name
    periodic_ms
    leader_name
    party_id
    party_name

(* Generate input node actor (dummy) - forwards to computation nodes *)
(* This corresponds to mpfrp-original's in_node function *)
let gen_input_node_dummy inst_id input_name downstream_nodes =
  let actor_name = String.lowercase_ascii inst_id ^ "_" ^ input_name in
  let inst_p = "p" in (* Simplified - single party *)
  
  let send_statements = 
    if downstream_nodes = [] then
      "                        void"
    else
      String.concat ";\n" (List.map (fun target_node ->
        "                        " ^ String.lowercase_ascii inst_id ^ "_" ^ target_node ^ 
        " ! {{" ^ inst_p ^ ", Ver}, " ^ input_name ^ ", Val}"
      ) downstream_nodes)
  in
  
  Printf.sprintf
    "%s() ->\n\
    \    receive\n\
    \        {{%s, Ver}, Val} ->\n\
    %s;\n\
    \        _ ->\n\
    \            void\n\
    \    end,\n\
    \    %s().\n"
    actor_name
    inst_p
    send_statements
    actor_name

(* Generate module actor with In_buffer processing *)
(* This processes incoming data and forwards to dummy input nodes *)
let gen_module_actor inst_id input_names =
  let actor_name = String.lowercase_ascii inst_id in
  let inst_p = "p" in
  
  (* Generate In_buffer processing clauses *)
  let in_buffer_clauses = List.map (fun input_name ->
    Printf.sprintf
      "                {{%s, Ver}, %s, Val} when Ver =:= P_verT ->\n\
      \                    %s ! {{%s, Ver}, Val},\n\
      \                    {Buf, P_verT + 1}"
      inst_p
      input_name
      (String.lowercase_ascii inst_id ^ "_" ^ input_name)
      inst_p
  ) input_names in
  
  let in_foldl_body =
    if in_buffer_clauses = [] then
      "        {NInBuffer, P_verN} = {[], P_ver0}"
    else
      Printf.sprintf
        "        {NInBuffer, P_verN} = lists:foldl(fun (Msg, {Buf, P_verT}) ->\n\
        \            case Msg of\n\
        %s;\n\
        \                _ ->\n\
        \                    {Buf, P_verT}\n\
        \            end\n\
        \        end, {[], P_ver0}, In_buffer)"
        (String.concat ";\n" in_buffer_clauses)
  in
  
  Printf.sprintf
    "%s(Ver_buffer, In_buffer, P_ver) ->\n\
    %s,\n\
    \    receive\n\
    \        {_, _} = Ver_msg ->\n\
    \            %s(lists:reverse([Ver_msg | []]), NInBuffer, P_verN);\n\
    \        {_, _, _} = In_msg ->\n\
    \            %s(Ver_buffer, lists:reverse([In_msg | NInBuffer]), P_verN)\n\
    \    end.\n"
    actor_name
    in_foldl_body
    actor_name
    actor_name

(* Generate start/0 function *)
let gen_start party_id inst_list =
  let party_name = "party_" ^ String.lowercase_ascii party_id in
  
  let module_spawns = List.map (fun (inst_id, _) ->
    let actor_name = String.lowercase_ascii inst_id in
    Printf.sprintf "    register(%s, spawn(?MODULE, %s, [[], [], 0]))" actor_name actor_name
  ) inst_list in
  
  let node_spawns = List.concat_map (fun (inst_id, input_names) ->
    List.map (fun input_name ->
      let node_name = String.lowercase_ascii inst_id ^ "_" ^ input_name in
      Printf.sprintf "    register(%s, spawn(?MODULE, %s, []))" node_name node_name
    ) input_names
  ) inst_list in
  
  let party_spawn = Printf.sprintf "    register(%s, spawn(?MODULE, %s, [0]))" party_name party_name in
  
  "start() ->\n" ^
  String.concat ",\n" (module_spawns @ node_spawns @ [party_spawn]) ^
  ".\n"

(* Generate complete Erlang module *)
let generate_erlang module_name party_id leader_inst periodic_ms inst_list =
  let module_header = Printf.sprintf "-module(%s).\n" (String.lowercase_ascii module_name) in
  let export = "-export([start/0]).\n\n" in
  
  (* Generate party actor *)
  let party_code = gen_party_actor party_id leader_inst periodic_ms in
  
  (* Generate module actors *)
  let module_actors = List.map (fun (inst_id, input_names) ->
    gen_module_actor inst_id input_names
  ) inst_list in
  
  (* Generate input node dummies *)
  let input_nodes = List.concat_map (fun (inst_id, input_names) ->
    List.map (fun input_name ->
      gen_input_node_dummy inst_id input_name [] (* TODO: Get downstream nodes *)
    ) input_names
  ) inst_list in
  
  (* Generate start function *)
  let start_code = gen_start party_id inst_list in
  
  module_header ^ export ^
  start_code ^ "\n\n" ^
  party_code ^ "\n" ^
  String.concat "\n\n" module_actors ^ "\n\n" ^
  String.concat "\n\n" input_nodes

end
