open Syntax
open Util

(* Generate Erlang code from new inst_program structure *)

(* Helper: Convert qualified_id to string *)
let string_of_qualified_id = function
  | SimpleId id -> id
  | QualifiedId (party_id, inst_id) -> party_id ^ "_" ^ inst_id

(* Generate party actor code *)
let gen_party_actor party =
  let party_name = String.uncapitalize_ascii party.party_id in
  let leader_ref = string_of_qualified_id (SimpleId party.leader) in
  Printf.sprintf
    "party_%s() ->\n\
    \    timer:sleep(%d),\n\
    \    %s ! sync_pulse,\n\
    \    party_%s().\n"
    party_name
    party.periodic_ms
    leader_ref
    party_name

(* Generate module actor skeleton *)
let gen_module_actor_skeleton inst_id module_name =
  Printf.sprintf
    "%s_module() ->\n\
    \    %% TODO: Implement module actor for %s\n\
    \    receive\n\
    \        _ -> ok\n\
    \    end.\n"
    (String.uncapitalize_ascii inst_id)
    module_name

(* Generate start function *)
let gen_start parties =
  let spawn_parties = List.map (fun p ->
    Printf.sprintf "    spawn(fun party_%s/0)" 
      (String.uncapitalize_ascii p.party_id)
  ) parties in
  Printf.sprintf
    "start() ->\n\
    %s.\n"
    (String.concat ",\n" spawn_parties)

(* Generate exports *)
let gen_exports parties =
  "-export([start/0]).\n" ^
  String.concat "" (List.map (fun p ->
    Printf.sprintf "-export([party_%s/0]).\n" 
      (String.uncapitalize_ascii p.party_id)
  ) parties)

(* Main code generation function *)
let gen_new_inst inst_prog module_map =
  let module_header = "-module(main).\n" in
  let exports = gen_exports inst_prog.parties in
  let party_actors = String.concat "\n" (List.map gen_party_actor inst_prog.parties) in
  let start_func = gen_start inst_prog.parties in
  
  module_header ^ exports ^ "\n" ^ party_actors ^ "\n" ^ start_func
