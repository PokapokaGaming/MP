(* === Copy & Parametrize Strategy ===
 * 
 * This file implements dynamic process generation while preserving
 * mpfrp-original's message flow and data flow logic.
 * 
 * Key principle: Copy original's code generation logic and parametrize
 * static values (inst_id, party) to enable runtime instantiation.
 *)

(* === Utility functions === *)

module M = Map.Make(String)

exception UnknownId of string

let try_find id m = begin
  try M.find id m with
    Not_found -> (raise (UnknownId(id)))
end

let is_empty lst = lst = []
let concat_map s f l = String.concat s (List.map f l)

(* === Helper functions from original === *)

let send target e =
  target ^ " ! " ^ e

let indent n s =
  String.make (n * 4) ' ' ^ s

(* === Input Node Actor Generation (Parametrized version of original's in_node) ===
 * 
 * Original: uncapitalize_inst_id ^ "_" ^ id ^ "() ->"
 * New:      module_name ^ "_input_" ^ id ^ "(Party, InstanceId) ->"
 * 
 * Message flow: Same as original
 * Difference: Dynamic resolution of downstream node names
 *)

let gen_input_node_actor module_name input_id downstream_nodes =
  let actor_func = String.uncapitalize_ascii module_name ^ "_input_" ^ input_id in
  
  (* Generate send statements to downstream nodes *)
  let send_code = 
    if downstream_nodes = [] then
      indent 3 "void;\n"
    else
      concat_map
        ",\n"
        (fun dst_node ->
          (* Dynamic resolution: list_to_atom(Party ++ "_" ++ InstanceId ++ "_" ++ NodeId) *)
          indent 3 ("DownstreamNode = list_to_atom(atom_to_list(Party) ++ \"_\" ++ atom_to_list(InstanceId) ++ \"_" ^ dst_node ^ "\"),") ^
          "\n" ^
          indent 3 (send "DownstreamNode" ("{{Party, Ver}, " ^ input_id ^ ", Val}")))
        downstream_nodes
    ^ ";\n"
  in
  
  (* Function definition - same structure as original *)
  actor_func ^ "(Party, InstanceId) ->\n"
  ^ indent 1 "receive\n"
  ^ indent 2 "{{Party, Ver}, Val} ->\n"
  ^ send_code
  ^ indent 2 "_ ->\n"
  ^ indent 3 "void\n"
  ^ indent 1 "end,\n"
  ^ indent 1 (actor_func ^ "(Party, InstanceId).")

(* === Module Actor Generation (Parametrized version of original's module actor) ===
 * 
 * Original: inst_id(Ver_buffer, In_buffer, p, P_ver) ->
 * New:      module_name_module(Ver_buffer, In_buffer, Party, P_ver, InstanceId) ->
 * 
 * Key: In_buffer processing - forwards to dummy input nodes
 *)

let gen_module_actor_in_buffer module_name input_names =
  let actor_func = String.uncapitalize_ascii module_name ^ "_module" in
  
  (* Generate In_buffer processing clauses - one per input *)
  (* This is from original's request_buf_compare_ver_code *)
  let in_buffer_clauses = List.map (fun input_name ->
    (* Dynamic input node resolution *)
    let send_to_input_node =
      "InputNodeActor = list_to_atom(atom_to_list(Party) ++ \"_\" ++ atom_to_list(InstanceId) ++ \"_" ^ input_name ^ "\"),\n" ^
      indent 4 (send "InputNodeActor" "{{Party, Ver}, Val},")
    in
    
    (* Ver > P_verT: buffer it *)
    indent 3 ("{{Party, Ver}, " ^ input_name ^ ", Val} when Ver > P_verT ->\n") ^
    indent 4 "{[Msg | Buf], P_verT};\n" ^
    (* Ver =:= P_verT: forward to input node dummy and downstream modules *)
    indent 3 ("{{Party, Ver}, " ^ input_name ^ ", Val} when Ver =:= P_verT ->\n") ^
    indent 4 send_to_input_node ^ "\n" ^
    indent 4 "lists:foreach(fun(Module) -> Module ! {Party, Ver} end, DownstreamModules),\n" ^
    indent 4 "{Buf, P_verT + 1};\n" ^
    (* Ver < P_verT: old message, just forward to input node *)
    indent 3 ("{{Party, Ver}, " ^ input_name ^ ", Val} when Ver < P_verT ->\n") ^
    indent 4 send_to_input_node ^ "\n" ^
    indent 4 "{Buf, P_verT}"
  ) input_names in
  
  let in_foldl_body =
    if in_buffer_clauses = [] then
      "{NInBuffer, P_verN} = {[], P_ver}"
    else
      "{NInBuffer, P_verN} = lists:foldl(fun (Msg, {Buf, P_verT}) ->\n"
      ^ indent 2 "case Msg of\n"
      ^ String.concat ";\n" in_buffer_clauses ^ ";\n"
      ^ indent 3 "_ ->\n"
      ^ indent 4 "{Buf, P_verT}\n"
      ^ indent 2 "end\n"
      ^ indent 1 "end, {[], P_ver}, In_buffer)"
  in
  
  (* Generate Ver_buffer processing *)
  let ver_foldl_body =
    "{NBuffer, P_ver0} = lists:foldl(fun (Version, {Buf, P_verT}) ->\n"
    ^ indent 2 "case Version of\n"
    ^ indent 3 "{Party, Ver} when Ver > P_verT ->\n"
    ^ indent 4 "{[{Party, Ver} | Buf], P_verT};\n"
    ^ indent 3 "{Party, Ver} when Ver =:= P_verT ->\n"
    ^ indent 4 "lists:foreach(fun(Module) -> Module ! {Party, Ver} end, DownstreamModules),\n"
    ^ indent 4 "lists:foreach(fun(NodeActor) -> NodeActor ! {request, {Party, Ver}} end, NodeActors),\n"
    ^ indent 4 "{Buf, P_verT + 1};\n"
    ^ indent 3 "{Party, Ver} when Ver < P_verT ->\n"
    ^ indent 4 "{Buf, P_verT};\n"
    ^ indent 3 "_ ->\n"
    ^ indent 4 "{Buf, P_verT}\n"
    ^ indent 2 "end\n"
    ^ indent 1 "end, {[], P_ver}, Ver_buffer)"
  in
  
  (* Complete module actor function *)
  actor_func ^ "(Ver_buffer, In_buffer, Party, P_ver, InstanceId, DownstreamModules, NodeActors) ->\n"
  ^ indent 1 ver_foldl_body ^ ",\n"
  ^ indent 1 in_foldl_body ^ ",\n"
  ^ indent 1 "receive\n"
  ^ indent 2 "{_, _} = Ver_msg ->\n"
  ^ indent 3 (actor_func ^ "(lists:reverse([Ver_msg | NBuffer]), NInBuffer, Party, P_verN, InstanceId, DownstreamModules, NodeActors);\n")
  ^ indent 2 "{_, _, _} = In_msg ->\n"
  ^ indent 3 (actor_func ^ "(NBuffer, lists:reverse([In_msg | NInBuffer]), Party, P_verN, InstanceId, DownstreamModules, NodeActors)\n")
  ^ indent 1 "end.\n"

(* === Computation Node Actor Generation (Parametrized version of original's def_node) ===
 * 
 * Original: inst_id ^ "_" ^ node_id ^ "(Buffer, NextVer, Processed, ReqBuffer, Deferred) ->"
 * New:      module_name ^ "_node_" ^ node_id ^ "(Party, InstanceId, State, DownstreamModules, IntraModuleNodes) ->"
 * 
 * Key: Two types of destinations:
 *   1. Downstream module actors (inter-module communication)
 *   2. Intra-module nodes (intra-module communication)
 *)

let gen_computation_node_actor module_name node_id computation_expr downstream_modules intra_module_nodes =
  let actor_func = String.uncapitalize_ascii module_name ^ "_node_" ^ node_id in
  
  (* Generate send to downstream modules *)
  let send_to_modules =
    if downstream_modules = [] then
      ""
    else
      indent 4 "lists:foreach(fun({TargetModule, InputName}) ->\n" ^
      indent 5 "TargetModule ! {{Party, Ver}, InputName, Curr}\n" ^
      indent 4 "end, DownstreamModules),\n"
  in
  
  (* Generate send to intra-module nodes *)
  let send_to_nodes =
    if intra_module_nodes = [] then
      indent 4 "void,\n"
    else
      indent 4 "lists:foreach(fun(NodeId) ->\n" ^
      indent 5 "NodeActor = list_to_atom(atom_to_list(Party) ++ \"_\" ++ atom_to_list(InstanceId) ++ \"_\" ++ NodeId),\n" ^
      indent 5 "NodeActor ! {{Party, Ver}, " ^ node_id ^ ", Curr}\n" ^
      indent 4 "end, [" ^ String.concat ", " (List.map (fun n -> "\"" ^ n ^ "\"") intra_module_nodes) ^ "]),\n"
  in
  
  (* Complete node actor - simplified version *)
  actor_func ^ "(Party, InstanceId, State, DownstreamModules, IntraModuleNodes) ->\n"
  ^ indent 1 "receive\n"
  ^ indent 2 "{request, {Party, Ver}} ->\n"
  ^ indent 3 "Curr = " ^ computation_expr ^ ",\n"
  ^ indent 3 "io:format(\"COMPUTED[~s_~s]=~p~n\", [atom_to_list(Party), atom_to_list(InstanceId), Curr]),\n"
  ^ send_to_modules
  ^ send_to_nodes
  ^ indent 3 (actor_func ^ "(Party, InstanceId, Curr, DownstreamModules, IntraModuleNodes);\n")
  ^ indent 2 "{{Party, Ver}, InputName, Val} ->\n"
  ^ indent 3 "%% Receive input from upstream\n"
  ^ indent 3 (actor_func ^ "(Party, InstanceId, Val, DownstreamModules, IntraModuleNodes)\n")
  ^ indent 1 "end.\n"

(* === Test: Generate simple example === *)

let test_gen_input_node () =
  gen_input_node_actor "Counter" "trigger" ["data"]

let test_gen_module () =
  gen_module_actor_in_buffer "Counter" ["trigger"]

let test_gen_computation_node () =
  gen_computation_node_actor "Counter" "data" "State + 1" 
    [("doubler", "count")] (* downstream module: doubler's count input *)
    [] (* no intra-module nodes *)

(* Print test output *)
let () =
  print_endline "=== Input Node Actor (Dummy) ===";
  print_endline (test_gen_input_node ());
  print_endline "\n=== Module Actor ===";
  print_endline (test_gen_module ());
  print_endline "\n=== Computation Node Actor ===";
  print_endline (test_gen_computation_node ());
