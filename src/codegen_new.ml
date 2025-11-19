open Syntax
open Util
open Module

module M = Map.Make(String)

(* === Party Syntax to Old Newnode Format Converter === *)

(* Extract instance name from qualified_id *)
let get_instance_name = function
  | SimpleId id -> id
  | QualifiedId (_, id) -> id

(* Convert party_block to newnode format *)
let convert_party_to_newnode party_block =
  (* newnode format: (outputs, module_id, parties, inputs) *)
  let party_id = party_block.party_id in
  List.map (fun (outputs, module_id, inputs) ->
    let input_names = List.map get_instance_name inputs in
    (* Assign party_id to all instances in this party *)
    (outputs, module_id, [party_id], input_names)
  ) party_block.instances

(* Main entry point: convert party syntax and call existing gen_inst *)
let gen_new_inst inst_prog module_map =
  match inst_prog.parties with
  | [] -> failwith "No party blocks found in instance file"
  | party_block :: _ ->
      (* Convert to newnode format *)
      let newnode = convert_party_to_newnode party_block in
      
      (* Build id2party map: map each instance output to its party *)
      let id2party = List.fold_left (fun acc (outputs, _, _, _) ->
        List.fold_left (fun acc' output -> M.add output party_block.party_id acc') acc outputs
      ) M.empty newnode in
      
      (* Enrich module_map with party information *)
      (* For each module used in newnode, assign party to all its nodes and add party to module.party *)
      let enriched_module_map = List.fold_left (fun acc_map (_, module_id, _, _) ->
        let mod_info = try M.find module_id acc_map with Not_found -> failwith ("Module not found: " ^ module_id) in
        (* Assign party_id to all nodes (source + sink + node names) in this module *)
        let all_node_names = mod_info.source @ mod_info.sink @ (List.map (fun (id, _, _, _) -> id) mod_info.node) in
        (* Filter out empty strings *)
        let all_node_names = List.filter (fun s -> s <> "") all_node_names in
        let enriched_id2party = List.fold_left (fun m node_id ->
          M.add node_id party_block.party_id m
        ) mod_info.id2party all_node_names in
        (* Add party_id to module.party list if not already present *)
        let enriched_party = if List.mem party_block.party_id mod_info.party then mod_info.party else party_block.party_id :: mod_info.party in
        let enriched_mod = { mod_info with id2party = enriched_id2party; party = enriched_party } in
        M.add module_id enriched_mod acc_map
      ) module_map newnode in
      
      (* Create pseudo-module for gen_inst *)
      let inst_module = {
        id = party_block.party_id;
        party = [party_block.party_id];
        source = [];
        extern_input = [];
        sink = [];
        const = [];
        func = [];
        node = [];
        newnode = newnode;
        (* leader format: (party_id, leader_instance_id, [periodic_expr]) *)
        leader = [(party_block.party_id, party_block.leader, [EConst(CInt(party_block.periodic_ms))])];
        typeinfo = M.empty;
        id2party = id2party;
      } in
      
      (* Delegate to existing Codegen.gen_inst with enriched module_map *)
      Printf.eprintf "Calling Codegen.gen_inst...\n";
      try
        let result = Codegen.gen_inst inst_module enriched_module_map in
        Printf.eprintf "Result length: %d bytes\n" (String.length result);
        result
      with
      | Util.UnknownId(id) ->
          Printf.eprintf "ERROR in Codegen.gen_inst: UnknownId(\"%s\")\n" id;
          Printf.eprintf "Backtrace:\n%s\n" (Printexc.get_backtrace ());
          raise (Util.UnknownId(id))
      | e ->
          Printf.eprintf "ERROR in Codegen.gen_inst: %s\n" (Printexc.to_string e);
          Printf.eprintf "Backtrace:\n%s\n" (Printexc.get_backtrace ());
          raise e
