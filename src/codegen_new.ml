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
      
      (* Find the leader module *)
      let leader_module_id = 
        let rec find_leader = function
          | [] -> failwith ("Leader instance not found: " ^ party_block.leader)
          | (outputs, module_id, _, _) :: rest ->
              if List.mem party_block.leader outputs then module_id
              else find_leader rest
        in find_leader newnode
      in
      
      (* Enrich module_map with party information *)
      (* Strategy: Add dummy party info to modules, then override with actual party from instance file *)
      let enriched_module_map = List.fold_left (fun acc_map (_, module_id, _, _) ->
        let mod_info = try M.find module_id acc_map with Not_found -> failwith ("Module not found: " ^ module_id) in
        
        (* If module already has party info (from original syntax), keep it *)
        if mod_info.party <> [] && not (M.is_empty mod_info.id2party) then
          (* Module already has party annotations - override them with instance party *)
          let enriched_id2party = M.map (fun _ -> party_block.party_id) mod_info.id2party in
          let enriched_nodes = List.map (fun (id, _, init, expr) ->
            (id, party_block.party_id, init, expr)
          ) mod_info.node in
          let enriched_mod = { mod_info with 
            id2party = enriched_id2party; 
            party = [party_block.party_id];
            node = enriched_nodes
          } in
          M.add module_id enriched_mod acc_map
        else
          (* Module has no party info - add dummy party to all nodes *)
          let all_node_names = mod_info.source @ mod_info.sink @ (List.map (fun (id, _, _, _) -> id) mod_info.node) in
          let all_node_names = List.filter (fun s -> s <> "") all_node_names in
          let enriched_id2party = List.fold_left (fun m node_id ->
            M.add node_id party_block.party_id m
          ) M.empty all_node_names in
          (* Also update node definitions to include party *)
          let enriched_nodes = List.map (fun (id, p, init, expr) ->
            let new_p = if p = "" then party_block.party_id else p in
            (id, new_p, init, expr)
          ) mod_info.node in
          (* Add leader info if this is the leader module *)
          let enriched_leader = 
            if module_id = leader_module_id then
              [(party_block.party_id, "periodic", [EConst(CInt(party_block.periodic_ms))])]
            else
              mod_info.leader
          in
          let enriched_mod = { mod_info with 
            id2party = enriched_id2party; 
            party = [party_block.party_id];
            node = enriched_nodes;
            leader = enriched_leader
          } in
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
      Codegen.gen_inst inst_module enriched_module_map
