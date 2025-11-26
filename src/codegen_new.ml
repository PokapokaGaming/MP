open Syntax
open Util
open Module

module M = Map.Make(String)
module StringSet = Set.Make(String)

(* ============================================================================
   New Architecture Code Generator
   Based on g_ideal.erl prototype with topology injection pattern
   
   Key Design Principles:
   1. Generate module-level generic actor functions (not per-instance)
   2. Inject computation logic as anonymous functions in Config
   3. Do NOT generate actors for source nodes (input ports)
   4. NodeSpecs contains only actual compute nodes and their dependencies
   ============================================================================ *)

(* === Helper Functions === *)

let get_instance_name qid = match qid with
  | Syntax.SimpleId id -> id
  | Syntax.QualifiedId (_, id) -> id

(* Extract unique module IDs from inst_prog *)
let get_unique_modules inst_prog =
  let modules = ref StringSet.empty in
  List.iter (fun party_block ->
    List.iter (fun (_, module_id, _) ->
      modules := StringSet.add module_id !modules
    ) party_block.Syntax.instances
  ) inst_prog.Syntax.parties;
  StringSet.elements !modules

(* Convert expression to Erlang code string *)
let rec expr_to_erlang = function
  | Syntax.EConst Syntax.CUnit -> "void"
  | Syntax.EConst (Syntax.CBool b) -> string_of_bool b
  | Syntax.EConst (Syntax.CInt i) -> string_of_int i
  | Syntax.EConst (Syntax.CFloat f) -> Printf.sprintf "%f" f
  | Syntax.EConst (Syntax.CChar c) -> string_of_int (int_of_char c)
  | Syntax.EId v -> "maps:get(" ^ v ^ ", Inputs, 0)"
  | Syntax.EAnnot (v, _) -> "maps:get(" ^ v ^ ", Inputs, 0)"
  | Syntax.EBin (Syntax.BAdd, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " + " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BSub, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " - " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BMul, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " * " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BDiv, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " / " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BMod, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " rem " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BLt, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " < " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BLte, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " =< " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BGt, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " > " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BGte, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " >= " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BEq, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " =:= " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BNe, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " =/= " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BAnd, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " band " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BOr, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " bor " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BLAnd, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " andalso " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EBin (Syntax.BLOr, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " orelse " ^ expr_to_erlang e2 ^ ")"
  | Syntax.EUni (Syntax.UNot, e) -> "(not " ^ expr_to_erlang e ^ ")"
  | Syntax.EUni (Syntax.UNeg, e) -> "(-" ^ expr_to_erlang e ^ ")"
  | Syntax.EIf (e1, e2, e3) -> 
      "(case " ^ expr_to_erlang e1 ^ " of true -> " ^ expr_to_erlang e2 ^ "; false -> " ^ expr_to_erlang e3 ^ " end)"
  | _ -> "0"

(* === Code Generation Functions === *)

(* Generate helper macros and functions *)
let gen_helpers () =
  String.concat "\n" [
    "-define(SORTBuffer, fun ({{K1, V1}, _}, {{K2, V2}, _}) ->";
    "    if V1 == V2 -> K1 < K2; true -> V1 < V2 end";
    "end).";
    "";
    "-define(SORTVerBuffer, fun ({P1, V1}, {P2, V2}) ->";
    "    if P1 == P2 -> V1 < V2; true -> P1 < P2 end";
    "end).";
    "";
    "-define(SORTInBuffer, fun ({{P1, V1}, _, _}, {{P2, V2}, _, _}) ->";
    "    if P1 == P2 -> V1 < V2; true -> P1 < P2 end";
    "end).";
    "";
    "buffer_update(Current, Last, {{RVId, RVersion}, Id, RValue}, Buffer) ->";
    "    H1 = case lists:member(Id, Current) of";
    "        true  -> maps:update_with({RVId, RVersion},";
    "                    fun(M) -> M#{Id => RValue} end,";
    "                    #{Id => RValue}, Buffer);";
    "        false -> Buffer";
    "    end,";
    "    case lists:member(Id, Last) of";
    "        true  -> maps:update_with({RVId, RVersion + 1},";
    "                    fun(M) -> M#{{last, Id} => RValue} end,";
    "                    #{{last, Id} => RValue}, H1);";
    "        false -> H1";
    "    end.";
    "";
    "periodic(Interval) ->";
    "    timer:sleep(Interval).";
  ]

(* Generate get_request_nodes/3 function *)
let gen_get_request_nodes () =
  String.concat "\n" [
    "get_request_nodes(NodeSpecs, ModuleNodes, ModuleParty) ->";
    "    lists:filter(fun(NodeName) ->";
    "        case maps:get(NodeName, NodeSpecs, undefined) of";
    "            undefined -> false;";
    "            #{party := NodeParty, module := NodeModule, dependencies := Dependencies} ->";
    "                case NodeParty of";
    "                    ModuleParty ->";
    "                        lists:all(fun(DepName) ->";
    "                            case maps:get(DepName, NodeSpecs, undefined) of";
    "                                undefined -> false;";
    "                                #{module := DepModule} -> DepModule =:= NodeModule";
    "                            end";
    "                        end, Dependencies);";
    "                    _ -> false";
    "                end";
    "        end";
    "    end, ModuleNodes).";
  ]

(* Generate run_party/2 function *)
let gen_run_party () =
  String.concat "\n" [
    "run_party(Config, Ver) ->";
    "    #{party := Party, leader := Leader, mode := Mode, subscribers := Subscribers} = Config,";
    "    case Mode of";
    "        periodic ->";
    "            #{interval := Interval} = Config,";
    "            Leader ! {Party, Ver},";
    "            lists:foreach(fun(Subscriber) ->";
    "                Subscriber ! {Party, Ver}";
    "            end, Subscribers),";
    "            timer:sleep(Interval),";
    "            run_party(Config, Ver + 1);";
    "        any_party ->";
    "            #{dependencies := Dependencies} = Config,";
    "            receive";
    "                {DepParty, _DepVer} ->";
    "                    case lists:member(DepParty, Dependencies) of";
    "                        true ->";
    "                            Leader ! {Party, Ver},";
    "                            lists:foreach(fun(Subscriber) ->";
    "                                Subscriber ! {Party, Ver}";
    "                            end, Subscribers),";
    "                            run_party(Config, Ver + 1);";
    "                        false ->";
    "                            run_party(Config, Ver)";
    "                    end";
    "            end";
    "    end.";
  ]

(* Generate generic module actor function for a module *)
let gen_module_actor module_id =
  let func_name = "run_" ^ module_id in
  String.concat "\n" [
    func_name ^ "(Ver_buffer, In_buffer, Party, Party_ver, Config, SyncDownstreams) ->";
    "    #{nodes := Nodes, request_targets := RequestTargets} = Config,";
    "    ";
    "    %% Process version buffer (sync pulses)";
    "    Sorted_ver_buf = lists:sort(?SORTVerBuffer, Ver_buffer),";
    "    {NBuffer, Party_ver1} = lists:foldl(fun(Version, {Buf, Party_verT}) ->";
    "        case Version of";
    "            {P, Ver} when P =:= Party andalso Ver > Party_verT ->";
    "                {[{P, Ver} | Buf], Party_verT};";
    "            {P, Ver} when P =:= Party andalso Ver =:= Party_verT ->";
    "                %% Forward sync pulse to downstream modules";
    "                lists:foreach(fun(ModulePid) ->";
    "                    ModulePid ! {Party, Ver}";
    "                end, SyncDownstreams),";
    "                ";
    "                %% Forward request only to target node actors";
    "                lists:foreach(fun(NodeName) ->";
    "                    NodeName ! {request, {Party, Ver}}";
    "                end, RequestTargets),";
    "                ";
    "                {Buf, Party_verT + 1};";
    "            {P, Ver} when P =:= Party andalso Ver < Party_verT ->";
    "                {Buf, Party_verT};";
    "            _ ->";
    "                {Buf, Party_verT}";
    "        end";
    "    end, {[], Party_ver}, Sorted_ver_buf),";
    "    ";
    "    %% Process input buffer (data messages)";
    "    Sorted_in_buf = lists:sort(?SORTInBuffer, In_buffer),";
    "    {NInBuffer, Party_verN} = lists:foldl(fun(Msg, {Buf, Party_verT}) ->";
    "        case Msg of";
    "            {{P, _Ver}, _InputName, _Value} when P =:= Party ->";
    "                %% Forward data to all managed node actors";
    "                lists:foreach(fun(NodeName) ->";
    "                    NodeName ! Msg";
    "                end, Nodes),";
    "                {Buf, Party_verT};";
    "            _ ->";
    "                {Buf, Party_verT}";
    "        end";
    "    end, {[], Party_ver1}, Sorted_in_buf),";
    "    ";
    "    receive";
    "        {update_sync_downstreams, NewSyncDownstreams} ->";
    "            " ^ func_name ^ "(NBuffer, NInBuffer, Party, Party_verN, Config, NewSyncDownstreams);";
    "        {_, _} = Ver_msg ->";
    "            " ^ func_name ^ "(lists:reverse([Ver_msg | NBuffer]), NInBuffer, Party, Party_verN, Config, SyncDownstreams);";
    "        {_, _, _} = In_msg ->";
    "            " ^ func_name ^ "(NBuffer, lists:reverse([In_msg | NInBuffer]), Party, Party_verN, Config, SyncDownstreams)";
    "    end.";
  ]

(* Generate generic node actor function for a module's node *)
let gen_node_actor module_id node_id has_deps =
  let func_name = "run_" ^ module_id ^ "_" ^ node_id in
  
  if has_deps then
    (* Compute node with dependencies - supports cross-party synchronization *)
    (* Based on Original's pattern: use Processed map to accumulate values from different parties *)
    String.concat "\n" [
      func_name ^ "(Config, Connections, NodeState) ->";
      "    #{dependencies := Deps, register_name := RegName, compute := ComputeFn} = Config,";
      "    #{downstreams := Downstreams} = Connections,";
      "    #{buffer := Buffer0, next_ver := NextVer0, processed := Processed0, req_buffer := ReqBuffer0, deferred := Deferred0} = NodeState,";
      "    ";
      "    %% Process Buffer: Handle cross-party synchronization using Processed map";
      "    %% Each party's data is stored with {Party, Ver} key";
      "    %% When a party's data is complete in buffer, merge to Processed and check if all deps ready";
      "    HL = lists:sort(?SORTBuffer, maps:to_list(Buffer0)),";
      "    {NBuffer, NextVerT, ProcessedT, DeferredT} = lists:foldl(fun(E, {Buffer, NextVer, Processed, Deferred}) ->";
      "        case E of";
      "            {{Party, Ver} = Version, InputMap} ->";
      "                %% Check sequential constraint for this party";
      "                CurrentNextVer = maps:get(Party, NextVer, 0),";
      "                case CurrentNextVer =:= Ver of";
      "                    true ->";
      "                        %% Merge this party's data into Processed";
      "                        MergedProcessed = maps:merge(Processed, InputMap),";
      "                        %% Check if all dependencies are now present in merged map";
      "                        AllPresent = lists:all(fun(DepName) ->";
      "                            maps:is_key(DepName, MergedProcessed)";
      "                        end, Deps),";
      "                        case AllPresent of";
      "                            true ->";
      "                                %% All deps ready - compute result";
      "                                case Deferred of";
      "                                    [] -> void;";
      "                                    _ ->";
      "                                        Result = ComputeFn(MergedProcessed),";
      "                                        io:format(\"[TRACE] Node:~p Ver:~p Event:Compute Payload:~p~n\", [trace_node_name(RegName), Ver, Result]),";
      "                                        out(RegName, Result),";
      "                                        lists:foreach(fun(Downstream) ->";
      "                                            case Downstream of";
      "                                                {DownstreamPid, PortTag} ->";
      "                                                    DownstreamPid ! {{Party, Ver}, PortTag, Result};";
      "                                                _ -> ok";
      "                                            end";
      "                                        end, Downstreams)";
      "                                end,";
      "                                {maps:remove(Version, Buffer), maps:update(Party, Ver + 1, NextVer), MergedProcessed, []};";
      "                            false ->";
      "                                %% Not all deps ready yet - save to Processed and add to Deferred";
      "                                {maps:remove(Version, Buffer), maps:update(Party, Ver + 1, NextVer), MergedProcessed, [Version | Deferred]}";
      "                        end;";
      "                    false ->";
      "                        {Buffer, NextVer, Processed, Deferred}";
      "                end;";
      "            _ ->";
      "                {Buffer, NextVer, Processed, Deferred}";
      "        end";
      "    end, {Buffer0, NextVer0, Processed0, Deferred0}, HL),";
      "    ";
      "    %% Process Request Buffer (for relay nodes triggered by request)";
      "    Sorted_req_buf = lists:sort(?SORTVerBuffer, ReqBuffer0),";
      "    {NNextVer, NProcessed, NReqBuffer, NDeferred} = lists:foldl(fun(E, {NextVer, Processed, ReqBuffer, Deferred}) ->";
      "        case E of";
      "            {Party, Ver} = Version ->";
      "                CurrentNextVer = maps:get(Party, NextVer, 0),";
      "                case CurrentNextVer =:= Ver of";
      "                    true ->";
      "                        %% Check if all deps are in Processed";
      "                        AllPresent = lists:all(fun(DepName) ->";
      "                            maps:is_key(DepName, Processed)";
      "                        end, Deps),";
      "                        case AllPresent of";
      "                            true ->";
      "                                Result = ComputeFn(Processed),";
      "                                io:format(\"[TRACE] Node:~p Ver:~p Event:Compute Payload:~p~n\", [trace_node_name(RegName), Ver, Result]),";
      "                                out(RegName, Result),";
      "                                lists:foreach(fun(Downstream) ->";
      "                                    case Downstream of";
      "                                        {DownstreamPid, PortTag} ->";
      "                                            DownstreamPid ! {{Party, Ver}, PortTag, Result};";
      "                                        _ -> ok";
      "                                    end";
      "                                end, Downstreams),";
      "                                {maps:update(Party, Ver + 1, NextVer), Processed, ReqBuffer, []};";
      "                            false ->";
      "                                {maps:update(Party, Ver + 1, NextVer), Processed, ReqBuffer, [Version | Deferred]}";
      "                        end;";
      "                    false ->";
      "                        {NextVer, Processed, [Version | ReqBuffer], Deferred}";
      "                end;";
      "            _ -> {NextVer, Processed, ReqBuffer, Deferred}";
      "        end";
      "    end, {NextVerT, ProcessedT, [], DeferredT}, Sorted_req_buf),";
      "    ";
      "    receive";
      "        {request, {Party, Ver}} ->";
      "            io:format(\"[TRACE] Node:~p Ver:~p Event:Request Payload:none~n\", [trace_node_name(RegName), Ver]),";
      "            " ^ func_name ^ "(Config, Connections,";
      "                NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed,";
      "                          req_buffer := lists:reverse([{Party, Ver} | NReqBuffer]), deferred := NDeferred});";
      "        {{Party, Ver}, InputName, Value} ->";
      "            io:format(\"[TRACE] Node:~p Ver:~p Event:Receive Payload:{~p,~p}~n\", [trace_node_name(RegName), Ver, InputName, Value]),";
      "            UpdatedBuffer = buffer_update(Deps, [], {{Party, Ver}, InputName, Value}, NBuffer),";
      "            " ^ func_name ^ "(Config, Connections,";
      "                NodeState#{buffer := UpdatedBuffer, next_ver := NNextVer, processed := NProcessed,";
      "                          req_buffer := NReqBuffer, deferred := NDeferred})";
      "    end.";
    ]
  else
    (* Source node (no dependencies) *)
    String.concat "\n" [
      func_name ^ "(Config, Connections, NodeState) ->";
      "    #{register_name := RegName, compute := ComputeFn} = Config,";
      "    #{downstreams := Downstreams} = Connections,";
      "    #{buffer := Buffer0, next_ver := NextVer0, processed := Processed0, req_buffer := ReqBuffer0, deferred := Deferred0} = NodeState,";
      "    ";
      "    %% Process Buffer (source nodes have no inputs)";
      "    HL = lists:sort(?SORTBuffer, maps:to_list(Buffer0)),";
      "    {NBuffer, NextVerT, ProcessedT, DeferredT} = lists:foldl(fun(_E, {Buffer, NextVer, Processed, Deferred}) ->";
      "        {Buffer, NextVer, Processed, Deferred}";
      "    end, {Buffer0, NextVer0, Processed0, Deferred0}, HL),";
      "    ";
      "    %% Process Request Buffer";
      "    Sorted_req_buf = lists:sort(?SORTVerBuffer, ReqBuffer0),";
      "    {NNextVer, NProcessed, NReqBuffer, NDeferred} = lists:foldl(fun(E, {NextVer, Processed, ReqBuffer, Deferred}) ->";
      "        case E of";
      "            {Party, Ver} = Version ->";
      "                CurrentNextVer = maps:get(Party, NextVer, 0),";
      "                case CurrentNextVer =:= Ver of";
      "                    true ->";
      "                        %% Source node: compute value using injected function";
      "                        Result = ComputeFn(#{}),";
      "                        io:format(\"[TRACE] Node:~p Ver:~p Event:Compute Payload:~p~n\", [trace_node_name(RegName), Ver, Result]),";
      "                        out(RegName, Result),";
      "                        ";
      "                        %% Send to downstreams";
      "                        lists:foreach(fun({DownstreamName, PortTag}) ->";
      "                            DownstreamName ! {{Party, Ver}, PortTag, Result}";
      "                        end, Downstreams),";
      "                        ";
      "                        {maps:update(Party, Ver + 1, NextVer), Processed, ReqBuffer, []};";
      "                    false ->";
      "                        {NextVer, Processed, [Version | ReqBuffer], Deferred}";
      "                end;";
      "            _ ->";
      "                {NextVer, Processed, ReqBuffer, Deferred}";
      "        end";
      "    end, {NextVerT, ProcessedT, [], DeferredT}, Sorted_req_buf),";
      "    ";
      "    receive";
      "        {request, {Party, Ver}} ->";
      "            io:format(\"[TRACE] Node:~p Ver:~p Event:Request Payload:none~n\", [trace_node_name(RegName), Ver]),";
      "            " ^ func_name ^ "(Config, Connections,";
      "                NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed,";
      "                          req_buffer := lists:reverse([{Party, Ver} | NReqBuffer]), deferred := NDeferred})";
      "    end.";
    ]

(* Generate all module and node actors for unique modules *)
let gen_all_actors inst_prog module_map =
  let unique_modules = get_unique_modules inst_prog in
  let actors = ref [] in
  
  List.iter (fun module_id ->
    let mod_info = try M.find module_id module_map with Not_found ->
      failwith ("Module not found: " ^ module_id) in
    
    (* Generate module actor *)
    actors := gen_module_actor module_id :: !actors;
    
    (* Generate node actors only for compute nodes (mod_info.node), NOT for source nodes *)
    List.iter (fun (node_id, _, _, _) ->
      (* Check if this node has dependencies *)
      let has_deps = List.length mod_info.source > 0 in
      actors := gen_node_actor module_id node_id has_deps :: !actors
    ) mod_info.node
  ) unique_modules;
  
  String.concat "\n\n" (List.rev !actors)

(* Build NodeSpecs map - only for actual compute nodes *)
let gen_node_specs inst_prog module_map =
  let specs = ref [] in
  
  List.iter (fun party_block ->
    let party_id = party_block.Syntax.party_id in
    
    List.iter (fun (outputs, module_id, inputs) ->
      let inst_name = List.hd outputs in
      
      let mod_info = try M.find module_id module_map with Not_found ->
        failwith ("Module not found: " ^ module_id) in
      
      (* Generate specs only for compute nodes (mod_info.node) *)
      List.iter (fun (node_id, _, _, _) ->
        let qualified_name = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
        
        (* Build dependency list from inputs - get the actual output node names of upstream instances *)
        let input_names = List.map get_instance_name inputs in
        let deps = List.filter_map (fun input_name ->
          (* Find the upstream instance in party_block.instances *)
          try
            let (upstream_outputs, upstream_module_id, _) = 
              List.find (fun (outs, _, _) -> List.mem input_name outs) party_block.Syntax.instances in
            let upstream_inst_name = List.hd upstream_outputs in
            let upstream_mod_info = M.find upstream_module_id module_map in
            (* Get the first output node of the upstream module *)
            match upstream_mod_info.node with
            | (first_node_id, _, _, _) :: _ ->
                Some (party_id ^ "_" ^ upstream_inst_name ^ "_" ^ first_node_id)
            | [] -> None
          with Not_found -> None
        ) input_names in
        let deps_str = "[" ^ String.concat ", " deps ^ "]" in
        
        let spec = Printf.sprintf "        %s => #{party => %s, module => %s, dependencies => %s}"
          qualified_name party_id module_id deps_str in
        specs := spec :: !specs
      ) mod_info.node
    ) party_block.Syntax.instances
  ) inst_prog.Syntax.parties;
  
  "    NodeSpecs = #{\n" ^ String.concat ",\n" (List.rev !specs) ^ "\n    }"

(* Generate start/0 function with actor spawning for ALL parties *)
let gen_start inst_prog module_map =
  let spawn_code = ref [] in
  let party_configs = ref [] in
  
  (* Generate spawn code for each party block *)
  List.iter (fun party_block ->
    let party_id = party_block.Syntax.party_id in
    let leader = party_block.Syntax.leader in
    let interval = party_block.Syntax.periodic_ms in
    let all_instances = party_block.Syntax.instances in
    
    (* Generate spawn code for each instance in this party *)
    List.iter (fun (outputs, module_id, inputs) ->
      let inst_name = List.hd outputs in
      let mod_info = try M.find module_id module_map with Not_found ->
        failwith ("Module not found: " ^ module_id) in
      
      (* Calculate node names for this module (only compute nodes) *)
      let node_names = List.map (fun (node_id, _, _, _) ->
        party_id ^ "_" ^ inst_name ^ "_" ^ node_id
      ) mod_info.node in
      let nodes_str = "[" ^ String.concat ", " node_names ^ "]" in
      
      (* Calculate SyncDownstreams: upstream instances within same party *)
      let upstream_instances = List.filter_map (fun input_qid ->
        match input_qid with
        | Syntax.SimpleId id ->
            let upstream_exists = List.exists (fun (out, _, _) -> List.hd out = id) all_instances in
            if upstream_exists && id <> inst_name then
              Some (party_id ^ "_" ^ id)
            else
              None
        | Syntax.QualifiedId (_, _) -> None  (* Cross-party: don't add to sync downstream *)
      ) inputs in
      let sync_downstreams = "[" ^ String.concat ", " upstream_instances ^ "]" in
      
      (* Spawn module actor *)
      let module_spawn = Printf.sprintf
        "    PID_%s_%s = spawn(?MODULE, run_%s, [[], [], %s, 0, #{nodes => %s, request_targets => get_request_nodes(NodeSpecs, %s, %s)}, %s]),\n    register(%s_%s, PID_%s_%s)"
        party_id inst_name module_id party_id nodes_str nodes_str party_id sync_downstreams party_id inst_name party_id inst_name in
      spawn_code := module_spawn :: !spawn_code;
      
      (* Spawn node actors (only for compute nodes) *)
      List.iter (fun (node_id, _, _, expr) ->
        let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
        
        (* Generate compute function *)
        let compute_fn = "fun(Inputs) -> " ^ expr_to_erlang expr ^ " end" in
        
        (* Build dependency list - use port names from mod_info.source *)
        let deps_str = "[" ^ String.concat ", " mod_info.source ^ "]" in
        
        (* Build downstreams list: find all instances that use this node as input *)
        (* Check both same-party and cross-party consumers *)
        let downstream_list = ref [] in
        
        (* Check all party blocks for cross-party dependencies *)
        List.iter (fun target_party_block ->
          let target_party_id = target_party_block.Syntax.party_id in
          
          List.iter (fun (outputs_dst, module_id_dst, inputs_dst) ->
            let inst_name_dst = List.hd outputs_dst in
            List.iteri (fun i input_qid ->
              let should_add = match input_qid with
                | Syntax.SimpleId id -> 
                    target_party_id = party_id && id = inst_name
                | Syntax.QualifiedId (ref_party, id) ->
                    ref_party = party_id && id = inst_name
              in
              if should_add then begin
                let dst_mod = M.find module_id_dst module_map in
                let dst_port = List.nth dst_mod.source i in
                (* Get first compute node of destination module *)
                match dst_mod.node with
                | (first_node_id, _, _, _) :: _ ->
                    let dst_qualified = target_party_id ^ "_" ^ inst_name_dst ^ "_" ^ first_node_id in
                    downstream_list := (dst_qualified, dst_port) :: !downstream_list
                | [] -> ()
              end
            ) inputs_dst
          ) target_party_block.Syntax.instances
        ) inst_prog.Syntax.parties;
        
        let downstreams = if !downstream_list = [] then "[]" else
          "[" ^ String.concat ", " (List.map (fun (node_name, port) ->
            Printf.sprintf "{%s, %s}" node_name port
          ) !downstream_list) ^ "]"
        in
        
        (* Calculate initial next_ver: include all parties that this node depends on *)
        let dep_parties = ref [party_id] in
        List.iter (fun input_qid ->
          match input_qid with
          | Syntax.QualifiedId (ref_party, _) ->
              if not (List.mem ref_party !dep_parties) then
                dep_parties := ref_party :: !dep_parties
          | Syntax.SimpleId _ -> ()
        ) inputs;
        let next_ver_init = String.concat ", " (List.map (fun p -> p ^ " => 0") !dep_parties) in
        
        let node_spawn = Printf.sprintf
          "    PID_%s = spawn(?MODULE, run_%s_%s, [#{register_name => %s, dependencies => %s, compute => %s}, #{downstreams => %s}, #{buffer => #{}, next_ver => #{%s}, processed => #{}, req_buffer => [], deferred => []}]),\n    register(%s, PID_%s)"
          qualified module_id node_id qualified deps_str compute_fn downstreams next_ver_init qualified qualified in
        spawn_code := node_spawn :: !spawn_code
      ) mod_info.node
    ) all_instances;
    
    (* Add party actor config *)
    let party_config_code = String.concat "\n" [
      "    PartyConfig_" ^ party_id ^ " = #{";
      "        party => " ^ party_id ^ ",";
      "        leader => " ^ party_id ^ "_" ^ leader ^ ",";
      "        mode => periodic,";
      "        interval => " ^ string_of_int interval ^ ",";
      "        subscribers => []";
      "    },";
      "    PID_party_" ^ party_id ^ " = spawn(?MODULE, run_party, [PartyConfig_" ^ party_id ^ ", 0]),";
      "    register(party_" ^ party_id ^ ", PID_party_" ^ party_id ^ ")";
    ] in
    party_configs := party_config_code :: !party_configs
  ) inst_prog.Syntax.parties;
  
  let spawn_lines = List.rev !spawn_code in
  let spawn_with_commas = match spawn_lines with
    | [] -> []
    | _ -> 
        let rec add_commas = function
          | [] -> []
          | [last] -> [last]
          | x :: xs -> (x ^ ",") :: add_commas xs
        in add_commas spawn_lines
  in
  
  let party_lines = List.rev !party_configs in
  let party_with_commas = match party_lines with
    | [] -> []
    | _ ->
        let rec add_commas = function
          | [] -> []
          | [last] -> [last]
          | x :: xs -> (x ^ ",") :: add_commas xs
        in add_commas party_lines
  in
  
  String.concat "\n" ([
    "start() ->";
    gen_node_specs inst_prog module_map ^ ",";
    "";
    "    %% Spawn module actors and node actors for all parties";
  ] @ spawn_with_commas @ [
    ",";
    "";
    "    %% Spawn party actors";
  ] @ party_with_commas @ [
    ",";
    "    void.";
  ])

(* Generate trace helper function *)
let gen_trace_helper () =
  String.concat "\n" [
    "%% Trace helper: removes party prefix from node name for comparison";
    "trace_node_name(FullName) ->";
    "    NameStr = atom_to_list(FullName),";
    "    case string:split(NameStr, \"_\", leading) of";
    "        [_Party, Rest] -> list_to_atom(Rest);";
    "        _ -> FullName";
    "    end.";
    "";
  ]

(* Generate output handler *)
let gen_out inst_prog module_map =
  let entries = ref [] in
  
  List.iter (fun party_block ->
    let party_id = party_block.Syntax.party_id in
    
    List.iter (fun (outputs, module_id, _) ->
      let inst_name = List.hd outputs in
      
      let mod_info = try M.find module_id module_map with Not_found ->
        failwith ("Module not found: " ^ module_id) in
      
      (* Generate output handler for each compute node *)
      List.iter (fun (node_id, _, _, _) ->
        let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
        
        (* Check if this is a sink node *)
        if List.mem node_id mod_info.sink then
          entries := (Printf.sprintf "out(%s, Value) -> io:format(\"Output from %s: ~p~n\", [Value]), void;"
            qualified qualified) :: !entries
        else
          entries := (Printf.sprintf "out(%s, _) -> void;" qualified) :: !entries
      ) mod_info.node
    ) party_block.Syntax.instances
  ) inst_prog.Syntax.parties;
  
  String.concat "\n" (!entries @ ["out(_, _) -> erlang:error(badarg)."])

(* Generate export declarations *)
let gen_exports inst_prog module_map =
  let unique_modules = get_unique_modules inst_prog in
  let exports = ref ["-export([start/0, out/2, run_party/2, get_request_nodes/3])."] in
  
  (* Export module actors *)
  let module_exports = List.map (fun m -> "run_" ^ m ^ "/6") unique_modules in
  if List.length module_exports > 0 then
    exports := ("-export([" ^ String.concat ", " module_exports ^ "]).") :: !exports;
  
  (* Export node actors *)
  let node_exports = ref [] in
  List.iter (fun module_id ->
    let mod_info = try M.find module_id module_map with Not_found ->
      failwith ("Module not found: " ^ module_id) in
    List.iter (fun (node_id, _, _, _) ->
      node_exports := ("run_" ^ module_id ^ "_" ^ node_id ^ "/3") :: !node_exports
    ) mod_info.node
  ) unique_modules;
  if List.length !node_exports > 0 then
    exports := ("-export([" ^ String.concat ", " !node_exports ^ "]).") :: !exports;
  
  String.concat "\n" (List.rev !exports)

(* Main entry point *)
let gen_new_inst inst_prog module_map =
  let party_block = List.hd inst_prog.Syntax.parties in
  let module_name = party_block.Syntax.party_id in
  
  String.concat "\n\n" [
    "-module(" ^ module_name ^ ").";
    gen_exports inst_prog module_map;
    "";
    gen_helpers ();
    "";
    gen_trace_helper ();
    "";
    gen_get_request_nodes ();
    "";
    gen_run_party ();
    "";
    gen_all_actors inst_prog module_map;
    "";
    gen_start inst_prog module_map;
    "";
    gen_out inst_prog module_map;
  ]
