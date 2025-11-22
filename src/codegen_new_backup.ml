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

let indent n str = String.make (n * 4) ' ' ^ str

let concat_map sep f lst = String.concat sep (List.map f lst)

(* Extract instance name from qualified_id *)
let get_instance_name = function
  | SimpleId id -> id
  | QualifiedId (_, id) -> id

(* Extract unique module IDs from inst_prog *)
let get_unique_modules inst_prog module_map =
  let modules = ref StringSet.empty in
  List.iter (fun party_block ->
    List.iter (fun (_, module_id, _) ->
      modules := StringSet.add module_id !modules
    ) party_block.instances
  ) inst_prog.parties;
  StringSet.elements !modules

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

(* Extract variable references from expression *)
let rec extract_vars = function
  | EId v -> [v]
  | EBin (_, e1, e2) -> extract_vars e1 @ extract_vars e2
  | EUni (_, e) -> extract_vars e
  | EAnnot (v, _) -> [v]
  | EApp (_, es) -> List.concat (List.map extract_vars es)
  | EList es -> List.concat (List.map extract_vars es)
  | ETuple es -> List.concat (List.map extract_vars es)
  | EIf (e1, e2, e3) -> extract_vars e1 @ extract_vars e2 @ extract_vars e3
  | ELet (bindings, e) -> 
      List.concat (List.map (fun (_, expr, _) -> extract_vars expr) bindings) @ extract_vars e
  | _ -> []

(* Build NodeSpecs map as Erlang code *)
let gen_node_specs inst_prog module_map =
  let specs = ref [] in
  
  List.iter (fun party_block ->
    let party_id = party_block.party_id in
    
    List.iter (fun (outputs, module_id, inputs) ->
      let inst_name = List.hd outputs in
      
      (* Get module info *)
      let mod_info = try M.find module_id module_map with Not_found ->
        failwith ("Module not found: " ^ module_id) in
      
      (* Get input names *)
      let input_names = List.map get_instance_name inputs in
      
      (* Build input mapping: input_param -> qualified_upstream_node *)
      let input_map = List.mapi (fun i input_name ->
        let param = List.nth mod_info.source i in
        (param, party_id ^ "_" ^ input_name ^ "_data")
      ) input_names in
      
      (* Generate specs for source nodes (inputs) *)
      List.iter (fun source ->
        let qualified_name = party_id ^ "_" ^ inst_name ^ "_" ^ source in
        let deps = "[]" in  (* Source nodes have no dependencies *)
        let spec = Printf.sprintf "        %s => #{party => %s, module => %s, dependencies => %s}"
          qualified_name party_id module_id deps in
        specs := spec :: !specs
      ) mod_info.source;
      
      (* Generate specs for sink/compute nodes *)
      let compute_nodes = List.map (fun (id, _, _, _) -> id) mod_info.node in
      List.iter (fun node_id ->
        let qualified_name = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
        
        (* Extract dependencies from node expression *)
        let deps = try
          let (_, _, _, expr) = List.find (fun (id, _, _, _) -> id = node_id) mod_info.node in
          let vars = extract_vars expr in
          (* Map vars to qualified names using input_map *)
          let dep_list = List.filter_map (fun v ->
            try Some (List.assoc v input_map)
            with Not_found ->
              (* Local node reference *)
              Some (party_id ^ "_" ^ inst_name ^ "_" ^ v)
          ) vars in
          "[" ^ String.concat ", " dep_list ^ "]"
        with Not_found ->
          "[]" in
        
        let spec = Printf.sprintf "        %s => #{party => %s, module => %s, dependencies => %s}"
          qualified_name party_id module_id deps in
        specs := spec :: !specs
      ) compute_nodes;
      
      (* Generate specs for sinks *)
      List.iter (fun sink ->
        if not (List.mem sink compute_nodes) then
          let qualified_name = party_id ^ "_" ^ inst_name ^ "_" ^ sink in
          let deps = "[]" in
          let spec = Printf.sprintf "        %s => #{party => %s, module => %s, dependencies => %s}"
            qualified_name party_id module_id deps in
          specs := spec :: !specs
      ) mod_info.sink
    ) party_block.instances
  ) inst_prog.parties;
  
  "    NodeSpecs = #{\n" ^ String.concat ",\n" (List.rev !specs) ^ "\n    }"

(* Generate module actor function *)
let gen_module_actor module_id party_id inst_name module_info =
  let func_name = "run_" ^ inst_name in
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

(* Generate node actor function - compute node *)
let gen_node_actor_compute party_id inst_name node_id deps expr =
  let func_name = "run_" ^ inst_name ^ "_" ^ node_id in
  
  (* Convert expression to Erlang *)
  let rec expr_to_erlang = function
    | EConst (CInt n) -> string_of_int n
    | EId v -> "maps:get(" ^ v ^ ", InputMap, 0)"
    | EBin (BAdd, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " + " ^ expr_to_erlang e2 ^ ")"
    | EBin (BSub, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " - " ^ expr_to_erlang e2 ^ ")"
    | EBin (BMul, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " * " ^ expr_to_erlang e2 ^ ")"
    | EBin (BDiv, e1, e2) -> "(" ^ expr_to_erlang e1 ^ " / " ^ expr_to_erlang e2 ^ ")"
    | EAnnot (v, _) -> "maps:get(" ^ v ^ ", InputMap, 0)"
    | _ -> "0" in
  
  let compute_expr = expr_to_erlang expr in
  
  String.concat "\n" [
    func_name ^ "(Config, Connections, NodeState) ->";
    "    #{dependencies := Deps, node_name := NodeName, register_name := RegName} = Config,";
    "    #{downstreams := Downstreams} = Connections,";
    "    #{buffer := Buffer0, next_ver := NextVer0, processed := Processed0, req_buffer := ReqBuffer0, deferred := Deferred0} = NodeState,";
    "    ";
    "    %% Process Buffer: Check if any complete sets of inputs are ready";
    "    HL = lists:sort(?SORTBuffer, maps:to_list(Buffer0)),";
    "    {NBuffer, NextVerT, ProcessedT, DeferredT} = lists:foldl(fun(E, {Buffer, NextVer, Processed, Deferred}) ->";
    "        case E of";
    "            {{Party, Ver} = Version, InputMap} ->";
    "                %% Check if all dependencies are present";
    "                AllPresent = lists:all(fun(DepName) ->";
    "                    maps:is_key(DepName, InputMap)";
    "                end, Deps),";
    "                case AllPresent of";
    "                    true ->";
    "                        %% Check sequential constraint";
    "                        CurrentNextVer = maps:get(Party, NextVer, 0),";
    "                        case CurrentNextVer =:= Ver of";
    "                            true ->";
    "                                %% Compute result";
    "                                Result = " ^ compute_expr ^ ",";
    "                                out(RegName, Result),";
    "                                ";
    "                                %% Send to downstreams";
    "                                lists:foreach(fun(DownstreamName) ->";
    "                                    DownstreamName ! {{Party, Ver}, NodeName, Result}";
    "                                end, Downstreams),";
    "                                ";
    "                                {maps:remove(Version, Buffer), maps:update(Party, Ver + 1, NextVer), Processed, []};";
    "                            false ->";
    "                                {Buffer, NextVer, Processed, Deferred}";
    "                        end;";
    "                    false ->";
    "                        {Buffer, NextVer, Processed, Deferred}";
    "                end;";
    "            _ ->";
    "                {Buffer, NextVer, Processed, Deferred}";
    "        end";
    "    end, {Buffer0, NextVer0, Processed0, Deferred0}, HL),";
    "    ";
    "    %% Process Request Buffer";
    "    Sorted_req_buf = lists:sort(?SORTVerBuffer, ReqBuffer0),";
    "    {NNextVer, NProcessed, NReqBuffer, NDeferred} = lists:foldl(fun(E, {NextVer, Processed, ReqBuffer, Deferred}) ->";
    "        case E of";
    "            {Party, Ver} = Version ->";
    "                CurrentNextVer = maps:get(Party, NextVer, 0),";
    "                case CurrentNextVer =:= Ver of";
    "                    true -> {NextVer, Processed, ReqBuffer, [Version | Deferred]};";
    "                    false -> {NextVer, Processed, [Version | ReqBuffer], Deferred}";
    "                end;";
    "            _ -> {NextVer, Processed, ReqBuffer, Deferred}";
    "        end";
    "    end, {NextVerT, ProcessedT, [], DeferredT}, Sorted_req_buf),";
    "    ";
    "    receive";
    "        {request, {Party, Ver}} ->";
    "            " ^ func_name ^ "(Config, Connections,";
    "                NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed,";
    "                          req_buffer := lists:reverse([{Party, Ver} | NReqBuffer]), deferred := NDeferred});";
    "        {{Party, Ver}, InputName, Value} ->";
    "            UpdatedBuffer = buffer_update(Deps, [], {{Party, Ver}, InputName, Value}, NBuffer),";
    "            " ^ func_name ^ "(Config, Connections,";
    "                NodeState#{buffer := UpdatedBuffer, next_ver := NNextVer, processed := NProcessed,";
    "                          req_buffer := NReqBuffer, deferred := NDeferred})";
    "    end.";
  ]

(* Generate node actor function - source node *)
let gen_node_actor_source party_id inst_name node_id =
  let qualified_name = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
  let func_name = "run_" ^ inst_name ^ "_" ^ node_id in
  
  String.concat "\n" [
    func_name ^ "(Config, Connections, NodeState) ->";
    "    #{register_name := RegName} = Config,";
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
    "                        %% Source node: generate value";
    "                        Result = rand:uniform(100),";
    "                        out(RegName, Result),";
    "                        ";
    "                        %% Send to downstreams";
    "                        lists:foreach(fun({DownstreamPid, TargetPortTag}) ->";
    "                            DownstreamPid ! {{Party, Ver}, TargetPortTag, Result}";
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
    "            " ^ func_name ^ "(Config, Connections,";
    "                NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed,";
    "                          req_buffer := lists:reverse([{Party, Ver} | NReqBuffer]), deferred := NDeferred})";
    "    end.";
  ]

(* Generate all actors for all instances *)
let gen_all_actors inst_prog module_map =
  let actors = ref [] in
  
  List.iter (fun party_block ->
    let party_id = party_block.party_id in
    
    List.iter (fun (outputs, module_id, inputs) ->
      let inst_name = List.hd outputs in
      
      let mod_info = try M.find module_id module_map with Not_found ->
        failwith ("Module not found: " ^ module_id) in
      
      (* Generate module actor *)
      actors := gen_module_actor module_id party_id inst_name mod_info :: !actors;
      
      (* Generate node actors for source nodes *)
      List.iter (fun source ->
        actors := gen_node_actor_source party_id inst_name source :: !actors
      ) mod_info.source;
      
      (* Generate node actors for compute/sink nodes *)
      List.iter (fun (node_id, _, _, expr) ->
        let vars = extract_vars expr in
        (* Get qualified dependency names *)
        let input_names = List.map get_instance_name inputs in
        let input_map = List.mapi (fun i input_name ->
          let param = List.nth mod_info.source i in
          (param, party_id ^ "_" ^ input_name ^ "_data")
        ) input_names in
        
        let deps = List.filter_map (fun v ->
          try Some (List.assoc v input_map)
          with Not_found ->
            Some (party_id ^ "_" ^ inst_name ^ "_" ^ v)
        ) vars in
        
        actors := gen_node_actor_compute party_id inst_name node_id deps expr :: !actors
      ) mod_info.node
    ) party_block.instances
  ) inst_prog.parties;
  
  String.concat "\n\n" (List.rev !actors)

(* Generate start/0 function with actor spawning *)
let gen_start inst_prog module_map =
  let party_block = List.hd inst_prog.parties in
  let party_id = party_block.party_id in
  let leader = party_block.leader in
  let interval = party_block.periodic_ms in
  
  let spawn_code = ref [] in
  
  (* Generate spawn code for each instance *)
  List.iter (fun (outputs, module_id, inputs) ->
    let inst_name = List.hd outputs in
    let mod_info = try M.find module_id module_map with Not_found ->
      failwith ("Module not found: " ^ module_id) in
    
    (* Calculate all node names for this module *)
    let all_nodes = (List.map (fun s -> party_id ^ "_" ^ inst_name ^ "_" ^ s) mod_info.source) @
                    (List.map (fun (id, _, _, _) -> party_id ^ "_" ^ inst_name ^ "_" ^ id) mod_info.node) in
    
    (* Calculate request targets using get_request_nodes *)
    let nodes_str = "[" ^ String.concat ", " all_nodes ^ "]" in
    let request_targets_call = Printf.sprintf "get_request_nodes(NodeSpecs, %s, %s)" nodes_str party_id in
    
    (* Spawn module actor *)
    let module_spawn = Printf.sprintf
      "    %sConfig = #{nodes => %s, request_targets => %s},\n    %sPid = spawn(?MODULE, run_%s, [[], [], %s, 0, %sConfig, []]),\n    register(%s_%s, %sPid),"
      inst_name nodes_str request_targets_call inst_name inst_name party_id inst_name party_id inst_name inst_name in
    spawn_code := module_spawn :: !spawn_code;
    
    (* Spawn source node actors *)
    List.iter (fun source ->
      let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ source in
      let node_spawn = Printf.sprintf
        "    %sConfig = #{register_name => %s},\n    %sConnections = #{downstreams => []},\n    %sState = #{buffer => #{}, next_ver => #{}, processed => #{}, req_buffer => [], deferred => []},\n    %sPid = spawn(?MODULE, run_%s_%s, [%sConfig, %sConnections, %sState]),\n    register(%s, %sPid),"
        qualified qualified qualified qualified qualified inst_name source qualified qualified qualified qualified qualified in
      spawn_code := node_spawn :: !spawn_code
    ) mod_info.source;
    
    (* Spawn compute/sink node actors *)
    List.iter (fun (node_id, _, _, expr) ->
      let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
      let vars = extract_vars expr in
      let input_names = List.map get_instance_name inputs in
      let input_map = List.mapi (fun i input_name ->
        let param = List.nth mod_info.source i in
        (param, party_id ^ "_" ^ input_name ^ "_data")
      ) input_names in
      let deps = List.filter_map (fun v ->
        try Some (List.assoc v input_map)
        with Not_found -> Some (party_id ^ "_" ^ inst_name ^ "_" ^ v)
      ) vars in
      let deps_str = "[" ^ String.concat ", " deps ^ "]" in
      
      let node_spawn = Printf.sprintf
        "    %sConfig = #{dependencies => %s, node_name => %s, register_name => %s},\n    %sConnections = #{downstreams => []},\n    %sState = #{buffer => #{}, next_ver => #{}, processed => #{}, req_buffer => [], deferred => []},\n    %sPid = spawn(?MODULE, run_%s_%s, [%sConfig, %sConnections, %sState]),\n    register(%s, %sPid),"
        qualified deps_str node_id qualified qualified qualified qualified inst_name node_id qualified qualified qualified qualified qualified in
      spawn_code := node_spawn :: !spawn_code
    ) mod_info.node
  ) party_block.instances;
  
  String.concat "\n" ([
    "start() ->";
    gen_node_specs inst_prog module_map ^ ",";
    "";
    "    %% Spawn module actors and node actors";
  ] @ List.rev !spawn_code @ [
    "";
    "    %% Spawn party actor";
    "    PartyConfig = #{";
    "        party => " ^ party_id ^ ",";
    "        leader => " ^ party_id ^ "_" ^ leader ^ ",";
    "        mode => periodic,";
    "        interval => " ^ string_of_int interval ^ ",";
    "        subscribers => []";
    "    },";
    "    PartyPid = spawn(?MODULE, run_party, [PartyConfig, 0]),";
    "    register(" ^ party_id ^ ", PartyPid),";
    "    void.";
  ])

(* Generate output handler *)
let gen_out inst_prog module_map =
  let entries = ref [] in
  
  List.iter (fun party_block ->
    let party_id = party_block.party_id in
    
    List.iter (fun (outputs, module_id, _) ->
      let inst_name = List.hd outputs in
      
      let mod_info = try M.find module_id module_map with Not_found ->
        failwith ("Module not found: " ^ module_id) in
      
      (* Generate output handler for each sink *)
      List.iter (fun sink ->
        let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ sink in
        entries := (Printf.sprintf "out(%s, Value) -> io:format(\"Output from %s: ~~p~~n\", [Value]), void;"
          qualified qualified) :: !entries
      ) mod_info.sink;
      
      (* Also handle non-sink nodes (just void) *)
      List.iter (fun source ->
        let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ source in
        entries := (Printf.sprintf "out(%s, _) -> void;" qualified) :: !entries
      ) mod_info.source
    ) party_block.instances
  ) inst_prog.parties;
  
  String.concat "\n" (!entries @ ["out(_, _) -> erlang:error(badarg)."])

(* Main entry point *)
let gen_new_inst inst_prog module_map =
  (* Get first party for module name *)
  let party_block = List.hd inst_prog.parties in
  let module_name = party_block.party_id in
  
  String.concat "\n\n" [
    "-module(" ^ module_name ^ ").";
    "-export([start/0, out/2, run_party/2, get_request_nodes/3]).";
    "";
    gen_helpers ();
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
