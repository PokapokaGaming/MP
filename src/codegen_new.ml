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
  (* Collect from static parties *)
  List.iter (fun party_block ->
    List.iter (fun (_, module_id, _) ->
      modules := StringSet.add module_id !modules
    ) party_block.Syntax.instances
  ) inst_prog.Syntax.parties;
  (* Collect from templates *)
  List.iter (fun template ->
    List.iter (fun (_, module_id, _) ->
      modules := StringSet.add module_id !modules
    ) template.Syntax.template_instances
  ) inst_prog.Syntax.templates;
  StringSet.elements !modules

(* Convert expression to Erlang code string *)
let rec expr_to_erlang = function
  | Syntax.EConst Syntax.CUnit -> "void"
  | Syntax.EConst (Syntax.CBool b) -> string_of_bool b
  | Syntax.EConst (Syntax.CInt i) -> string_of_int i
  | Syntax.EConst (Syntax.CFloat f) -> Printf.sprintf "%f" f
  | Syntax.EConst (Syntax.CChar c) -> string_of_int (int_of_char c)
  | Syntax.EId v -> "maps:get(" ^ v ^ ", Inputs, 0)"
  | Syntax.EAnnot (v, Syntax.ALast) -> 
      (* @last reference: get from Processed map with {last, Id} key, following Original *)
      (* Key format: {last, atom} to match Original's buffer_update behavior *)
      "maps:get({last, " ^ v ^ "}, Processed, 0)"
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
  | Syntax.EFold (op, init, id) ->
      (* fold(op, init, variadic_input) -> lists:foldl(fun(X, Acc) -> X op Acc end, Init, InputList) *)
      (* Ensure input is a list - wrap single values in a list if needed *)
      let op_str = match op with
        | Syntax.BAdd -> "+"
        | Syntax.BSub -> "-"
        | Syntax.BMul -> "*"
        | Syntax.BLAnd -> "andalso"
        | Syntax.BLOr -> "orelse"
        | _ -> "+" (* default to add *)
      in
      Printf.sprintf "(fun() -> V = maps:get(%s, Inputs, []), L = if is_list(V) -> V; true -> [V] end, lists:foldl(fun(X, Acc) -> X %s Acc end, %s, L) end)()" 
        id op_str (expr_to_erlang init)
  | Syntax.ECount id ->
      (* count(variadic_input) -> length(InputList) *)
      (* Ensure input is a list - wrap single values in a list if needed *)
      Printf.sprintf "(fun() -> V = maps:get(%s, Inputs, []), L = if is_list(V) -> V; true -> [V] end, length(L) end)()" id
  | _ -> "0"

(* Extract current dependencies from an expression (return list of referenced node IDs) *)
(* NOTE: @last references are extracted separately by extract_last_deps *)
let rec extract_node_deps expr =
  match expr with
  | Syntax.EConst _ -> []
  | Syntax.EId v -> [v]
  | Syntax.EAnnot (_, Syntax.ALast) -> []  (* @last is handled separately *)
  | Syntax.EApp (_, args) -> List.concat_map extract_node_deps args
  | Syntax.EBin (_, e1, e2) -> extract_node_deps e1 @ extract_node_deps e2
  | Syntax.EUni (_, e) -> extract_node_deps e
  | Syntax.ETuple es -> List.concat_map extract_node_deps es
  | Syntax.EList es -> List.concat_map extract_node_deps es
  | Syntax.EIf (e1, e2, e3) -> extract_node_deps e1 @ extract_node_deps e2 @ extract_node_deps e3
  | Syntax.ELet (bindings, body) ->
      let bound_deps = List.concat_map (fun (_, e, _) -> extract_node_deps e) bindings in
      bound_deps @ extract_node_deps body
  | Syntax.ECase (scrut, branches) ->
      let scrut_deps = extract_node_deps scrut in
      let branch_deps = List.concat_map (fun (_, e) -> extract_node_deps e) branches in
      scrut_deps @ branch_deps
  | Syntax.EFun (_, body) -> extract_node_deps body
  | Syntax.EFold (_, init, id) -> extract_node_deps init @ [id]
  | Syntax.ECount id -> [id]

(* Extract @last dependencies from an expression (return list of referenced node IDs with @last) *)
(* Following Original: @last references are dependencies stored with {last, Id} key *)
let rec extract_last_deps expr =
  match expr with
  | Syntax.EConst _ -> []
  | Syntax.EId _ -> []
  | Syntax.EAnnot (v, Syntax.ALast) -> [v]  (* @last reference *)
  | Syntax.EApp (_, args) -> List.concat_map extract_last_deps args
  | Syntax.EBin (_, e1, e2) -> extract_last_deps e1 @ extract_last_deps e2
  | Syntax.EUni (_, e) -> extract_last_deps e
  | Syntax.ETuple es -> List.concat_map extract_last_deps es
  | Syntax.EList es -> List.concat_map extract_last_deps es
  | Syntax.EIf (e1, e2, e3) -> extract_last_deps e1 @ extract_last_deps e2 @ extract_last_deps e3
  | Syntax.ELet (bindings, body) ->
      let bound_deps = List.concat_map (fun (_, e, _) -> extract_last_deps e) bindings in
      bound_deps @ extract_last_deps body
  | Syntax.ECase (scrut, branches) ->
      let scrut_deps = extract_last_deps scrut in
      let branch_deps = List.concat_map (fun (_, e) -> extract_last_deps e) branches in
      scrut_deps @ branch_deps
  | Syntax.EFun (_, body) -> extract_last_deps body
  | Syntax.EFold (_, init, _) -> extract_last_deps init
  | Syntax.ECount _ -> []

(* Remove duplicates from list *)
let unique_list lst =
  let rec aux acc = function
    | [] -> List.rev acc
    | x :: xs -> if List.mem x acc then aux acc xs else aux (x :: acc) xs
  in aux [] lst

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

(* Generate compute_request_targets/4 function - dynamic request target computation *)
(* Follows Original's gen_request_node_fun logic:
   - Request targets are: extern_input (source nodes) + sinks without same-party upstream
   - Regular nodes with only other-party deps are NOT request targets (they are data-driven)
   
   Parameters:
   - NodeDependencies: #{NodePid => [dep_name, ...]}
   - UpstreamParties: #{dep_name => PartyId}
   - MyParty: this instance's party
   - SinkNodes: [NodePid] - list of sink (output) nodes *)
let gen_compute_request_targets () =
  String.concat "\n" [
    "%% Compute request targets following Original's gen_request_node_fun logic";
    "%% Only Source nodes (no deps) and Sink nodes without same-party deps are request targets";
    "%% Regular relay nodes are data-driven (NOT request targets even if no same-party deps)";
    "compute_request_targets(NodeDependencies, UpstreamParties, MyParty, SinkNodes) ->";
    "    maps:fold(fun(NodePid, Deps, Acc) ->";
    "        case Deps of";
    "            [] ->";
    "                %% Source node (no dependencies) - always a request target";
    "                [NodePid | Acc];";
    "            _ ->";
    "                %% Check if this is a Sink node";
    "                IsSink = lists:member(NodePid, SinkNodes),";
    "                case IsSink of";
    "                    false ->";
    "                        %% Not a sink - NOT a request target (data-driven)";
    "                        Acc;";
    "                    true ->";
    "                        %% Sink node: check if any dependency comes from same party";
    "                        HasSamePartyDep = lists:any(fun(DepName) ->";
    "                            case maps:get(DepName, UpstreamParties, external) of";
    "                                MyParty -> true;";
    "                                _ -> false";
    "                            end";
    "                        end, Deps),";
    "                        case HasSamePartyDep of";
    "                            true -> Acc;           %% Has same-party dep - data-driven";
    "                            false -> [NodePid | Acc]  %% No same-party dep - request target";
    "                        end";
    "                end";
    "        end";
    "    end, [], NodeDependencies).";
  ]

(* Generate run_party/2 function - sends sync pulse to leader only *)
(* Extended with suspend/resume support for atomic dynamic updates *)
let gen_run_party () =
  String.concat "\n" [
    "run_party(Config, Ver) ->";
    "    #{party := Party, leader := Leader, mode := Mode} = Config,";
    "    case Mode of";
    "        periodic ->";
    "            #{interval := Interval} = Config,";
    "            Leader ! {Party, Ver},";
    "            %% Use receive...after for suspend/resume support";
    "            receive";
    "                {suspend, Caller} ->";
    "                    io:format(\"[PARTY] ~p suspended at Ver:~p~n\", [Party, Ver + 1]),";
    "                    Caller ! {ok, Ver + 1},";
    "                    run_party_suspended(Config, Ver + 1);";
    "                {get_version, Caller} ->";
    "                    Caller ! {version, Ver + 1},";
    "                    run_party(Config, Ver)";
    "            after Interval ->";
    "                run_party(Config, Ver + 1)";
    "            end;";
    "        any_party ->";
    "            #{dependencies := Dependencies} = Config,";
    "            receive";
    "                {suspend, Caller} ->";
    "                    io:format(\"[PARTY] ~p suspended at Ver:~p~n\", [Party, Ver + 1]),";
    "                    Caller ! {ok, Ver + 1},";
    "                    run_party_suspended(Config, Ver + 1);";
    "                {get_version, Caller} ->";
    "                    Caller ! {version, Ver + 1},";
    "                    run_party(Config, Ver);";
    "                {DepParty, _DepVer} ->";
    "                    case lists:member(DepParty, Dependencies) of";
    "                        true ->";
    "                            Leader ! {Party, Ver},";
    "                            run_party(Config, Ver + 1);";
    "                        false ->";
    "                            run_party(Config, Ver)";
    "                    end";
    "            end";
    "    end.";
    "";
    "%% Suspended state - waiting for resume";
    "run_party_suspended(Config, Ver) ->";
    "    #{party := Party, leader := Leader} = Config,";
    "    receive";
    "        {resume} ->";
    "            io:format(\"[PARTY] ~p resumed at Ver:~p~n\", [Party, Ver]),";
    "            Leader ! {Party, Ver},";
    "            run_party(Config, Ver);";
    "        {get_version, Caller} ->";
    "            Caller ! {version, Ver},";
    "            run_party_suspended(Config, Ver);";
    "        {suspend, Caller} ->";
    "            %% Already suspended, just reply";
    "            Caller ! {ok, Ver},";
    "            run_party_suspended(Config, Ver)";
    "    end.";
  ]

(* Generate generic module actor function for a module *)
(* New design: request_targets computed dynamically from node_dependencies and upstream_parties *)
(* Following Original's gen_request_node_fun: only Source nodes and Sinks without same-party deps get requests *)
let gen_module_actor module_id =
  let func_name = "run_" ^ module_id in
  String.concat "\n" [
    func_name ^ "(Ver_buffer, In_buffer, Party, Party_ver, Config, SyncDownstreams) ->";
    "    #{nodes := Nodes, node_dependencies := NodeDeps, upstream_parties := UpstreamParties, sink_nodes := SinkNodes} = Config,";
    "    ";
    "    %% Dynamically compute request targets following Original's logic:";
    "    %% Only Source nodes (no deps) and Sink nodes without same-party deps";
    "    RequestTargets = compute_request_targets(NodeDeps, UpstreamParties, Party, SinkNodes),";
    "    io:format(\"[DEBUG] Module:~p === Sync Pulse Processing === Party=~p, Ver=~p~n\", [self(), Party, Party_ver]),";
    "    io:format(\"[DEBUG] Module:~p Config: Nodes=~p, NodeDeps=~p, UpstreamParties=~p, SinkNodes=~p~n\", [self(), Nodes, NodeDeps, UpstreamParties, SinkNodes]),";
    "    io:format(\"[DEBUG] Module:~p Computed RequestTargets=~p~n\", [self(), RequestTargets]),";
    "    ";
    "    %% Process version buffer (sync pulses)";
    "    Sorted_ver_buf = lists:sort(?SORTVerBuffer, Ver_buffer),";
    "    {NBuffer, Party_ver1} = lists:foldl(fun(Version, {Buf, Party_verT}) ->";
    "        case Version of";
    "            {P, Ver} when P =:= Party andalso Ver > Party_verT ->";
    "                io:format(\"[DEBUG] Module:~p Sync pulse FUTURE: Party=~p, Ver=~p > Expected=~p, buffering~n\", [self(), P, Ver, Party_verT]),";
    "                {[{P, Ver} | Buf], Party_verT};";
    "            {P, Ver} when P =:= Party andalso Ver =:= Party_verT ->";
    "                io:format(\"[DEBUG] Module:~p Sync pulse MATCH: Party=~p, Ver=~p~n\", [self(), P, Ver]),";
    "                %% Forward sync pulse to downstream modules";
    "                io:format(\"[DEBUG] Module:~p Forwarding sync pulse to SyncDownstreams=~p~n\", [self(), SyncDownstreams]),";
    "                lists:foreach(fun(ModulePid) ->";
    "                    io:format(\"[DEBUG] Module:~p SEND sync_pulse -> ~p : {~p, ~p}~n\", [self(), ModulePid, Party, Ver]),";
    "                    ModulePid ! {Party, Ver}";
    "                end, SyncDownstreams),";
    "                ";
    "                %% Forward request only to dynamically computed target node actors";
    "                io:format(\"[DEBUG] Module:~p Sending requests to RequestTargets=~p~n\", [self(), RequestTargets]),";
    "                lists:foreach(fun(NodePid) ->";
    "                    io:format(\"[DEBUG] Module:~p SEND request -> ~p : {request, {~p, ~p}}~n\", [self(), NodePid, Party, Ver]),";
    "                    NodePid ! {request, {Party, Ver}}";
    "                end, RequestTargets),";
    "                ";
    "                {Buf, Party_verT + 1};";
    "            {P, Ver} when P =:= Party andalso Ver < Party_verT ->";
    "                io:format(\"[DEBUG] Module:~p Sync pulse OLD: Party=~p, Ver=~p < Expected=~p, dropping~n\", [self(), P, Ver, Party_verT]),";
    "                {Buf, Party_verT};";
    "            _ ->";
    "                {Buf, Party_verT}";
    "        end";
    "    end, {[], Party_ver}, Sorted_ver_buf),";
    "    ";
    "    %% Process input buffer (data messages)";
    "    Sorted_in_buf = lists:sort(?SORTInBuffer, In_buffer),";
    "    io:format(\"[DEBUG] Module:~p === Data Message Processing === entries=~p~n\", [self(), length(Sorted_in_buf)]),";
    "    {NInBuffer, Party_verN} = lists:foldl(fun(Msg, {Buf, Party_verT}) ->";
    "        case Msg of";
    "            {{P, Ver}, InputName, Value} when P =:= Party ->";
    "                io:format(\"[DEBUG] Module:~p Forwarding data to Nodes=~p: {{~p,~p}, ~p, ~p}~n\", [self(), Nodes, P, Ver, InputName, Value]),";
    "                %% Forward data to all managed node actors";
    "                lists:foreach(fun(NodeName) ->";
    "                    io:format(\"[DEBUG] Module:~p SEND data -> ~p~n\", [self(), NodeName]),";
    "                    NodeName ! Msg";
    "                end, Nodes),";
    "                {Buf, Party_verT};";
    "            {{P, Ver}, InputName, Value} ->";
    "                io:format(\"[DEBUG] Module:~p Data from OTHER party ~p (my party=~p), dropping: {{~p,~p}, ~p, ~p}~n\", [self(), P, Party, P, Ver, InputName, Value]),";
    "                {Buf, Party_verT};";
    "            _ ->";
    "                {Buf, Party_verT}";
    "        end";
    "    end, {[], Party_ver1}, Sorted_in_buf),";
    "    ";
    "    receive";
    "        %% Dynamic connection: add upstream with its party (sync call with Ack)";
    "        {add_upstream, DepName, DepParty, Caller} ->";
    "            NewUpstreamParties = maps:put(DepName, DepParty, UpstreamParties),";
    "            NewConfig = Config#{upstream_parties := NewUpstreamParties},";
    "            Caller ! {ok, connected},";
    "            " ^ func_name ^ "(NBuffer, NInBuffer, Party, Party_verN, NewConfig, SyncDownstreams);";
    "        ";
    "        %% Dynamic connection: add sync downstream (for sync pulse forwarding)";
    "        {add_sync_downstream, NewModulePid, Caller} ->";
    "            NewSyncDownstreams = [NewModulePid | SyncDownstreams],";
    "            io:format(\"[DYN] Module adding sync downstream: ~p~n\", [NewModulePid]),";
    "            Caller ! {ok, connected},";
    "            " ^ func_name ^ "(NBuffer, NInBuffer, Party, Party_verN, Config, NewSyncDownstreams);";
    "        ";
    "        %% Dynamic disconnection: remove upstream";
    "        {remove_upstream, DepName} ->";
    "            NewUpstreamParties = maps:remove(DepName, UpstreamParties),";
    "            NewConfig = Config#{upstream_parties := NewUpstreamParties},";
    "            " ^ func_name ^ "(NBuffer, NInBuffer, Party, Party_verN, NewConfig, SyncDownstreams);";
    "        ";
    "        {update_sync_downstreams, NewSyncDownstreams} ->";
    "            " ^ func_name ^ "(NBuffer, NInBuffer, Party, Party_verN, Config, NewSyncDownstreams);";
    "        {_, _} = Ver_msg ->";
    "            " ^ func_name ^ "(lists:reverse([Ver_msg | NBuffer]), NInBuffer, Party, Party_verN, Config, SyncDownstreams);";
    "        {_, _, _} = In_msg ->";
    "            " ^ func_name ^ "(NBuffer, lists:reverse([In_msg | NInBuffer]), Party, Party_verN, Config, SyncDownstreams)";
    "    end.";
  ]

(* Generate generic node actor function for a module's node *)
(* trigger_parties is passed via Config - computation triggers when data from any of these parties arrives *)
(* dep_to_port: #{DepName => PortName} - maps upstream nodes to their target ports *)
(* variadic_ports: [PortName] - list of ports that accept multiple inputs (aggregated as lists) *)
let gen_node_actor module_id node_id has_deps =
  let func_name = "run_" ^ module_id ^ "_" ^ node_id in
  
  if has_deps then
    (* Compute node with dependencies - supports cross-party synchronization *)
    (* Key insight from Original: 
       - When data from a trigger party arrives AND all deps ready -> compute immediately
       - When data from non-trigger party arrives AND all deps ready -> compute only if Deferred non-empty
       This ensures computation is triggered by the correct party's sync pulse
       For periodic nodes: trigger_parties = [OwnerParty]
       For any_party(p,q): trigger_parties = [p, q] *)
    String.concat "\n" [
      func_name ^ "(Config, Connections, NodeState) ->";
      "    #{dependencies := Deps, last_deps := LastDeps, register_name := RegName, compute := ComputeFn, trigger_parties := TriggerParties,";
      "      dep_to_port := DepToPort, variadic_ports := VariadicPorts} = Config,";
      "    #{downstreams := Downstreams} = Connections,";
      "    #{buffer := Buffer0, next_ver := NextVer0, processed := Processed0, req_buffer := ReqBuffer0, deferred := Deferred0, last := Last0} = NodeState,";
      "    ";
      "    %% Aggregate function: convert Processed (#{DepName => Value}) to #{PortName => ValueOrList}";
      "    %% Generic: works for all nodes based on dep_to_port and variadic_ports config";
      "    Aggregate = fun(Proc) ->";
      "        %% Group values by port name";
      "        Grouped = maps:fold(fun(DepName, Value, Acc) ->";
      "            PortName = maps:get(DepName, DepToPort, DepName),  %% Default to DepName if not mapped";
      "            case maps:find(PortName, Acc) of";
      "                {ok, Existing} -> maps:put(PortName, [Value | Existing], Acc);";
      "                error -> maps:put(PortName, [Value], Acc)";
      "            end";
      "        end, #{}, Proc),";
      "        %% For non-variadic ports, unwrap single-element lists; for variadic, keep as list";
      "        maps:map(fun(PortName, Values) ->";
      "            case lists:member(PortName, VariadicPorts) of";
      "                true -> Values;  %% Variadic: keep as list";
      "                false ->";
      "                    case Values of";
      "                        [Single] -> Single;  %% Single input: unwrap";
      "                        _ -> Values  %% Multiple values to non-variadic port (shouldn't happen, but safe)";
      "                    end";
      "            end";
      "        end, Grouped)";
      "    end,";
      "    ";
      "    %% Process Buffer: Handle cross-party synchronization using Processed map";
      "    %% Key: computation triggers only when data from TriggerParties arrives (or clears Deferred)";
      "    HL = lists:sort(?SORTBuffer, maps:to_list(Buffer0)),";
      "    io:format(\"[DEBUG] Node:~p === Buffer Processing Start ===~n\", [trace_node_name(RegName)]),";
      "    io:format(\"[DEBUG] Node:~p State: NextVer=~p, Processed=~p, Deferred=~p~n\", [trace_node_name(RegName), NextVer0, Processed0, Deferred0]),";
      "    io:format(\"[DEBUG] Node:~p Buffer entries to process: ~p~n\", [trace_node_name(RegName), length(HL)]),";
      "    {NBuffer, NextVerT, ProcessedT, DeferredT, LastT} = lists:foldl(fun(E, {Buffer, NextVer, Processed, Deferred, Last}) ->";
      "        case E of";
      "            {{Party, Ver} = Version, InputMap} ->";
      "                CurrentNextVer = maps:get(Party, NextVer, 0),";
      "                io:format(\"[DEBUG] Node:~p Checking buffer entry: Party=~p, Ver=~p, ExpectedVer=~p, InputMap=~p~n\", [trace_node_name(RegName), Party, Ver, CurrentNextVer, InputMap]),";
      "                case CurrentNextVer =:= Ver of";
      "                    true ->";
      "                        io:format(\"[DEBUG] Node:~p Version MATCH (Party=~p, Ver=~p)~n\", [trace_node_name(RegName), Party, Ver]),";
      "                        MergedProcessed = maps:merge(Processed, InputMap),";
      "                        io:format(\"[DEBUG] Node:~p MergedProcessed=~p, RequiredDeps=~p~n\", [trace_node_name(RegName), MergedProcessed, Deps]),";
      "                        AllPresent = lists:all(fun(DepName) ->";
      "                            maps:is_key(DepName, MergedProcessed)";
      "                        end, Deps),";
      "                        io:format(\"[DEBUG] Node:~p AllDepsPresent=~p~n\", [trace_node_name(RegName), AllPresent]),";
      "                        case AllPresent of";
      "                            true ->";
      "                                %% All deps ready - check if this party is in trigger list";
      "                                IsTrigger = lists:member(Party, TriggerParties),";
      "                                io:format(\"[DEBUG] Node:~p Party=~p in TriggerParties=~p? ~p~n\", [trace_node_name(RegName), Party, TriggerParties, IsTrigger]),";
      "                                case IsTrigger of";
      "                                    true ->";
      "                                        %% Trigger party data: compute immediately";
      "                                        io:format(\"[DEBUG] Node:~p COMPUTE TRIGGER: Party=~p is trigger, computing...~n\", [trace_node_name(RegName), Party]),";
      "                                        AggregatedInputs = Aggregate(MergedProcessed),";
      "                                        Result = ComputeFn(AggregatedInputs, MergedProcessed),";
      "                                        io:format(\"[TRACE] Node:~p Ver:~p Event:Compute Payload:~p~n\", [trace_node_name(RegName), Ver, Result]),";
      "                                        out(RegName, Result),";
      "                                        %% Update Processed map with {last, NodeId} => Result for next @last reference";
      "                                        NodeId = list_to_atom(lists:last(string:split(atom_to_list(RegName), \"_\", all))),";
      "                                        NewProcessed = maps:put({last, NodeId}, Result, MergedProcessed),";
      "                                        io:format(\"[DEBUG] Node:~p Sending to downstreams: ~p~n\", [trace_node_name(RegName), Downstreams]),";
      "                                        lists:foreach(fun(Downstream) ->";
      "                                            case Downstream of";
      "                                                {DownstreamPid, PortTag} ->";
      "                                                    io:format(\"[DEBUG] Node:~p SEND -> ~p : {{~p,~p}, ~p, ~p}~n\", [trace_node_name(RegName), DownstreamPid, Party, Ver, PortTag, Result]),";
      "                                                    DownstreamPid ! {{Party, Ver}, PortTag, Result};";
      "                                                _ -> ok";
      "                                            end";
      "                                        end, Downstreams),";
      "                                        {maps:remove(Version, Buffer), maps:update(Party, Ver + 1, NextVer), NewProcessed, [], Last};";
      "                                    false ->";
      "                                        %% Non-trigger party data: check if Deferred has pending requests.";
      "                                        %% If Deferred is non-empty, it means trigger party already sent request but deps were not ready.";
      "                                        %% Now deps are ready, so we should compute.";
      "                                        io:format(\"[DEBUG] Node:~p Party=~p is NOT trigger, checking Deferred=~p~n\", [trace_node_name(RegName), Party, Deferred]),";
      "                                        case Deferred of";
      "                                            [] ->";
      "                                                %% No pending request from trigger party - just buffer data";
      "                                                io:format(\"[DEBUG] Node:~p No deferred request, buffering data only~n\", [trace_node_name(RegName)]),";
      "                                                {maps:remove(Version, Buffer), maps:update(Party, Ver + 1, NextVer), MergedProcessed, [], Last};";
      "                                            [{DeferredParty, DeferredVer} | RestDeferred] ->";
      "                                                %% Deferred request exists - now deps are ready, compute!";
      "                                                io:format(\"[DEBUG] Node:~p Processing deferred request from ~p Ver ~p, computing...~n\", [trace_node_name(RegName), DeferredParty, DeferredVer]),";
      "                                                AggregatedInputs2 = Aggregate(MergedProcessed),";
      "                                                Result = ComputeFn(AggregatedInputs2, MergedProcessed),";
      "                                                io:format(\"[TRACE] Node:~p Ver:~p Event:Compute(Deferred) Payload:~p~n\", [trace_node_name(RegName), DeferredVer, Result]),";
      "                                                out(RegName, Result),";
      "                                                NodeId2 = list_to_atom(lists:last(string:split(atom_to_list(RegName), \"_\", all))),";
      "                                                NewProcessed2 = maps:put({last, NodeId2}, Result, MergedProcessed),";
      "                                                lists:foreach(fun(Downstream) ->";
      "                                                    case Downstream of";
      "                                                        {DownstreamPid, PortTag} ->";
      "                                                            %% Use DeferredParty and DeferredVer for the message version";
      "                                                            io:format(\"[DEBUG] Node:~p SEND(Deferred) -> ~p : {{~p,~p}, ~p, ~p}~n\", [trace_node_name(RegName), DownstreamPid, DeferredParty, DeferredVer, PortTag, Result]),";
      "                                                            DownstreamPid ! {{DeferredParty, DeferredVer}, PortTag, Result};";
      "                                                        _ -> ok";
      "                                                    end";
      "                                                end, Downstreams),";
      "                                                %% Advance NextVer for both current party AND deferred party";
      "                                                NewNextVer = maps:update(Party, Ver + 1, maps:update(DeferredParty, DeferredVer + 1, NextVer)),";
      "                                                {maps:remove(Version, Buffer), NewNextVer, NewProcessed2, RestDeferred, Last}";
      "                                        end";
      "                                end;";
      "                            false ->";
      "                                %% Not all deps ready - keep in Buffer, merge to Processed, DO NOT advance NextVer";
      "                                io:format(\"[DEBUG] Node:~p DEPS NOT READY, buffering. Missing deps in MergedProcessed~n\", [trace_node_name(RegName)]),";
      "                                {Buffer, NextVer, MergedProcessed, [Version | Deferred], Last}";
      "                        end;";
      "                    false ->";
      "                        io:format(\"[DEBUG] Node:~p Version MISMATCH (Party=~p, Got=~p, Expected=~p), keeping in buffer~n\", [trace_node_name(RegName), Party, Ver, CurrentNextVer]),";
      "                        {Buffer, NextVer, Processed, Deferred, Last}";
      "                end;";
      "            _ ->";
      "                {Buffer, NextVer, Processed, Deferred, Last}";
      "        end";
      "    end, {Buffer0, NextVer0, Processed0, Deferred0, Last0}, HL),";
      "    io:format(\"[DEBUG] Node:~p === Buffer Processing End === NextVer=~p~n\", [trace_node_name(RegName), NextVerT]),";
      "    ";
      "    %% Process Request Buffer (for nodes triggered by sync pulse/request)";
      "    Sorted_req_buf = lists:sort(?SORTVerBuffer, ReqBuffer0),";
      "    io:format(\"[DEBUG] Node:~p === Request Buffer Processing === entries=~p~n\", [trace_node_name(RegName), length(Sorted_req_buf)]),";
      "    {NNextVer, NProcessed, NReqBuffer, NDeferred, NLast} = lists:foldl(fun(E, {NextVer, Processed, ReqBuffer, Deferred, Last}) ->";
      "        case E of";
      "            {Party, Ver} = Version ->";
      "                io:format(\"[DEBUG] Node:~p ReqBuffer entry: Party=~p, Ver=~p~n\", [trace_node_name(RegName), Party, Ver]),";
      "                case lists:member(Party, TriggerParties) of";
      "                    true ->";
      "                        %% Trigger party request: strict version matching required";
      "                        CurrentNextVer = maps:get(Party, NextVer, 0),";
      "                        io:format(\"[DEBUG] Node:~p Request from TRIGGER party ~p: Got=~p, Expected=~p~n\", [trace_node_name(RegName), Party, Ver, CurrentNextVer]),";
      "                        case CurrentNextVer =:= Ver of";
      "                            true ->";
      "                                AllPresent = lists:all(fun(DepName) ->";
      "                                    maps:is_key(DepName, Processed)";
      "                                end, Deps),";
      "                                io:format(\"[DEBUG] Node:~p Request processing: AllDepsPresent=~p, Processed=~p~n\", [trace_node_name(RegName), AllPresent, Processed]),";
      "                                case AllPresent of";
      "                                    true ->";
      "                                        io:format(\"[DEBUG] Node:~p Request COMPUTE: deps ready, computing...~n\", [trace_node_name(RegName)]),";
      "                                        AggregatedInputs3 = Aggregate(Processed),";
      "                                        Result = ComputeFn(AggregatedInputs3, Processed),";
      "                                        io:format(\"[TRACE] Node:~p Ver:~p Event:Compute Payload:~p~n\", [trace_node_name(RegName), Ver, Result]),";
      "                                        out(RegName, Result),";
      "                                        NodeId3 = list_to_atom(lists:last(string:split(atom_to_list(RegName), \"_\", all))),";
      "                                        NewProcessed3 = maps:put({last, NodeId3}, Result, Processed),";
      "                                        lists:foreach(fun(Downstream) ->";
      "                                            case Downstream of";
      "                                                {DownstreamPid, PortTag} ->";
      "                                                    io:format(\"[DEBUG] Node:~p SEND -> ~p : {{~p,~p}, ~p, ~p}~n\", [trace_node_name(RegName), DownstreamPid, Party, Ver, PortTag, Result]),";
      "                                                    DownstreamPid ! {{Party, Ver}, PortTag, Result};";
      "                                                _ -> ok";
      "                                            end";
      "                                        end, Downstreams),";
      "                                        {maps:update(Party, Ver + 1, NextVer), NewProcessed3, ReqBuffer, [], Last};";
      "                                    false ->";
      "                                        %% Deps not ready - defer";
      "                                        io:format(\"[DEBUG] Node:~p Request DEFER: deps not ready~n\", [trace_node_name(RegName)]),";
      "                                        {maps:update(Party, Ver + 1, NextVer), Processed, ReqBuffer, [Version | Deferred], Last}";
      "                                end;";
      "                            false ->";
      "                                %% Future version - keep in buffer";
      "                                io:format(\"[DEBUG] Node:~p Request BUFFER: future version~n\", [trace_node_name(RegName)]),";
      "                                {NextVer, Processed, [Version | ReqBuffer], Deferred, Last}";
      "                        end;";
      "                    false ->";
      "                        %% Non-trigger party request - use strict version matching";
      "                        CurrentNextVer = maps:get(Party, NextVer, 0),";
      "                        io:format(\"[DEBUG] Node:~p Request from NON-trigger party ~p: Got=~p, Expected=~p~n\", [trace_node_name(RegName), Party, Ver, CurrentNextVer]),";
      "                        case CurrentNextVer =:= Ver of";
      "                            true ->";
      "                                io:format(\"[DEBUG] Node:~p Non-trigger request: version match, advancing NextVer~n\", [trace_node_name(RegName)]),";
      "                                {maps:update(Party, Ver + 1, NextVer), Processed, ReqBuffer, Deferred, Last};";
      "                            false ->";
      "                                io:format(\"[DEBUG] Node:~p Non-trigger request: version mismatch, buffering~n\", [trace_node_name(RegName)]),";
      "                                {NextVer, Processed, [Version | ReqBuffer], Deferred, Last}";
      "                        end";
      "                end;";
      "            _ -> {NextVer, Processed, ReqBuffer, Deferred, Last}";
      "        end";
      "    end, {NextVerT, ProcessedT, [], DeferredT, LastT}, Sorted_req_buf),";
      "    ";
      "    receive";
      "        %% Dynamic connection: add downstream (sync call with Ack)";
      "        {add_downstream, NewDownstream, Caller} ->";
      "            NewDownstreams = [NewDownstream | Downstreams],";
      "            Caller ! {ok, connected},";
      "            " ^ func_name ^ "(Config, Connections#{downstreams := NewDownstreams}, NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed, req_buffer := NReqBuffer, deferred := NDeferred, last := NLast});";
      "        ";
      "        %% Dynamic connection: add upstream with port name and initial version (sync call with Ack)";
      "        {add_upstream, DepName, DepParty, PortName, InitialVer, Caller} ->";
      "            io:format(\"[DYN] Node:~p adding upstream ~p from party ~p to port ~p at Ver:~p, CurrentDeps=~p~n\", [RegName, DepName, DepParty, PortName, InitialVer, Deps]),";
      "            NewDeps = [DepName | Deps],";
      "            %% Update dep_to_port mapping";
      "            NewDepToPort = maps:put(DepName, PortName, DepToPort),";
      "            NewConfig = Config#{dependencies := NewDeps, dep_to_port := NewDepToPort},";
      "            %% Initialize NextVer for new party with the provided InitialVer";
      "            NewNextVer = case maps:is_key(DepParty, NNextVer) of true -> NNextVer; false -> maps:put(DepParty, InitialVer, NNextVer) end,";
      "            io:format(\"[DYN] Node:~p NextVer after add_upstream: ~p~n\", [RegName, NewNextVer]),";
      "            %% Also add new party to trigger_parties";
      "            CurrentTriggers = maps:get(trigger_parties, Config, []),";
      "            NewTriggers = case lists:member(DepParty, CurrentTriggers) of true -> CurrentTriggers; false -> [DepParty | CurrentTriggers] end,";
      "            NewConfig2 = NewConfig#{trigger_parties := NewTriggers},";
      "            Caller ! {ok, connected},";
      "            " ^ func_name ^ "(NewConfig2, Connections, NodeState#{buffer := NBuffer, next_ver := NewNextVer, processed := NProcessed, req_buffer := NReqBuffer, deferred := NDeferred, last := NLast});";
      "        ";
      "        {request, {Party, Ver}} ->";
      "            io:format(\"[TRACE] Node:~p Ver:~p Event:Request Payload:none~n\", [trace_node_name(RegName), Ver]),";
      "            " ^ func_name ^ "(Config, Connections,";
      "                NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed,";
      "                          req_buffer := lists:reverse([{Party, Ver} | NReqBuffer]), deferred := NDeferred, last := NLast});";
      "        {{Party, Ver}, InputName, Value} ->";
      "            io:format(\"[TRACE] Node:~p Ver:~p Event:Receive Payload:{~p,~p}~n\", [trace_node_name(RegName), Ver, InputName, Value]),";
      "            UpdatedBuffer = buffer_update(Deps, LastDeps, {{Party, Ver}, InputName, Value}, NBuffer),";
      "            " ^ func_name ^ "(Config, Connections,";
      "                NodeState#{buffer := UpdatedBuffer, next_ver := NNextVer, processed := NProcessed,";
      "                          req_buffer := NReqBuffer, deferred := NDeferred, last := NLast})";
      "    end.";
    ]
  else
    (* Source node (no dependencies) *)
    String.concat "\n" [
      func_name ^ "(Config, Connections, NodeState) ->";
      "    #{register_name := RegName, compute := ComputeFn} = Config,";
      "    #{downstreams := Downstreams} = Connections,";
      "    #{buffer := Buffer0, next_ver := NextVer0, processed := Processed0, req_buffer := ReqBuffer0, deferred := Deferred0, last := Last0} = NodeState,";
      "    ";
      "    %% Process Buffer (source nodes have no inputs)";
      "    HL = lists:sort(?SORTBuffer, maps:to_list(Buffer0)),";
      "    {NBuffer, NextVerT, ProcessedT, DeferredT, LastT} = lists:foldl(fun(_E, {Buffer, NextVer, Processed, Deferred, Last}) ->";
      "        {Buffer, NextVer, Processed, Deferred, Last}";
      "    end, {Buffer0, NextVer0, Processed0, Deferred0, Last0}, HL),";
      "    ";
      "    %% Process Request Buffer";
      "    Sorted_req_buf = lists:sort(?SORTVerBuffer, ReqBuffer0),";
      "    io:format(\"[DEBUG] SourceNode:~p === Request Processing === NextVer=~p, ReqBuf=~p~n\", [trace_node_name(RegName), NextVer0, Sorted_req_buf]),";
      "    {NNextVer, NProcessed, NReqBuffer, NDeferred, NLast} = lists:foldl(fun(E, {NextVer, Processed, ReqBuffer, Deferred, Last}) ->";
      "        case E of";
      "            {Party, Ver} = Version ->";
      "                CurrentNextVer = maps:get(Party, NextVer, 0),";
      "                io:format(\"[DEBUG] SourceNode:~p Request: Party=~p, Ver=~p, Expected=~p~n\", [trace_node_name(RegName), Party, Ver, CurrentNextVer]),";
      "                case CurrentNextVer =:= Ver of";
      "                    true ->";
      "                        %% Source node: compute value using injected function (passes Processed for @last references)";
      "                        io:format(\"[DEBUG] SourceNode:~p Version MATCH, computing...~n\", [trace_node_name(RegName)]),";
      "                        Result = ComputeFn(#{}, Processed),";
      "                        io:format(\"[TRACE] Node:~p Ver:~p Event:Compute Payload:~p~n\", [trace_node_name(RegName), Ver, Result]),";
      "                        out(RegName, Result),";
      "                        %% Update Processed map with {last, NodeId} => Result for next @last reference";
      "                        NodeId = list_to_atom(lists:last(string:split(atom_to_list(RegName), \"_\", all))),";
      "                        NewProcessed = maps:put({last, NodeId}, Result, Processed),";
      "                        ";
      "                        %% Send to downstreams";
      "                        io:format(\"[DEBUG] SourceNode:~p Sending to Downstreams=~p~n\", [trace_node_name(RegName), Downstreams]),";
      "                        lists:foreach(fun({DownstreamName, PortTag}) ->";
      "                            io:format(\"[DEBUG] SourceNode:~p SEND -> ~p : {{~p,~p}, ~p, ~p}~n\", [trace_node_name(RegName), DownstreamName, Party, Ver, PortTag, Result]),";
      "                            DownstreamName ! {{Party, Ver}, PortTag, Result}";
      "                        end, Downstreams),";
      "                        ";
      "                        {maps:update(Party, Ver + 1, NextVer), NewProcessed, ReqBuffer, [], Last};";
      "                    false ->";
      "                        io:format(\"[DEBUG] SourceNode:~p Version MISMATCH, buffering~n\", [trace_node_name(RegName)]),";
      "                        {NextVer, Processed, [Version | ReqBuffer], Deferred, Last}";
      "                end;";
      "            _ ->";
      "                {NextVer, Processed, ReqBuffer, Deferred, Last}";
      "        end";
      "    end, {NextVerT, ProcessedT, [], DeferredT, LastT}, Sorted_req_buf),";
      "    ";
      "    receive";
      "        %% Dynamic connection: add downstream (sync call with Ack)";
      "        {add_downstream, NewDownstream, Caller} ->";
      "            NewDownstreams = [NewDownstream | Downstreams],";
      "            Caller ! {ok, connected},";
      "            " ^ func_name ^ "(Config, Connections#{downstreams := NewDownstreams}, NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed, req_buffer := NReqBuffer, deferred := NDeferred, last := NLast});";
      "        ";
      "        {request, {Party, Ver}} ->";
      "            io:format(\"[TRACE] Node:~p Ver:~p Event:Request Payload:none~n\", [trace_node_name(RegName), Ver]),";
      "            " ^ func_name ^ "(Config, Connections,";
      "                NodeState#{buffer := NBuffer, next_ver := NNextVer, processed := NProcessed,";
      "                          req_buffer := lists:reverse([{Party, Ver} | NReqBuffer]), deferred := NDeferred, last := NLast})";
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
    (* owner_party is passed via Config at spawn time, not here *)
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
        
        let spec = Printf.sprintf "        %s => #{party => %s, module => '%s', dependencies => %s}"
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
      
      (* Build node_dependencies map: NodePid => [dependency names from expr] *)
      let node_deps_entries = List.map (fun (node_id, _, _, expr) ->
        let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
        let deps = unique_list (extract_node_deps expr) in
        Printf.sprintf "%s => [%s]" qualified (String.concat ", " deps)
      ) mod_info.node in
      let node_deps_str = "#{" ^ String.concat ", " node_deps_entries ^ "}" in
      
      (* Build upstream_parties map: dependency_name => PartyId *)
      (* For each input port, determine which party it comes from *)
      let upstream_entries = List.mapi (fun i input_qid ->
        let port_name = List.nth mod_info.source i in
        match input_qid with
        | Syntax.SimpleId _ -> 
            (* Same party input *)
            Printf.sprintf "%s => %s" port_name party_id
        | Syntax.QualifiedId (ref_party, _) ->
            (* Cross-party input *)
            Printf.sprintf "%s => %s" port_name ref_party
      ) inputs in
      let upstream_parties_str = "#{" ^ String.concat ", " upstream_entries ^ "}" in
      
      (* Build sink_nodes list: nodes that are marked as output (sink) in the module *)
      (* Following Original's gen_request_node_fun: only sink nodes without same-party deps get requests *)
      let sink_node_names = List.filter_map (fun (node_id, _, _, _) ->
        if List.mem node_id mod_info.sink then
          Some (party_id ^ "_" ^ inst_name ^ "_" ^ node_id)
        else
          None
      ) mod_info.node in
      let sink_nodes_str = "[" ^ String.concat ", " sink_node_names ^ "]" in
      
      (* Spawn module actor with new Config structure including sink_nodes *)
      let module_spawn = Printf.sprintf
        "    PID_%s_%s = spawn(?MODULE, run_%s, [[], [], %s, 0, #{nodes => %s, node_dependencies => %s, upstream_parties => %s, sink_nodes => %s}, %s]),\n    register(%s_%s, PID_%s_%s)"
        party_id inst_name module_id party_id nodes_str node_deps_str upstream_parties_str sink_nodes_str sync_downstreams party_id inst_name party_id inst_name in
      spawn_code := module_spawn :: !spawn_code;
      
      (* Spawn node actors (only for compute nodes) *)
      List.iter (fun (node_id, _, _, expr) ->
        let qualified = party_id ^ "_" ^ inst_name ^ "_" ^ node_id in
        
        (* Generate compute function - takes Inputs and Processed maps *)
        (* Processed map contains {last, Id} keys for @last references (following Original) *)
        let compute_fn = "fun(Inputs, Processed) -> " ^ expr_to_erlang expr ^ " end" in
        
        (* Build dependency list - use actual expression dependencies, not module input ports *)
        let node_deps = unique_list (extract_node_deps expr) in
        let deps_str = "[" ^ String.concat ", " node_deps ^ "]" in
        
        (* Build @last dependency list - nodes referenced with @last annotation *)
        let last_deps = unique_list (extract_last_deps expr) in
        let last_deps_str = "[" ^ String.concat ", " last_deps ^ "]" in
        
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
        
        (* Build dep_to_port: maps upstream node names to their target port names *)
        (* For this node, check which module input ports its dependencies correspond to *)
        let dep_to_port_entries = List.filter_map (fun dep_name ->
          (* Find which port this dependency maps to *)
          let port_idx = List.mapi (fun i port -> (i, port)) mod_info.source in
          let matching_port = List.find_opt (fun (_, port) -> port = dep_name) port_idx in
          match matching_port with
          | Some (_, port) -> Some (Printf.sprintf "%s => %s" dep_name port)
          | None -> None
        ) node_deps in
        let dep_to_port_str = "#{" ^ String.concat ", " dep_to_port_entries ^ "}" in
        
        (* Get variadic ports from module info *)
        let variadic_ports_str = "[" ^ String.concat ", " mod_info.variadic_inputs ^ "]" in
        
        (* Add trigger_parties to Config - determines when computation triggers *)
        (* For periodic nodes, trigger_parties = [OwnerParty] *)
        (* For any_party(p, q) nodes, trigger_parties = [p, q] (future extension) *)
        (* Config includes last_deps for @last references - used in buffer_update (following Original) *)
        let node_spawn = Printf.sprintf
          "    PID_%s = spawn(?MODULE, run_%s_%s, [#{register_name => %s, dependencies => %s, last_deps => %s, compute => %s, trigger_parties => [%s], dep_to_port => %s, variadic_ports => %s}, #{downstreams => %s}, #{buffer => #{}, next_ver => #{%s}, processed => #{}, req_buffer => [], deferred => [], last => #{}}]),\n    register(%s, PID_%s)"
          qualified module_id node_id qualified deps_str last_deps_str compute_fn party_id dep_to_port_str variadic_ports_str downstreams next_ver_init qualified qualified in
        spawn_code := node_spawn :: !spawn_code
      ) mod_info.node
    ) all_instances;
    
    (* Add party actor config - no subscribers, only leader receives sync pulse *)
    (* Party is registered directly as party_id (e.g., 'main') for consistency with factory *)
    let party_config_code = String.concat "\n" [
      "    PartyConfig_" ^ party_id ^ " = #{";
      "        party => " ^ party_id ^ ",";
      "        leader => " ^ party_id ^ "_" ^ leader ^ ",";
      "        mode => periodic,";
      "        interval => " ^ string_of_int interval;
      "    },";
      "    PID_party_" ^ party_id ^ " = spawn(?MODULE, run_party, [PartyConfig_" ^ party_id ^ ", 0]),";
      "    register(" ^ party_id ^ ", PID_party_" ^ party_id ^ ")";
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
  
  (* Default case: allow dynamic nodes (undefined name) or unknown nodes *)
  String.concat "\n" (!entries @ ["out(undefined, Value) -> io:format(\"[Dynamic] Output: ~p~n\", [Value]), void;"; "out(_, _) -> void."])

(* Generate export declarations *)
let gen_exports inst_prog module_map =
  let unique_modules = get_unique_modules inst_prog in
  let exports = ref ["-export([start/0, out/2, run_party/2, compute_request_targets/4])."] in
  
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

(* Generate exports for template factory functions *)
let gen_template_exports inst_prog =
  let templates = inst_prog.Syntax.templates in
  if List.length templates = 0 then ""
  else begin
    let factory_exports = List.map (fun template ->
      let has_params = List.length template.Syntax.template_params > 0 in
      let arity = if has_params then "3" else "2" in  (* PartyName, InstanceNameMap [, ConnectionTargets] *)
      "create_" ^ template.Syntax.template_id ^ "/" ^ arity
    ) templates in
    let manager_exports = ["list_templates/0"; "stop_party/1"; "get_parties/0"; "get_instances/0"; "check_instance_names/1"] in
    "-export([" ^ String.concat ", " (factory_exports @ manager_exports) ^ "])."
  end

(* Generate a single template factory function with dynamic instance naming *)
let gen_single_template_factory template module_map inst_prog =
  let tid = template.Syntax.template_id in
  let params = template.Syntax.template_params in  (* テンプレートパラメータ *)
  let leader = template.Syntax.template_leader in
  let periodic = string_of_int template.Syntax.template_periodic in
  let instances = template.Syntax.template_instances in
  let template_outputs = template.Syntax.template_outputs in  (* 動的接続定義 *)
  let static_parties = inst_prog.Syntax.parties in  (* 静的パーティ情報 *)
  
  (* Collect template instance names (local names like c4, c5) *)
  let template_inst_names = List.map (fun (outputs, _, _) -> List.hd outputs) instances in
  let template_names_str = "[" ^ String.concat ", " template_inst_names ^ "]" in
  
  (* Generate parameter keys list for API *)
  let param_names_str = "[" ^ String.concat ", " params ^ "]" in
  
  (* Helper: capitalize first letter for Erlang variable *)
  let capitalize s = String.capitalize_ascii s in
  
  (* Two-pass generation: first define all NodeName variables, then spawn *)
  let name_def_lines = ref [] in
  let spawn_lines = ref [] in
  let instance_vars = ref [] in
  
  (* Pass 1: Define all NodeName variables for all instances *)
  (* NodeName format: PartyName_InstanceName_NodeId for proper namespacing *)
  List.iter (fun (outputs, module_id, _inputs) ->
    let local_name = List.hd outputs in
    let var_prefix = capitalize local_name in
    
    if M.mem module_id module_map then begin
      let mod_info = M.find module_id module_map in
      List.iter (fun (node_id, _, _, _) ->
        name_def_lines := (Printf.sprintf "                    %s_NodeName = list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)) ++ \"_%s\")" var_prefix local_name node_id) :: !name_def_lines
      ) mod_info.node
    end
  ) instances;
  
  (* Pass 2: Spawn all node actors and module actors *)
  List.iter (fun (outputs, module_id, inputs) ->
    let local_name = List.hd outputs in  (* Template local name like c4 *)
    let var_prefix = capitalize local_name in  (* C4 for Erlang variable *)
    
    let has_mod_info = M.mem module_id module_map in
    if has_mod_info then begin
      let mod_info = M.find module_id module_map in
      
      (* Build node_dependencies map: NodePid variable => [dependency names from expr] *)
      (* This is the static information from the module definition *)
      let node_deps_entries = List.map (fun (node_id, _, _, expr) ->
        let deps = unique_list (extract_node_deps expr) in
        Printf.sprintf "%s_NodePid => [%s]" var_prefix (String.concat ", " deps)
      ) mod_info.node in
      let node_deps_str = "#{" ^ String.concat ", " node_deps_entries ^ "}" in
      
      (* Build upstream_parties map: dependency_name => PartyId *)
      (* For template instances, we need to determine party based on input sources *)
      let upstream_entries = List.mapi (fun i input_qid ->
        let port_name = List.nth mod_info.source i in
        match input_qid with
        | Syntax.SimpleId _ -> 
            (* Same party input - use PartyName variable *)
            Printf.sprintf "%s => PartyName" port_name
        | Syntax.QualifiedId (ref_party, _) ->
            (* Cross-party input - use the referenced party atom *)
            Printf.sprintf "%s => %s" port_name ref_party
        | Syntax.ParamRef param_name ->
            (* Parameter reference - party is determined at runtime from ConnectionTargets *)
            Printf.sprintf "%s => list_to_atom(hd(string:split(atom_to_list(maps:get(%s, ConnectionTargets)), \"_\", leading)))" port_name param_name
      ) inputs in
      let upstream_parties_str = "#{" ^ String.concat ", " upstream_entries ^ "}" in
      
      (* Spawn node actors for this module *)
      List.iter (fun (node_id, _, _, expr) ->
        let compute_fn = "fun(Inputs, Processed) -> " ^ expr_to_erlang expr ^ " end" in
        (* Build dependency list - use actual expression dependencies, not module input ports *)
        let node_deps = unique_list (extract_node_deps expr) in
        let deps_str = "[" ^ String.concat ", " node_deps ^ "]" in
        
        (* Build @last dependency list - nodes referenced with @last annotation *)
        let last_deps = unique_list (extract_last_deps expr) in
        let last_deps_str = "[" ^ String.concat ", " last_deps ^ "]" in
        
        (* Build downstreams list: find all instances within this template that use this node as input *)
        let downstream_list = ref [] in
        List.iter (fun (outputs_dst, module_id_dst, inputs_dst) ->
          let inst_name_dst = List.hd outputs_dst in
          let dst_var_prefix = capitalize inst_name_dst in
          (* Check if any input of dst instance references current instance (local_name) *)
          List.iteri (fun i input_qid ->
            let should_add = match input_qid with
              | Syntax.SimpleId id -> id = local_name
              | Syntax.QualifiedId (_, _) -> false  (* Cross-party refs handled differently *)
              | Syntax.ParamRef _ -> false  (* Parameter refs are external, not local *)
            in
            if should_add then begin
              let dst_mod = M.find module_id_dst module_map in
              let dst_port = List.nth dst_mod.source i in
              (* Get first compute node of destination module *)
              match dst_mod.node with
              | (first_node_id, _, _, _) :: _ ->
                  (* Use dynamic name reference: {maps:get(dst, InstanceNameMap)_NodeName, port} *)
                  let dst_node_name = Printf.sprintf "{%s_NodeName, %s}" dst_var_prefix dst_port in
                  downstream_list := dst_node_name :: !downstream_list
              | [] -> ()
            end
          ) inputs_dst
        ) instances;
        
        let downstreams_str = if !downstream_list = [] then "[]" else
          "[" ^ String.concat ", " !downstream_list ^ "]"
        in
        
        (* Build dep_to_port mapping for this node *)
        let dep_to_port_entries = List.map (fun dep -> Printf.sprintf "%s => %s" dep dep) node_deps in
        let dep_to_port_str = "#{" ^ String.concat ", " dep_to_port_entries ^ "}" in
        
        (* Build variadic_ports list from module info *)
        let variadic_str = "[" ^ String.concat ", " mod_info.variadic_inputs ^ "]" in
        
        (* NodeName is already defined in Pass 1, only spawn here *)
        (* Use CurrentVer (from suspend or 0 for new party) for initial next_ver *)
        (* Config includes last_deps for @last references - used in buffer_update (following Original) *)
        spawn_lines := (Printf.sprintf "                    %s_NodePid = spawn(fun() -> run_%s_%s(#{register_name => %s_NodeName, dependencies => %s, last_deps => %s, compute => %s, trigger_parties => [PartyName], dep_to_port => %s, variadic_ports => %s}, #{downstreams => %s}, #{buffer => #{}, next_ver => #{PartyName => CurrentVer}, processed => #{}, req_buffer => [], deferred => [], last => #{}}) end)" var_prefix module_id node_id var_prefix deps_str last_deps_str compute_fn dep_to_port_str variadic_str downstreams_str) :: !spawn_lines;
        spawn_lines := (Printf.sprintf "                    true = register(%s_NodeName, %s_NodePid)" var_prefix var_prefix) :: !spawn_lines
      ) mod_info.node;
      
      (* Build node PIDs list *)
      let node_pids = List.map (fun (node_id, _, _, _) ->
        var_prefix ^ "_NodePid"
      ) mod_info.node in
      let nodes_str = "[" ^ String.concat ", " node_pids ^ "]" in
      
      (* Calculate SyncDownstreams for this module *)
      (* Same-party upstream instances that should receive sync pulse forwarding *)
      (* Use namespaced names: PartyName_InstanceName *)
      let upstream_instance_names = List.filter_map (fun input_qid ->
        match input_qid with
        | Syntax.SimpleId id ->
            (* Same-party dependency - check if it's a different instance in this template *)
            let is_other_instance = List.exists (fun (out, _, _) -> 
              List.hd out = id && id <> local_name
            ) instances in
            if is_other_instance then
              Some (Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)))" id)
            else
              None
        | Syntax.QualifiedId (_, _) -> None  (* Cross-party: don't add to sync downstream *)
        | Syntax.ParamRef _ -> None  (* Parameter refs: connection handled by output_stmt *)
      ) inputs in
      let sync_downstreams_str = "[" ^ String.concat ", " upstream_instance_names ^ "]" in
      
      (* Spawn module actor with new Config structure: node_dependencies + upstream_parties *)
      (* Instance registration name format: PartyName_InstanceName for proper namespacing *)
      (* Use CurrentVer for Party_ver to sync with existing party state *)
      spawn_lines := (Printf.sprintf "                    %s_Name = list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)))" var_prefix local_name) :: !spawn_lines;
      spawn_lines := (Printf.sprintf "                    %s_Pid = spawn(fun() -> run_%s([], [], PartyName, CurrentVer, #{nodes => %s, node_dependencies => %s, upstream_parties => %s}, %s) end)" var_prefix module_id nodes_str node_deps_str upstream_parties_str sync_downstreams_str) :: !spawn_lines;
      spawn_lines := (Printf.sprintf "                    true = register(%s_Name, %s_Pid)" var_prefix var_prefix) :: !spawn_lines;
      instance_vars := (var_prefix ^ "_Pid") :: !instance_vars
    end else begin
      spawn_lines := (Printf.sprintf "                    %s_Name = list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)))" var_prefix local_name) :: !spawn_lines;
      spawn_lines := (Printf.sprintf "                    %s_Pid = spawn(fun() -> receive stop -> ok end end)" var_prefix) :: !spawn_lines;
      spawn_lines := (Printf.sprintf "                    true = register(%s_Name, %s_Pid)" var_prefix var_prefix) :: !spawn_lines;
      instance_vars := (var_prefix ^ "_Pid") :: !instance_vars
    end
  ) instances;
  
  (* Generate leader name and party config (used later in suspend/resume flow) *)
  let leader_var = capitalize leader in
  let _ = leader_var in  (* suppress unused warning *)
  spawn_lines := (Printf.sprintf "                    LeaderName = list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)))" leader) :: !spawn_lines;
  spawn_lines := (Printf.sprintf "                    PartyConfig = #{party => PartyName, leader => LeaderName, mode => periodic, interval => %s}" periodic) :: !spawn_lines;
  (* Note: PartyPid is handled in the suspend/resume flow at the end of the factory function *)
  
  (* Generate dynamic connection code for template_outputs *)
  (* template_outputs example: main.s1(c4, c5) => target=main.s1 receives data from c4, c5 *)
  (* Format: (QualifiedId(party, node), [local_instance1; local_instance2; ...]) *)
  let connection_lines = ref [] in
  
  (* Generate connections for ParamRef inputs in newnode statements *)
  (* e.g., newnode (p) = pass($upstream) needs: upstream -> p connection *)
  List.iter (fun (outputs, module_id, inputs) ->
    let local_name = List.hd outputs in
    if M.mem module_id module_map then begin
      let mod_info = M.find module_id module_map in
      List.iteri (fun i input_qid ->
        match input_qid with
        | Syntax.ParamRef param_name ->
            (* Parameter reference - need to connect from ConnectionTargets[param_name] to local instance *)
            let port_name = List.nth mod_info.source i in
            (match mod_info.node with
            | (first_node_id, _, _, _) :: _ ->
                let local_node_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)) ++ \"_%s\")" local_name first_node_id in
                let upstream_node_name = Printf.sprintf "maps:get(%s, ConnectionTargets)" param_name in
                (* Extract party name from upstream node's registered name *)
                connection_lines := (Printf.sprintf "                    UpstreamNodeName_%s = %s" local_name upstream_node_name) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    UpstreamParty_%s = list_to_atom(hd(string:split(atom_to_list(UpstreamNodeName_%s), \"_\", leading)))" local_name local_name) :: !connection_lines;
                (* Get current version from upstream party for proper synchronization *)
                connection_lines := (Printf.sprintf "                    UpstreamParty_%s ! {get_version, self()}" local_name) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    UpstreamVer_%s = receive {version, V_%s} -> V_%s after 5000 -> 0 end" local_name local_name local_name) :: !connection_lines;
                (* Upstream adds local as downstream: upstream sends data to local *)
                connection_lines := (Printf.sprintf "                    UpstreamNodeName_%s ! {add_downstream, {%s, %s}, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" local_name local_node_name port_name) :: !connection_lines;
                (* Local adds upstream as upstream with initial version: local receives data from upstream *)
                connection_lines := (Printf.sprintf "                    %s ! {add_upstream, %s, UpstreamParty_%s, %s, UpstreamVer_%s, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" local_node_name port_name local_name port_name local_name) :: !connection_lines;
                (* Add sync downstream: upstream's module actor forwards sync pulses to local's module actor *)
                let local_module_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)))" local_name in
                connection_lines := (Printf.sprintf "                    UpstreamModuleName_%s = list_to_atom(string:join(lists:droplast(string:split(atom_to_list(UpstreamNodeName_%s), \"_\", all)), \"_\"))" local_name local_name) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    UpstreamModuleName_%s ! {add_sync_downstream, %s, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" local_name local_module_name) :: !connection_lines
            | [] -> ())
        | Syntax.SimpleId _ | Syntax.QualifiedId _ -> ()  (* Handled elsewhere *)
      ) inputs
    end
  ) instances;
  
  (* Helper: find module_id for an instance in a static party *)
  let find_static_instance_module party_id inst_id =
    try
      let party = List.find (fun p -> p.Syntax.party_id = party_id) static_parties in
      let (_, mod_id, _) = List.find (fun (outs, _, _) -> List.hd outs = inst_id) party.Syntax.instances in
      Some mod_id
    with Not_found -> None
  in
  
  List.iter (fun (target_qid, source_locals) ->
    match target_qid with
    | Syntax.QualifiedId (target_party, target_inst) ->
        (* Target is in another static party *)
        (* For each source local instance, find its output node and send add_downstream to it *)
        (* Also send add_upstream to the target node *)
        
        (* Find target instance's module and its first node id and first input port *)
        let (target_node_suffix, target_input_port) = 
          match find_static_instance_module target_party target_inst with
          | Some target_mod_id when M.mem target_mod_id module_map ->
              let target_mod = M.find target_mod_id module_map in
              let node_id = (match target_mod.node with
               | (node_id, _, _, _) :: _ -> node_id
               | [] -> "data") in
              (* Get first variadic input port name if available, otherwise first source *)
              let input_port = (match target_mod.variadic_inputs with
               | port_name :: _ -> port_name  (* variadic input *)
               | [] -> 
                   match target_mod.source with
                   | src_id :: _ -> src_id
                   | [] -> "input") in
              (node_id, input_port)
          | _ -> ("data", "input")  (* fallback *)
        in
        
        List.iter (fun src_local ->
          let src_var = capitalize src_local in
          let _ = src_var in
          (* Source node name: PartyName_InstanceName_<first_node_id> *)
          (* We need to find the first node of the source instance *)
          let src_module_id = 
            try 
              let (_, mod_id, _) = List.find (fun (outs, _, _) -> List.hd outs = src_local) instances in
              mod_id
            with Not_found -> "unknown"
          in
          if M.mem src_module_id module_map then begin
            let src_mod = M.find src_module_id module_map in
            match src_mod.node with
            | (first_node_id, _, _, _) :: _ ->
                (* Source node actor name: template instance's node *)
                let src_node_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)) ++ \"_%s\")" src_local first_node_id in
                (* Target node actor name: static party's node <target_party>_<target_inst>_<target_node_id> *)
                let target_node_name = Printf.sprintf "'%s_%s_%s'" target_party target_inst target_node_suffix in
                (* Use src_local as the dependency name (for tracking), and target_input_port for port mapping *)
                let dep_name = src_local in
                let port_name = target_input_port in
                (* Sync call with Ack for add_downstream: src node sends data to target node with dep_name tag *)
                (* Data will be tagged with dep_name so it can be uniquely identified in MergedProcessed *)
                connection_lines := (Printf.sprintf "                    %s ! {add_downstream, {%s, %s}, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" src_node_name target_node_name dep_name) :: !connection_lines;
                (* Sync call with Ack for add_upstream with initial version: target node receives data from src *)
                (* dep_name is used as dependency identifier, port_name for aggregation mapping *)
                connection_lines := (Printf.sprintf "                    %s ! {add_upstream, %s, PartyName, %s, CurrentVer, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" target_node_name dep_name port_name) :: !connection_lines;
                (* Add sync downstream: TARGET's module actor (s1) forwards sync pulses to SOURCE's module actor (c4) *)
                (* This is because s1 is the leader/sync source and needs to trigger c4's computation *)
                let src_module_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)))" src_local in
                let target_module_name = Printf.sprintf "'%s_%s'" target_party target_inst in
                connection_lines := (Printf.sprintf "                    %s ! {add_sync_downstream, %s, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" target_module_name src_module_name) :: !connection_lines
            | [] -> ()
          end
        ) source_locals
    | Syntax.ParamRef param_name ->
        (* ParamRef: $upstream(n1) means "n1 receives data from upstream" *)
        (* upstream = the external data source (e.g., main_src_data) *)
        (* n1 = the local instance that will receive data *)
        (* Data flow: upstream -> n1 *)
        List.iter (fun local_inst ->
          let _ = capitalize local_inst in
          let local_module_id = 
            try 
              let (_, mod_id, _) = List.find (fun (outs, _, _) -> List.hd outs = local_inst) instances in
              mod_id
            with Not_found -> "unknown"
          in
          if M.mem local_module_id module_map then begin
            let local_mod = M.find local_module_id module_map in
            match local_mod.node with
            | (first_node_id, _, _, _) :: _ ->
                (* Local node actor name: this template instance's node (receiver) *)
                let local_node_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)) ++ \"_%s\")" local_inst first_node_id in
                (* Upstream node actor name: resolved from ConnectionTargets parameter (sender) *)
                let upstream_node_name = Printf.sprintf "maps:get(%s, ConnectionTargets)" param_name in
                (* Port tag should match the module's input port name (e.g., "input" for pass module) *)
                let port_tag = if List.length local_mod.source > 0 then List.hd local_mod.source else local_inst in
                (* Extract party name from upstream node's registered name *)
                connection_lines := (Printf.sprintf "                    UpstreamNodeName_%s = %s" local_inst upstream_node_name) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    UpstreamParty_%s = list_to_atom(hd(string:split(atom_to_list(UpstreamNodeName_%s), \"_\", leading)))" local_inst local_inst) :: !connection_lines;
                (* Get current version from upstream party *)
                connection_lines := (Printf.sprintf "                    UpstreamParty_%s ! {get_version, self()}" local_inst) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    UpstreamVer_%s = receive {version, V_%s} -> V_%s after 5000 -> 0 end" local_inst local_inst local_inst) :: !connection_lines;
                (* Upstream adds local as downstream: upstream sends data to local *)
                connection_lines := (Printf.sprintf "                    UpstreamNodeName_%s ! {add_downstream, {%s, %s}, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" local_inst local_node_name port_tag) :: !connection_lines;
                (* Local adds upstream as upstream with initial version: local receives data from upstream *)
                connection_lines := (Printf.sprintf "                    %s ! {add_upstream, %s, UpstreamParty_%s, %s, UpstreamVer_%s, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" local_node_name port_tag local_inst port_tag local_inst) :: !connection_lines;
                (* Add sync downstream: upstream's module actor forwards sync pulses to local's module actor *)
                let local_module_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)))" local_inst in
                connection_lines := (Printf.sprintf "                    UpstreamModuleName_%s = list_to_atom(string:join(lists:droplast(string:split(atom_to_list(UpstreamNodeName_%s), \"_\", all)), \"_\"))" local_inst local_inst) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    UpstreamModuleName_%s ! {add_sync_downstream, %s, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" local_inst local_module_name) :: !connection_lines
            | [] -> ()
          end
        ) source_locals
    | Syntax.SimpleId target_inst ->
        (* Target is in the same party - need different handling *)
        List.iter (fun src_local ->
          let src_var = capitalize src_local in
          let _ = src_var in
          let src_module_id = 
            try 
              let (_, mod_id, _) = List.find (fun (outs, _, _) -> List.hd outs = src_local) instances in
              mod_id
            with Not_found -> "unknown"
          in
          if M.mem src_module_id module_map then begin
            let src_mod = M.find src_module_id module_map in
            match src_mod.node with
            | (first_node_id, _, _, _) :: _ ->
                let src_node_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_\" ++ atom_to_list(maps:get(%s, InstanceNameMap)) ++ \"_%s\")" src_local first_node_id in
                let target_node_name = Printf.sprintf "list_to_atom(atom_to_list(PartyName) ++ \"_%s_mean\")" target_inst in
                let port_tag = src_local in
                (* Sync call with Ack for add_downstream *)
                connection_lines := (Printf.sprintf "                    %s ! {add_downstream, {%s, %s}, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" src_node_name target_node_name port_tag) :: !connection_lines;
                (* Get current version from party for proper sync *)
                connection_lines := (Printf.sprintf "                    PartyPid_%s = whereis(PartyName)" src_local) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    PartyPid_%s ! {get_version, self()}" src_local) :: !connection_lines;
                connection_lines := (Printf.sprintf "                    InitVer_%s = receive {version, V_%s} -> V_%s after 5000 -> 0 end" src_local src_local src_local) :: !connection_lines;
                (* Sync call with Ack for add_upstream - with version *)
                connection_lines := (Printf.sprintf "                    %s ! {add_upstream, %s, PartyName, %s, InitVer_%s, self()}, receive {ok, connected} -> ok after 5000 -> timeout end" target_node_name port_tag port_tag src_local) :: !connection_lines
            | [] -> ()
          end
        ) source_locals
  ) template_outputs;
  
  let pids_list = "[" ^ String.concat ", " (List.rev !instance_vars) ^ ", FinalPartyPid]" in
  let instance_names_list = "[" ^ String.concat ", " (List.map (fun (outputs, _, _) -> capitalize (List.hd outputs) ^ "_Name") instances) ^ "]" in
  
  (* Combine name definitions (Pass 1) with spawn code (Pass 2) and connection code *)
  let all_code_lines = (List.rev !name_def_lines) @ (List.rev !spawn_lines) @ (List.rev !connection_lines) in
  
  (* Generate function signature based on whether template has params *)
  let has_params = List.length params > 0 in
  let func_signature = if has_params then
    "create_" ^ tid ^ "(PartyName, InstanceNameMap, ConnectionTargets) ->"
  else
    "create_" ^ tid ^ "(PartyName, InstanceNameMap) ->"
  in
  let param_check_code = if has_params then
    [
      "    RequiredParams = " ^ param_names_str ^ ",";
      "    case lists:all(fun(K) -> maps:is_key(K, ConnectionTargets) end, RequiredParams) of";
      "        false -> {error, {missing_connection_targets, RequiredParams}};";
      "        true ->"
    ]
  else
    []
  in
  (* When params exist, we have 4 nested cases, so we need 3 "end" (4th closed by "end,")
     When no params, we have 3 nested cases, so we need 2 "end" *)
  let param_end_code = if has_params then ["            end"; "    end."] else ["    end."] in
  
  String.concat "\n" ([
    "%% Factory function for template: " ^ tid;
    "%% InstanceNameMap: #{" ^ String.concat ", " (List.map (fun n -> n ^ " => actual_name") template_inst_names) ^ "}";
    (if has_params then "%% ConnectionTargets: #{" ^ String.concat ", " (List.map (fun p -> p ^ " => target_node_name") params) ^ "}" else "");
    "%% Supports atomic updates via suspend/resume when adding to existing party";
    func_signature;
  ] @ param_check_code @ [
    "    RequiredKeys = " ^ template_names_str ^ ",";
    "    case lists:all(fun(K) -> maps:is_key(K, InstanceNameMap) end, RequiredKeys) of";
    "        false -> {error, {missing_instance_names, RequiredKeys}};";
    "        true ->";
    "            NewNames = maps:values(InstanceNameMap),";
    "            case check_instance_names(NewNames) of";
    "                {error, Reason} -> {error, Reason};";
    "                ok ->";
    "                    %% Check if party already exists - if so, suspend it first";
    "                    %% Party is registered directly as PartyName (e.g., 'main')";
    "                    {PartyPid, CurrentVer} = case whereis(PartyName) of";
    "                        undefined ->";
    "                            %% New party - start fresh at Ver 0";
    "                            {undefined, 0};";
    "                        ExistingPid ->";
    "                            %% Existing party - suspend and get current version";
    "                            ExistingPid ! {suspend, self()},";
    "                            receive";
    "                                {ok, Ver} -> {ExistingPid, Ver}";
    "                            after 5000 ->";
    "                                {ExistingPid, 0}  %% Timeout fallback";
    "                            end";
    "                    end,";
    "                    io:format(\"[FACTORY] Creating instances in party ~p at Ver:~p~n\", [PartyName, CurrentVer]),";
    String.concat ",\n" all_code_lines ^ ",";
    "                    %% All connections complete (sync calls with Ack)";
    "                    %% Now safe to resume or start the party";
    "                    io:format(\"[FACTORY] All connections complete, resuming party~n\"),";
    "                    FinalPartyPid = case PartyPid of";
    "                        undefined ->";
    "                            NewPid = spawn(fun() -> run_party(PartyConfig, CurrentVer) end),";
    "                            true = register(PartyName, NewPid),";
    "                            NewPid;";
    "                        _ ->";
    "                            PartyPid ! {resume},";
    "                            PartyPid";
    "                    end,";
    "                    manager_register(PartyName, " ^ pids_list ^ ", " ^ instance_names_list ^ "),";
    "                    {ok, PartyName, FinalPartyPid}";
    "            end";
  ] @ param_end_code)

(* Generate factory functions for each template *)
let gen_template_factories inst_prog module_map =
  let templates = inst_prog.Syntax.templates in
  if List.length templates = 0 then ""
  else begin
    String.concat "\n\n" (List.map (fun template ->
      gen_single_template_factory template module_map inst_prog
    ) templates)
  end

(* Generate manager functions *)
let gen_manager inst_prog =
  let templates = inst_prog.Syntax.templates in
  if List.length templates = 0 then ""
  else begin
    let template_names = List.map (fun t -> "'" ^ t.Syntax.template_id ^ "'") templates in
    let template_atoms = "[" ^ String.concat ", " template_names ^ "]" in
    
    String.concat "\n\n" [
      "%% Manager functions for dynamic party management";
      "";
      "%% ETS tables for party and instance registry";
      "ensure_manager_table() ->";
      "    case ets:info(mpfrp_parties) of";
      "        undefined -> ets:new(mpfrp_parties, [named_table, public, set]);";
      "        _ -> ok";
      "    end,";
      "    case ets:info(mpfrp_instances) of";
      "        undefined -> ets:new(mpfrp_instances, [named_table, public, set]);";
      "        _ -> ok";
      "    end.";
      "";
      "manager_register(PartyName, Pids, InstanceNames) ->";
      "    ensure_manager_table(),";
      "    ets:insert(mpfrp_parties, {PartyName, Pids}),";
      "    lists:foreach(fun(InstName) ->";
      "        ets:insert(mpfrp_instances, {InstName, PartyName})";
      "    end, InstanceNames),";
      "    io:format(\"[Manager] Party ~p registered with ~p PIDs and instances: ~p~n\", [PartyName, length(Pids), InstanceNames]).";
      "";
      "check_instance_names(NewNames) ->";
      "    ensure_manager_table(),";
      "    Duplicates = lists:filter(fun(Name) ->";
      "        case ets:lookup(mpfrp_instances, Name) of";
      "            [] -> false;";
      "            _ -> true";
      "        end";
      "    end, NewNames),";
      "    case Duplicates of";
      "        [] -> ok;";
      "        _ -> {error, {duplicate_instance_names, Duplicates}}";
      "    end.";
      "";
      "get_instances() ->";
      "    ensure_manager_table(),";
      "    ets:tab2list(mpfrp_instances).";
      "";
      "list_templates() ->";
      "    " ^ template_atoms ^ ".";
      "";
      "get_parties() ->";
      "    ensure_manager_table(),";
      "    ets:tab2list(mpfrp_parties).";
      "";
      "stop_party(PartyName) ->";
      "    ensure_manager_table(),";
      "    case ets:lookup(mpfrp_parties, PartyName) of";
      "        [{PartyName, Pids}] ->";
      "            %% Remove instance entries for this party";
      "            Instances = ets:match_object(mpfrp_instances, {'_', PartyName}),";
      "            lists:foreach(fun({InstName, _}) ->";
      "                ets:delete(mpfrp_instances, InstName)";
      "            end, Instances),";
      "            %% Stop all processes";
      "            lists:foreach(fun(Pid) ->";
      "                exit(Pid, shutdown)";
      "            end, Pids),";
      "            ets:delete(mpfrp_parties, PartyName),";
      "            io:format(\"[Manager] Party ~p stopped~n\", [PartyName]),";
      "            ok;";
      "        [] ->";
      "            {error, not_found}";
      "    end.";
    ]
  end

(* Main entry point *)
let gen_new_inst inst_prog module_map =
  let party_block = List.hd inst_prog.Syntax.parties in
  let module_name = party_block.Syntax.party_id in
  
  (* Generate template factory functions if templates exist *)
  let template_exports = gen_template_exports inst_prog in
  let template_factories = gen_template_factories inst_prog module_map in
  let manager_code = gen_manager inst_prog in
  
  String.concat "\n\n" [
    "-module(" ^ module_name ^ ").";
    gen_exports inst_prog module_map;
    template_exports;
    "";
    gen_helpers ();
    "";
    gen_trace_helper ();
    "";
    gen_compute_request_targets ();
    "";
    gen_run_party ();
    "";
    gen_all_actors inst_prog module_map;
    "";
    gen_start inst_prog module_map;
    "";
    gen_out inst_prog module_map;
    "";
    template_factories;
    "";
    manager_code;
  ]
