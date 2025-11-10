open Syntax
open Util
open Module

(* Generate Erlang code from new inst_program structure *)

(* === Import helper functions and types from Codegen === *)

(* Erlang identifier types *)
type erl_id = 
  | EIConst of string 
  | EIFun of string * int
  | EINative of string * int
  | EIVar of string
  | EISigVar of string
  | EILast of erl_id

exception AtLastError of string

(* Convert erl_id to Erlang string *)
let string_of_eid ?(raw=true) = function
  | EIConst(id) -> "const_" ^ id ^ "()"
  | EIFun(id, n) -> (if raw then "fun_" ^ id
                            else "fun ?MODULE:fun_" ^ id ^ "/" ^ string_of_int n)
  | EINative(id, n) -> (if raw then id
                            else "fun ?MODULE:" ^ id ^ "/" ^ string_of_int n)
  | EIVar(id) -> (match id with
    | "_" -> "_"
    | s -> "V" ^ s)
  | EISigVar(id) -> "S" ^ id
  | EILast(EISigVar(id)) -> "LS" ^ id
  | EILast(EIConst(id)) | EILast(EIFun(id, _)) | EILast(EINative(id, _)) | EILast(EIVar(id)) ->
    (raise (AtLastError(id ^ " is not node")))
  | EILast(EILast _) ->
    (raise (AtLastError("@last operator cannot be applied twice, make another delay node")))

(* Convert MPFRP expression to Erlang code *)
let erlang_of_expr env e = 
  let rec f env = function
    | EConst CUnit -> "void"
    | EConst (CBool b) -> string_of_bool b
    | EConst (CInt i)  -> string_of_int i
    | EConst (CFloat f)  -> Printf.sprintf "%f" f
    | EConst (CChar c) -> string_of_int (int_of_char c)
    | EId id -> string_of_eid ~raw:false (try_find id env)
    | EAnnot (id, ALast) -> string_of_eid (EILast(try_find id env))
    | EApp (id, es) ->
      string_of_eid (try_find id env) ^ "(" ^ (concat_map "," (f env) es) ^ ")"
    | EBin (BCons, hd, tl) ->
      "[" ^ f env hd ^ "|" ^ f env tl ^ "]"
    | EBin (op, e1, e2) -> "(" ^ f env e1 ^ (match op with
        | BMul -> " * "   | BDiv -> " / "        | BMod -> " rem "
        | BAdd -> " + "   | BSub -> " - "        | BShL -> " bsl "
        | BShR -> " bsr " | BLt -> " < "         | BLte -> " =< "
        | BGt -> " > "    | BGte -> " >= "       | BEq -> " == "
        | BNe -> " /= "   | BAnd -> " band "     | BXor -> " bxor "
        | BOr -> " bor "  | BLAnd -> " andalso " | BLOr -> " orelse " | _ -> "") ^ f env e2 ^ ")"
    | EUni (op, e) -> (match op with 
      | UNot -> "(not " ^ f env e ^ ")"
      | UNeg -> "-" ^ f env e
      | UInv -> "(bnot " ^ f env e ^ ")") 
    | ELet (binders, e) -> 
      let bid = List.map (fun (i,_,_) -> string_of_eid (EIVar(i))) binders in
      let bex = List.map (fun (_,e,_) -> f env e) binders in
      let newenv = List.fold_left (fun env (i,_,_) -> M.add i (EIVar(i)) env) env binders in
      "(case {" ^ String.concat "," bex ^ "} of " ^ 
      "{" ^ String.concat "," bid ^ "} -> " ^ f newenv e ^ " end)"
    | EIf(c, a, b) -> 
      "(case " ^ f env c ^ " of true -> " ^ f env a ^ "; false -> " ^ f env b ^ " end)"
    | EList es ->
      "[" ^ (concat_map "," (f env) es) ^ "]"
    | ETuple es ->
      "{" ^ (concat_map "," (f env) es) ^ "}"
    | EFun (args, e) ->
      let newenv = List.fold_left (fun env i -> M.add i (EIVar(i)) env) env args in
      "(" ^ concat_map "," (fun i -> string_of_eid (EIVar i)) args ^ ") -> " ^ f newenv e
    | ECase(m, list) -> 
      let rec pat = function
        | PWild -> ("_", [])
        | PNil  -> ("[]", [])
        | PConst c -> (f env (EConst c), [])
        | PVar v -> (string_of_eid (EIVar v), [v])
        | PTuple ts -> 
          let (s, vs) = List.split (List.map pat ts) in
          ("{" ^ (String.concat "," s) ^ "}", List.flatten vs) 
        | PCons (hd, tl) ->
          let (hdt, hdbinds) = pat hd in
          let (tlt, tlbinds) = pat tl in
          ("[" ^ hdt ^ "|" ^ tlt ^ "]", hdbinds @ tlbinds) in
      let body (p, e) =
        let (ps, pvs) = pat p in
        let newenv = List.fold_left (fun e i -> M.add i (EIVar i) e) env pvs in
        ps ^ " -> " ^ f newenv e
      in
      "(case " ^ f env m ^ " of " ^
        concat_map "; " body list ^
      " end)"
  in f env e

(* Create environment for expression evaluation *)
let create_env (module_info : Module.t) =
  let user_funs =
    List.map (fun (i,_) -> (i, EIConst i)) module_info.const
    @ List.map (function
        | (i, Module.InternFun EFun(args, _)) -> (i, EIFun (i, List.length args))
        | (i, Module.NativeFun (arg_t, _)) -> (i, EINative (i, List.length arg_t))
        | _ -> assert false) module_info.func
  in
  List.fold_left (fun m (i,e) -> M.add i e m) M.empty user_funs

(* Get initial value for a node *)
let get_init_value (module_info : Module.t) node_id =
  let rec find_node = function
    | [] -> None
    | (id, _, init_opt, _) :: rest ->
        if id = node_id then Some init_opt else find_node rest
  in
  match find_node module_info.node with
  | Some (Some init_expr) -> Some init_expr
  | _ -> None

(* Generate registered name for module actor *)
let module_actor_name party_id inst_id =
  String.lowercase_ascii party_id ^ "_" ^ inst_id

(* Generate registered name for node actor *)
let node_actor_name party_id inst_id node_id =
  String.lowercase_ascii party_id ^ "_" ^ inst_id ^ "_" ^ node_id

(* Generate registered name for input node actor *)
let input_node_actor_name party_id inst_id input_name =
  String.lowercase_ascii party_id ^ "_" ^ inst_id ^ "_" ^ input_name

(* Generate function name for module actor (module-based) *)
let module_actor_func_name module_name =
  String.lowercase_ascii module_name ^ "_module_actor"

(* Generate function name for input node actor (module-based) *)
let input_node_actor_func_name module_name input_name =
  String.lowercase_ascii module_name ^ "_input_" ^ input_name

(* Generate function name for computation node actor (module-based) *)
let node_actor_func_name module_name node_id =
  String.lowercase_ascii module_name ^ "_node_" ^ node_id

(* Generate registered name for party actor *)
let party_actor_name party_id =
  "party_" ^ String.lowercase_ascii party_id

(* Helper: Convert qualified_id to registered actor name *)
let qualified_id_to_actor_name party_id = function
  | SimpleId id -> module_actor_name party_id id
  | QualifiedId (other_party_id, inst_id) -> 
      module_actor_name other_party_id inst_id

(* Helper: Extract instance id from qualified_id *)
let qualified_id_to_instance_id = function
  | SimpleId id -> id
  | QualifiedId (_, inst_id) -> inst_id

(* Helper: Get downstream modules (dependencies) for an instance *)
let get_downstream_modules party inst_id =
  (* Find the newnode definition for this instance *)
  let rec find_in_instances = function
    | [] -> []
    | (outputs, _, inputs) :: rest ->
        if List.mem inst_id outputs then inputs
        else find_in_instances rest
  in
  let inputs = find_in_instances party.instances in
  List.map (qualified_id_to_actor_name party.party_id) inputs

(* Get downstream computation nodes for a specific node in an instance *)
(* Returns list of (party_id, instance_id, node_id, input_name) *)
let get_downstream_computation_nodes party module_map inst_id =
  (* まず、このインスタンスの下流インスタンスを取得 *)
  let rec find_in_instances = function
    | [] -> []
    | (outputs, module_name, inputs) :: rest ->
        if List.mem inst_id outputs then 
          (* inputsは[(module_name, instance_id)]のリスト *)
          List.map (fun qual_id ->
            let downstream_inst = qualified_id_to_instance_id qual_id in
            let downstream_module = 
              (* 下流インスタンスのモジュール名を検索 *)
              let rec find_module = function
                | [] -> failwith ("Cannot find module for instance " ^ downstream_inst)
                | (outs, mname, _) :: rest ->
                    if List.mem downstream_inst outs then mname
                    else find_module rest
              in
              find_module party.instances
            in
            (downstream_module, downstream_inst)
          ) inputs
        else find_in_instances rest
  in
  let downstream_instances = find_in_instances party.instances in
  
  (* 各下流インスタンスの計算ノード（入力ノードを含む）を取得 *)
  List.concat_map (fun (downstream_module, downstream_inst) ->
    let module_info = M.find downstream_module module_map in
    let input_names = module_info.Module.extern_input in
    
    (* 各入力ノードに対して、対応する計算ノードを返す *)
    (* originalでは、入力ノード自体が計算ノードなので、入力名＝ノード名 *)
    List.map (fun input_name ->
      (party.party_id, downstream_inst, input_name, input_name)
    ) input_names
  ) downstream_instances

(* Helper: Get all instances from a party block *)
let get_all_instances party =
  List.concat_map (fun (outputs, _, _) -> outputs) party.instances

(* Helper: Build instance to module mapping *)
let build_inst_module_map party =
  List.fold_left (fun map (outputs, module_name, _) ->
    List.fold_left (fun m inst_id ->
      M.add inst_id module_name m
    ) map outputs
  ) M.empty party.instances

(* Generate party actor code *)
let gen_party_actor party =
  let party_name = party_actor_name party.party_id in
  let leader_name = module_actor_name party.party_id party.leader in
  let party_atom = String.capitalize_ascii party.party_id in
  Printf.sprintf
    "%s(Ver) ->\n\
    \    timer:sleep(%d),\n\
    \    %s ! {'%s', Ver},\n\
    \    %s(Ver + 1).\n"
    party_name
    party.periodic_ms
    leader_name
    party_atom
    party_name

(* Generate module actor with Ver_buffer and In_buffer (mpfrp-original style) *)
(* Module-based: generates one function per module type *)
let gen_module_actor module_name module_info =
  let actor_name = module_actor_func_name module_name in
  let input_names = module_info.Module.extern_input in
  
  (* Get all computation nodes (excluding input nodes) *)
  let all_nodes = module_info.Module.node in
  let computation_nodes = List.filter (fun (node_id, _, _, _) ->
    not (List.mem node_id input_names)
  ) all_nodes in
  
  (* Generate request sending code for all computation nodes *)
  let request_nodes_code = 
    if computation_nodes = [] then ""
    else
      let node_requests = List.map (fun (node_id, _, _, _) ->
        Printf.sprintf "                                NodeActor_%s = list_to_atom(string:to_lower(atom_to_list(IncomingParty)) ++ \"_\" ++ atom_to_list(InstanceId) ++ \"_%s\"),\n\
        \                                NodeActor_%s ! {request, {IncomingParty, Ver}}" node_id node_id node_id
      ) computation_nodes in
      String.concat ",\n" node_requests ^ ",\n"
  in
  
  (* Generate clauses for Ver_buffer processing with lists:foldl *)
  (* When Ver == P_verT, forward sync_pulse to downstream and request nodes *)
  let ver_foldl_body =
    let downstream_forwards = 
      "                lists:foreach(fun(Module) -> Module ! {IncomingParty, Ver} end, DownstreamModules)," in
    Printf.sprintf
      "        {NBuffer, P_ver0} = lists:foldl(fun (Version, {Buf, P_verT}) ->\n\
      \                case Version of\n\
      \                        {IncomingParty, Ver} when Ver > P_verT ->\n\
      \                                {[{IncomingParty, Ver} | Buf], P_verT};\n\
      \                        {IncomingParty, Ver} when Ver =:= P_verT ->\n\
      %s\n\
      %s\
      \                                {Buf, P_verT + 1};\n\
      \                        {IncomingParty, Ver} when Ver < P_verT ->\n\
      \                                {Buf, P_verT};\n\
      \                        _ ->\n\
      \                                {Buf, P_verT}\n\
      \                end\n\
      \        end, {[], P_ver}, Ver_buffer)"
      downstream_forwards
      request_nodes_code
  in
  
  (* Generate clauses for In_buffer processing - one clause per input *)
  let in_foldl_clauses = List.map (fun input_name ->
    let input_actor = input_node_actor_func_name module_name input_name in
    Printf.sprintf
      "                        {{IncomingParty, Ver}, %s, Val} when Ver > P_verT ->\n\
      \                                {[Msg | Buf], P_verT};\n\
      \                        {{IncomingParty, Ver}, %s, Val} when Ver =:= P_verT ->\n\
      \                                %s ! {{IncomingParty, Ver}, Val},\n\
      \                                lists:foreach(fun(Module) -> Module ! {IncomingParty, Ver} end, DownstreamModules),\n\
      \                                {Buf, P_verT + 1};\n\
      \                        {{IncomingParty, Ver}, %s, Val} when Ver < P_verT ->\n\
      \                                %s ! {{IncomingParty, Ver}, Val},\n\
      \                                {Buf, P_verT}"
      input_name
      input_name
      input_actor
      input_name
      input_actor
  ) input_names in
  
  let in_foldl_body = 
    Printf.sprintf
      "        {NInBuffer, P_verN} = lists:foldl(fun (Msg, {Buf, P_verT}) ->\n\
      \                case Msg of\n\
      %s%s\n\
      \                        _ ->\n\
      \                                {Buf, P_verT}\n\
      \                end\n\
      \        end, {[], P_ver0}, In_buffer)"
      (String.concat ";\n" in_foldl_clauses)
      (if in_foldl_clauses = [] then "" else ";")
  in
  
  (* Generate complete module actor *)
  actor_name ^ "(Ver_buffer, In_buffer, Party, P_ver, InstanceId, DownstreamModules) ->\n" ^
  ver_foldl_body ^ ",\n" ^
  in_foldl_body ^ ",\n" ^
  "        receive\n" ^
  "                {_, _} = Ver_msg ->\n" ^
  "                        " ^ actor_name ^ "(lists:reverse([Ver_msg | NBuffer]), NInBuffer, Party, P_verN, InstanceId, DownstreamModules);\n" ^
  "                {_, _, _} = In_msg ->\n" ^
  "                        " ^ actor_name ^ "(NBuffer, lists:reverse([In_msg | NInBuffer]), Party, P_verN, InstanceId, DownstreamModules)\n" ^
  "        end.\n"

(* Generate input node actor - receives data and tags it with input name *)
(* Module-based: generates one function per module type *)
let gen_input_node_actor module_name input_name =
  let actor_name = input_node_actor_func_name module_name input_name in
  let target_node = node_actor_func_name module_name "data" in  (* Assumes computation node is named 'data' *)
  actor_name ^ "() ->\n" ^
  "        receive\n" ^
  "                {{Party, Ver}, Val} ->\n" ^
  "                        " ^ target_node ^ " ! {{Party, Ver}, " ^ input_name ^ ", Val};\n" ^
  "                _ ->\n" ^
  "                        void\n" ^
  "        end,\n" ^
  "        " ^ actor_name ^ "().\n"

(* Generate node actor for one node with computation logic and buffering *)
(* Module-based: generates one function per module type *)
let gen_node_actor module_name node_id (module_info : Module.t) =
  let actor_name = node_actor_func_name module_name node_id in
  
  (* Find node definition *)
  let node_def = List.find (fun (id, _, _, _) -> id = node_id) module_info.node in
  let (_, _, _, expr) = node_def in
  
  (* Create environment for expression evaluation *)
  let env = create_env module_info in
  
  (* Get list of input names (extern_input) for this module *)
  let input_names = module_info.extern_input in
  
  (* Build pattern match for checking if all inputs are present *)
  let input_pattern = 
    if List.length input_names = 0 then
      "_"
    else
      "#{" ^ String.concat ", " (List.map (fun name -> 
        name ^ " := S" ^ name
      ) input_names) ^ "}"
  in
  
  (* Build environment for expression evaluation with signal variables *)
  (* This maps input names (e.g., "client1") to EISigVar which renders as "Sclient1" *)
  let input_env = List.fold_left (fun e id ->
    M.add id (EISigVar id) e
  ) env input_names in
  
  (* Add module's own nodes to environment as signal variables *)
  let input_env = List.fold_left (fun e (id, _, _, _) ->
    M.add id (EISigVar id) e
  ) input_env module_info.node in
  
  (* Generate the computation expression *)
  let computation_code_raw = erlang_of_expr input_env expr in
  
  (* Replace LSnode_id with State for self-referential @last *)
  let ls_pattern = "LS" ^ node_id in
  let rec replace_in_string str pattern replacement =
    let len = String.length pattern in
    let strlen = String.length str in
    let rec search pos =
      if pos > strlen - len then str
      else if String.sub str pos len = pattern then
        String.sub str 0 pos ^ replacement ^ String.sub str (pos + len) (strlen - pos - len)
      else search (pos + 1)
    in search 0
  in
  let computation_code = replace_in_string computation_code_raw ls_pattern "State" in
  
  (* Build receive clauses based on whether there are inputs *)
  let receive_clauses = 
    if List.length input_names = 0 then
      (* No inputs - compute immediately on request *)
      "        receive\n" ^
      "                {request, {Party, Ver}} ->\n" ^
      "                        VersionKey = {Party, Ver},\n" ^
      "                        ProcessedValue = " ^ computation_code ^ ",\n" ^
      "                        io:format(\"NEW_DATA[~s]=~p~n\", [\"" ^ node_id ^ "\", ProcessedValue]),\n" ^
      "                        lists:foreach(fun(DownstreamModule) ->\n" ^
      "                                DownstreamModule ! {VersionKey, " ^ node_id ^ ", ProcessedValue}\n" ^
      "                        end, DownstreamModules),\n" ^
      "                        " ^ actor_name ^ "(Buffer, ProcessedValue, NextVer, DownstreamModules)\n" ^
      "        end.\n"
    else
      (* Has inputs - wait for all inputs before computing, or compute on request if no dependencies *)
      "        receive\n" ^
      "                {request, {Party, Ver}} ->\n" ^
      "                        VersionKey = {Party, Ver},\n" ^
      "                        ProcessedValue = " ^ computation_code ^ ",\n" ^
      "                        io:format(\"NEW_DATA[~s]=~p~n\", [\"" ^ node_id ^ "\", ProcessedValue]),\n" ^
      "                        lists:foreach(fun(DownstreamModule) ->\n" ^
      "                                DownstreamModule ! {VersionKey, " ^ node_id ^ ", ProcessedValue}\n" ^
      "                        end, DownstreamModules),\n" ^
      "                        " ^ actor_name ^ "(Buffer, ProcessedValue, NextVer, DownstreamModules);\n" ^
      "                {{Party, Ver}, InputName, Val} ->\n" ^
      "                        %% Update buffer with received input\n" ^
      "                        VersionKey = {Party, Ver},\n" ^
      "                        NewBuffer = maps:update_with(VersionKey,\n" ^
      "                                fun(InputMap) -> maps:put(InputName, Val, InputMap) end,\n" ^
      "                                #{InputName => Val},\n" ^
      "                                Buffer),\n" ^
      "                        %% Check if all inputs are ready for this version\n" ^
      "                        case maps:find(VersionKey, NewBuffer) of\n" ^
      "                                {ok, " ^ input_pattern ^ "} ->\n" ^
      "                                        %% All inputs ready, compute the value\n" ^
      "                                        ProcessedValue = " ^ computation_code ^ ",\n" ^
      "                                        io:format(\"NEW_DATA[~s]=~p~n\", [\"" ^ node_id ^ "\", ProcessedValue]),\n" ^
      "                                        %% Send result to downstream modules\n" ^
      "                                        lists:foreach(fun(DownstreamModule) ->\n" ^
      "                                                DownstreamModule ! {VersionKey, " ^ node_id ^ ", ProcessedValue}\n" ^
      "                                        end, DownstreamModules),\n" ^
      "                                        %% Remove processed version from buffer and update state\n" ^
      "                                        CleanBuffer = maps:remove(VersionKey, NewBuffer),\n" ^
      "                                        " ^ actor_name ^ "(CleanBuffer, ProcessedValue, NextVer, DownstreamModules);\n" ^
      "                                _ ->\n" ^
      "                                        %% Not all inputs ready yet, wait for more\n" ^
      "                                        " ^ actor_name ^ "(NewBuffer, State, NextVer, DownstreamModules)\n" ^
      "                        end\n" ^
      "        end.\n"
  in
  
  (* Build the function code with buffering logic *)
  actor_name ^ "(Buffer, State, NextVer, DownstreamModules) ->\n" ^
  receive_clauses

(* Generate spawn and register code for one instance *)
let gen_spawn_instance party module_map inst_id module_name =
  let module_info : Module.t = try M.find module_name module_map with Not_found -> 
    failwith ("Module " ^ module_name ^ " not found") 
  in
  let party_id = party.party_id in
  let instance_node_actor = module_actor_name party_id inst_id in
  let module_func = module_actor_func_name module_name in
  
  (* Get downstream modules (dependencies) for this instance *)
  let downstream = get_downstream_modules party inst_id in
  let downstream_str = "[" ^ String.concat ", " downstream ^ "]" in
  
  (* Spawn module actor with Ver_buffer, In_buffer, Party, P_ver, InstanceId, DownstreamModules *)
  let spawn_module = 
    Printf.sprintf "    register(%s, spawn(fun() -> %s([], [], %s, 0, %s, %s) end))"
      instance_node_actor module_func (String.lowercase_ascii party_id) inst_id downstream_str
  in
  
  (* Spawn input node actors for each extern_input *)
  let input_names = module_info.extern_input in
  let spawn_input_nodes = List.map (fun input_name ->
    let input_actor_registered = input_node_actor_name party_id inst_id input_name in
    let input_func = input_node_actor_func_name module_name input_name in
    Printf.sprintf "    register(%s, spawn(fun() -> %s() end))"
      input_actor_registered input_func
  ) input_names in
  
  (* Spawn computation node actors for each non-input node in the module *)
  let nodes = module_info.node in
  let computation_nodes = List.filter (fun (node_id, _, _, _) ->
    not (List.mem node_id input_names)
  ) nodes in
  let spawn_nodes = List.map (fun (node_id, _, init_opt, _) ->
    let node_actor_registered = node_actor_name party_id inst_id node_id in
    let node_func = node_actor_func_name module_name node_id in
    let init_value = match init_opt with
      | Some _ -> "undefined"  (* TODO: Evaluate init expression properly *)
      | None -> "undefined"
    in
    (* Node actors now have Buffer, State, NextVer, DownstreamModules parameters *)
    Printf.sprintf "    register(%s, spawn(fun() -> %s(#{}, %s, 0, []) end))"
      node_actor_registered node_func init_value
  ) computation_nodes in
  
  String.concat ",\n" ([spawn_module] @ spawn_input_nodes @ spawn_nodes)

(* Generate spawn code for all instances in a party *)
let gen_spawn_party party module_map =
  let party_id = party.party_id in
  
  (* First spawn party actor *)
  let party_spawn = 
    Printf.sprintf "    register(%s, spawn(fun() -> %s(0) end))"
      (party_actor_name party_id)
      (party_actor_name party_id)
  in
  
  (* Then spawn all instances *)
  let instance_spawns = List.concat_map (fun (outputs, module_name, _) ->
    List.map (fun inst_id ->
      gen_spawn_instance party module_map inst_id module_name
    ) outputs
  ) party.instances in
  
  String.concat ",\n" ([party_spawn] @ instance_spawns)

(* Generate all actor definitions *)
(* Module-based: generates one set of functions per module type *)
let gen_all_actors parties module_map =
  let party_actors = List.map gen_party_actor parties in
  
  (* Generate module actors - one per module type, not per instance *)
  let module_actors = M.fold (fun module_name module_info acc ->
    (gen_module_actor module_name module_info) :: acc
  ) module_map [] in
  
  (* Generate input node actors - one per module type and input name *)
  let input_node_actors = M.fold (fun module_name module_info acc ->
    let input_names = module_info.extern_input in
    List.map (fun input_name ->
      gen_input_node_actor module_name input_name
    ) input_names @ acc
  ) module_map [] in
  
  (* Generate computation node actors - one per module type and node *)
  let node_actors = M.fold (fun module_name module_info acc ->
    let nodes = module_info.node in
    let input_names = module_info.extern_input in
    (* Filter out input nodes, only generate computation nodes *)
    let computation_nodes = List.filter (fun (node_id, _, _, _) ->
      not (List.mem node_id input_names)
    ) nodes in
    List.map (fun (node_id, _, _, _) ->
      gen_node_actor module_name node_id module_info
    ) computation_nodes @ acc
  ) module_map [] in
  
  String.concat "\n" (party_actors @ module_actors @ input_node_actors @ node_actors)

(* Generate start function *)
let gen_start parties module_map =
  let spawn_code = String.concat ",\n" (List.map (fun party ->
    gen_spawn_party party module_map
  ) parties) in
  
  Printf.sprintf "start() ->\n%s.\n" spawn_code

(* Generate exports *)
let gen_exports parties module_map =
  let party_exports = List.map (fun party ->
    Printf.sprintf "-export([party_%s/1])." (String.lowercase_ascii party.party_id)
  ) parties in
  
  (* Module actor exports - arity /6 (Ver_buffer, In_buffer, Party, P_ver, InstanceId, DownstreamModules) *)
  let module_exports = M.fold (fun module_name _ acc ->
    let func_name = module_actor_func_name module_name in
    Printf.sprintf "-export([%s/6])." func_name :: acc
  ) module_map [] in
  
  (* Export input node actors (arity 0 for loop function) *)
  let input_node_exports = M.fold (fun module_name module_info acc ->
    let input_names = module_info.extern_input in
    List.fold_left (fun acc2 input_name ->
      let func_name = input_node_actor_func_name module_name input_name in
      Printf.sprintf "-export([%s/0])." func_name :: acc2
    ) acc input_names
  ) module_map [] in
  
  (* Export computation node actors (arity 4: Buffer, State, NextVer, DownstreamModules) *)
  let node_exports = M.fold (fun module_name module_info acc ->
    let input_names = module_info.extern_input in
    (* Filter out input nodes, only export computation nodes *)
    let computation_nodes = List.filter (fun (node_id, _, _, _) ->
      not (List.mem node_id input_names)
    ) module_info.node in
    List.fold_left (fun acc2 (node_id, _, _, _) ->
      let func_name = node_actor_func_name module_name node_id in
      Printf.sprintf "-export([%s/4])." func_name :: acc2
    ) acc computation_nodes
  ) module_map [] in
  
  "-export([start/0]).\n" ^
  String.concat "\n" (party_exports @ module_exports @ input_node_exports @ node_exports) ^ "\n"

(* Main code generation function *)
let gen_new_inst inst_prog module_map =
  let module_header = "-module(main).\n" in
  let exports = gen_exports inst_prog.parties module_map in
  let actor_defs = gen_all_actors inst_prog.parties module_map in
  let start_func = gen_start inst_prog.parties module_map in
  
  module_header ^ exports ^ "\n" ^ actor_defs ^ "\n" ^ start_func
