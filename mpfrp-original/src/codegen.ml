open Syntax
open Util
open Module
open Dependency

exception AtLastError of string
exception NodeLoop of (string * string list) list
exception PartyLoop of string list list

type erl_id = 
  | EIConst of string 
  | EIFun of string * int
  | EINative of string * int
  | EIVar of string
  | EISigVar of string
  | EILast of erl_id

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

let send target e =
  target ^ " ! " ^ e

let erlang_of_expr env e = 
  let rec f env = function
    | EConst CUnit -> "void"
    | EConst (CBool b) -> string_of_bool b
    | EConst (CInt i)  -> string_of_int i
    | EConst (CFloat f)  -> Printf.sprintf "%f" f
    | EConst (CChar c) -> string_of_int (int_of_char c)
    | EId id -> string_of_eid ~raw:false (try_find id env)
    | EAnnot (id, ALast) -> string_of_eid (EILast(try_find id env))
    | EApp (id, es) -> (* workaround *)
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

let init_values x ti =
  let rec of_type = let open Type in function
    | TUnit -> EConst(CUnit)
    | TBool -> EConst(CBool(false))
    | TInt  -> EConst(CInt(0))
    | TFloat -> EConst(CFloat(0.0))
    | TChar -> EConst(CBool(false))
    | TTuple ts -> ETuple(List.map of_type ts)
    | TList t -> EList([])
    | _ -> assert false in
  let node_init = List.fold_left (fun m -> function
    | (id, _, Some(init), _) -> M.add id init m
    | (id, _, None, _) -> M.add id (of_type (Typeinfo.find id ti)) m) M.empty x.node in
  List.fold_left (fun m id -> M.add id (of_type (Typeinfo.find id ti)) m) node_init x.source

(* create env per module *)
let env_map module_map =
  let create_env module_info =
    let user_funs =
      List.map (fun (i,_) -> (i, EIConst i)) module_info.const
      @ List.map (function
          | (i, InternFun EFun(args, _)) -> (i, EIFun (i, List.length args))
          | (i, NativeFun (arg_t, _)) -> (i, EINative (i, List.length arg_t))
          | _ -> assert false) module_info.func
    in
      List.fold_left (fun m (i,e) -> M.add i e m) M.empty user_funs
  in
    list2map (List.map (fun (module_id, module_info) -> (module_id, create_env module_info)) (M.bindings module_map))

(* type per module *)
let ti_map module_map =
  List.fold_left
    (fun m (module_id, module_info) ->
      M.add module_id (Typing.type_module module_info) m)
    M.empty
    (M.bindings module_map)

(* map graph per module *)
let node_dep_map module_map =
  List.fold_left
    (fun m (module_id, module_info) ->
      let node_dep = Dependency.get_graph module_info in
        M.add module_id node_dep m)
    M.empty
    (M.bindings module_map)

let init_value_map module_map =
  List.fold_left
    (fun m (module_id, module_info) ->
      M.add
        module_id
        (init_values module_info (try_find module_id (ti_map module_map)))
        m)
    M.empty
    (M.bindings module_map)

let in_node inst_id id graph mod_dep_map =
  let inst_info = try_find inst_id mod_dep_map in
  let inst_p = try_find (try_find id inst_info.module_info.id2party) inst_info.inst_party_map in
  let outputs = (try_find id graph).output in
  let uncapitalize_inst_id = String.uncapitalize_ascii inst_id in
    uncapitalize_inst_id ^ "_" ^ id ^ "() ->\n"
    ^ indent 1 "receive\n"
    ^ indent 2 "{{" ^ inst_p ^ ", Ver}, Val} ->\n"
    ^ (if is_empty outputs then indent 3 "void;\n"
      else
        concat_map
          ",\n"
          (fun (dstid, _) ->
            indent 3 (send (uncapitalize_inst_id ^ "_" ^ dstid) ("{{" ^ inst_p ^ ", Ver}, " ^ id ^ ", Val}")))
          outputs
    ^ ";\n")
    ^ indent 2 "_ ->\n"
    ^ indent 3 "void\n"
    ^ indent 1 "end,\n"
    ^ indent 1 (uncapitalize_inst_id ^ "_" ^ id ^ "().")

let def_key_input_node graph (inst_id, inst_info) (id, p, init, expr) =
  let node_info = try_find id graph in
  let prefix_module = String.uncapitalize_ascii inst_id in
  let send_code n =
    let output_lst = extract_node node_info.output in
      if is_empty output_lst then indent n "void,\n"
      else
        concat_map
          ",\n"
          (indent n)
          (List.map
            (fun i -> send (prefix_module ^ "_" ^ i) ("{Version, " ^ id ^ ", Value}"))
          output_lst)
        ^ ",\n"
  in
  prefix_module ^ "_" ^ id ^ "(Value, NextVer0, ReqBuffer0) ->\n"
  ^ indent 1 "Sorted_req_buf = lists:sort(ReqBuffer0),\n"
  ^ indent 1 "{NNextVer, NReqBuffer} = lists:foldl(fun ({_, Ver} = Version, {NextVer, ReqBuffer}) -> \n"
  ^ indent 2 "case Ver =:= NextVer of\n"
  ^ indent 3 "true ->\n"
  ^ send_code 4
  ^ indent 4 "{NextVer + 1, ReqBuffer};\n"
  ^ indent 3 "false ->\n"
  ^ indent 4 "{NextVer, [Version|ReqBuffer]}\n"
  ^ indent 2 "end\n"
  ^ indent 1 "end, {NextVer0, []}, Sorted_req_buf),\n"
  ^ indent 1 "receive\n"
  ^ indent 2 "{request, Version} ->\n"
  ^ indent 3 (prefix_module ^ "_" ^ id ^ "(Value, NNextVer, lists:reverse([Version|NReqBuffer]));\n")
  ^ indent 2 "NewValue ->\n"
  ^ indent 3 (prefix_module ^ "_" ^ id ^ "(NewValue, NNextVer, NReqBuffer)\n")
  ^ indent 1 "end."

(* assign value in map*)
let bind (cs, ls) = "#{" ^ String.concat ", " (
  List.map (fun id -> id ^ " := " ^ string_of_eid (EISigVar id)) cs
  @ List.map (fun id -> "{last, " ^ id ^ "} := " ^ string_of_eid (EILast (EISigVar id))) ls)
  ^ "}"

let same_party_code n dep (inst_id, inst_info) env id expr output_lst =
  indent n "Curr = " ^ erlang_of_expr env expr ^ ",\n"
  ^ indent n "io:format(\"ORIGINAL_DATA[~s_~s]=~p~n\", [\"" ^ String.uncapitalize_ascii inst_id ^ "\", \"" ^ id ^ "\", Curr]),\n"
  ^ (if dep.is_output then
      indent n ("out(" ^ String.uncapitalize_ascii inst_id ^ "_" ^ id ^ ", Curr),\n")
      ^ let target_module_lst = try_find (try_find id inst_info.out_nns) inst_info.inst_in_map in
          if is_empty target_module_lst then ""
          else
            indent n "lists:foreach(fun (V) -> \n"
            ^ concat_map
                ",\n"
                (fun (target_module, prog_in) ->
                  indent (n + 1) (send (String.uncapitalize_ascii target_module) ("{V, " ^ prog_in ^ ", Curr}")))
                target_module_lst
            ^ "\n"
            ^ indent n "end, [Version|Deferred]),\n"
    else "")
  ^ indent n "lists:foreach(fun (V) -> \n"
  ^ (if is_empty output_lst then indent (n + 1) "void\n"
    else
      concat_map
        ",\n"
        (indent (n + 1))
        (List.map
          (fun i -> send (String.uncapitalize_ascii inst_id ^ "_" ^ i) ("{V, " ^ id ^ ", Curr}"))
          output_lst)
    ^ "\n")
  ^ indent n "end, [Version|Deferred])"
  ^ ",\n"

let other_party_code n dep (inst_id, inst_info) env id expr output_lst =
  indent n "case Deferred of\n"
  ^ indent (n + 1) "[] ->\n"
  ^ indent (n + 2) "void;\n"
  ^ indent (n + 1) "_ ->\n"
  ^ indent (n + 2) "Curr = " ^ erlang_of_expr env expr ^ ",\n"
  ^ indent (n + 2) "io:format(\"ORIGINAL_DATA[~s_~s]=~p~n\", [\"" ^ String.uncapitalize_ascii inst_id ^ "\", \"" ^ id ^ "\", Curr]),\n"
  ^ (if dep.is_output then
      indent (n + 2) ("out(" ^ String.uncapitalize_ascii inst_id ^ "_" ^ id ^ ", Curr),\n")
      ^ let target_module_lst = try_find (try_find id inst_info.out_nns) inst_info.inst_in_map in
        if is_empty target_module_lst then ""
        else
          (indent (n + 2) "lists:foreach(fun (V) -> \n"
          ^ concat_map
              ",\n"
              (fun (target_module, prog_in) ->
                indent (n + 3) (send (String.uncapitalize_ascii target_module) ("{V, " ^ prog_in ^ ", Curr}")))
              target_module_lst
          ^ "\n"
          ^ indent (n + 2) "end, Deferred),\n")
    else "")
  ^ indent (n + 2) "lists:foreach(fun (V) -> \n"
  ^ (if is_empty output_lst then indent (n + 3) "void\n"
    else concat_map
          ",\n"
          (indent (n + 3))
          (List.map
            (fun i -> send (String.uncapitalize_ascii inst_id ^ "_" ^ i) ("{V, " ^ id ^ ", Curr}"))
            output_lst)
  ^ "\n")
  ^ indent (n + 2) "end, Deferred)\n"
  ^ indent n "end,\n"

(* generate code sending msg *)
let send_code n dep (inst_id, inst_info) env id expr is_self_party =
  let output_lst = extract_node dep.output in
    if is_self_party then same_party_code n dep (inst_id, inst_info) env id expr output_lst
    else other_party_code n dep (inst_id, inst_info) env id expr output_lst

let gen_request_rel_code inst_party dep (inst_id, inst_info) env id expr =
  indent 3 "_ -> {Buffer, NextVer, Processed, Deferred}\n"
  ^ indent 2 "end\n"
  ^ indent 1 "end, {Buffer0, NextVer0, Processed0, Deferred0}, HL),\n"
  ^ indent 1 "{NNextVer, NProcessed, NReqBuffer, NDeferred} = lists:foldl(fun (E, {NextVer, Processed, ReqBuffer, Deferred}) -> \n"
  ^ indent 2 "case E of\n"
  ^ indent 3 "{" ^ inst_party ^ ", Ver} = Version ->\n"
  ^ indent 4 ("io:format(\"[STATE_REQ] Actor: ~p, Ver: ~p, NextVer: ~p, Processed: ~p~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver, maps:get(" ^ inst_party ^ ", NextVer), Processed]),\n")
  ^ indent 4 "case maps:get(" ^ inst_party ^ ", NextVer) =:= Ver of\n"
  ^ indent 5 "true ->\n"
  ^ indent 6 "case Processed of\n"
  ^ indent 7 (bind (extract_node dep.input_current, extract_node dep.input_last) ^ " ->\n")
  ^ indent 8 ("io:format(\"[STATE_REQ] Actor: ~p, Ver: ~p, Inputs_Ready: true, Computing~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]),\n")
  ^ send_code 8 dep (inst_id, inst_info) env id expr true
  ^ indent 8 ("{maps:update(" ^ inst_party ^ ", Ver + 1, NextVer), Processed, ReqBuffer, []};\n")
  ^ indent 7 "_ ->\n"
  ^ indent 8 ("io:format(\"[STATE_REQ] Actor: ~p, Ver: ~p, Inputs_Ready: false, Deferring~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]),\n")
  ^ indent 8 ("{maps:update(" ^ inst_party ^ ", Ver + 1, NextVer), Processed, ReqBuffer, [Version|Deferred]}\n")
  ^ indent 6 "end;\n"
  ^ indent 5 "false ->\n"
  ^ indent 6 ("io:format(\"[STATE_REQ] Actor: ~p, Ver: ~p, Match: false, Buffering~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]),\n")
  ^ indent 6 ("{NextVer, Processed, [Version|ReqBuffer], Deferred}\n")
  ^ indent 4 "end\n"
  ^ indent 2 "end\n"
  ^ indent 1 "end, {NextVerT, ProcessedT, [], DeferredT}, Sorted_req_buf),\n"

let def_node graph (inst_id, inst_info) env (id, p, init, expr) =
  let dep = try_find id graph
  and node_party = try_find p inst_info.inst_party_map in
  if erlang_of_expr env expr = "key_input()" then def_key_input_node graph (inst_id, inst_info) (id, p, init, expr)
  else
    String.uncapitalize_ascii inst_id ^ "_" ^ id ^ "(Buffer0, NextVer0, Processed0, ReqBuffer0, Deferred0) ->\n"
    ^ indent 1 "HL = lists:sort(?SORTBuffer, maps:to_list(Buffer0)),\n"
    ^ indent 1 "Sorted_req_buf = lists:sort(?SORTVerBuffer, ReqBuffer0),\n"
    ^ indent 1 "{NBuffer, NextVerT, ProcessedT, DeferredT} = lists:foldl(fun (E, {Buffer, NextVer, Processed, Deferred}) -> \n"
    ^ indent 2 "case E of\n"
    ^ let pcl_lst =
        let init_map = map_nil (List.map snd (dep.input_current @ dep.input_last)) in
          let collect_node inputs =
                List.fold_left
                  (fun m (id, party) ->
                    let old_lst = try_find party m in
                      M.add party (id :: old_lst) m)
                  init_map
                  inputs
          in
            M.bindings
              (M.merge
                (fun k currents lasts ->
                  match currents, lasts with
                    Some lst_c, Some lst_l -> Some (lst_c, lst_l)
                    | _ -> None)
                (collect_node dep.input_current)
                (collect_node dep.input_last))
      in
        concat_map
          ""
          (fun (party, (currents, lasts)) ->
            let inst_party = try_find party inst_info.inst_party_map in
            let other_vars =
              let sub l1 l2 = List.filter (fun x -> not (List.mem x l2)) l1 in
                (sub (extract_node dep.input_current) currents, sub (extract_node dep.input_last) lasts)
              in
                indent 3 "{ {" ^ inst_party ^ ", Ver} = Version, " ^ bind (currents, lasts) ^ " = Map} ->\n"
                ^ indent 4 ("io:format(\"[STATE_BUF] Actor: ~p, Ver: ~p, NextVer: ~p, Map: ~p~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver, maps:get(" ^ inst_party ^ ", NextVer), Map]),\n")
                ^ match other_vars with
                  | ([],[]) ->
                    indent 4 "case maps:get(" ^ inst_party ^ ", NextVer) =:= Ver of\n"
                    ^ indent 5 "true ->\n"
                    ^ indent 6 ("io:format(\"[STATE_BUF] Actor: ~p, Ver: ~p, Match: true, Computing~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]),\n")
                    ^ send_code 6 dep (inst_id, inst_info) env id expr (node_party = inst_party)
                    ^ indent 6 "{maps:remove(Version, Buffer), maps:update(" ^ inst_party ^ ", Ver + 1, NextVer), maps:merge(Processed, Map), []};\n"
                    ^ indent 5 "false ->\n"
                    ^ indent 6 ("io:format(\"[STATE_BUF] Actor: ~p, Ver: ~p, Match: false, Skipping~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]),\n")
                    ^ indent 6 "{Buffer, NextVer, Processed, Deferred}\n"
                    ^ indent 4 "end;\n"
                  | others ->
                    indent 4 "case maps:get(" ^  inst_party ^ ", NextVer) =:= Ver of\n"
                    ^ indent 5 "true ->\n"
                    ^ indent 6 "case Processed of\n"
                    ^ indent 7 (bind others) ^ " -> \n"
                    ^ indent 8 ("io:format(\"[STATE_BUF] Actor: ~p, Ver: ~p, Match: true, Inputs_Ready: true, Computing~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]),\n")
                    ^ send_code 8 dep (inst_id, inst_info) env id expr (node_party = inst_party)
                    ^ indent 8 "{maps:remove(Version, Buffer), maps:update(" ^ inst_party ^ ", Ver + 1, NextVer), maps:merge(Processed, Map), []};\n"
                    ^ indent 7 ("_ -> io:format(\"[STATE_BUF] Actor: ~p, Ver: ~p, Match: true, Inputs_Ready: false, Deferring~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]), {maps:remove(Version, Buffer), maps:update(" ^ inst_party ^ ", Ver + 1, NextVer), maps:merge(Processed, Map), [Version|Deferred]}\n")
                    ^ indent 6 "end;\n"
                    ^ indent 5 "false ->\n"
                    ^ indent 6 ("io:format(\"[STATE_BUF] Actor: ~p, Ver: ~p, Match: false, Skipping~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Ver]),\n")
                    ^ indent 6 "{Buffer, NextVer, Processed, Deferred}\n"
                    ^ indent 4 "end;\n")
          pcl_lst
    ^ (gen_request_rel_code node_party dep (inst_id, inst_info) env id expr)
    ^ indent 1 ("io:format(\"[STATE_END] Actor: ~p, NextVer: ~p, BufferKeys: ~p, Processed: ~p, ReqBuf: ~p, Deferred: ~p~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", NNextVer, maps:keys(NBuffer), NProcessed, NReqBuffer, NDeferred]),\n")
    ^ indent 1 "receive\n"
    ^ indent 2 "{{_, _}, _, _} = Received ->\n"
    ^ indent 3 ("io:format(\"[RECV] Actor: ~p, Msg: ~p~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", Received]),\n")
    ^ indent
        3
        ((String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ "(buffer_update([" ^ String.concat ", " (extract_node dep.input_current) ^"], [" ^ String.concat ", " (extract_node dep.input_last)
        ^ "], Received, NBuffer), NNextVer, NProcessed, NReqBuffer, NDeferred);\n")
    ^ indent 2 "{request, Ver} ->\n"
    ^ indent 3 ("io:format(\"[RECV] Actor: ~p, Msg: ~p~n\", [" ^ (String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ ", {request, Ver}]),\n")
    ^ indent 3 ((String.uncapitalize_ascii inst_id) ^ "_" ^ id ^ "(NBuffer, NNextVer, NProcessed, lists:reverse([Ver|NReqBuffer]), NDeferred)\n")
    ^ indent 1 "end."

let def_const env (id, e) =
  (string_of_eid (EIConst id)) ^ " -> " ^ erlang_of_expr env e ^ "."

let def_fun env (id, body) = match body with
  | InternFun (EFun(a, _) as expr) ->
    (string_of_eid (EIFun (id, -1))) ^ erlang_of_expr env expr ^ "."
  | NativeFun _ -> ""
  | _ -> assert false

let lib_funcs = String.concat "\n" [
  (* sort function *)
  "-define(SORTBuffer, fun ({{K1, V1}, _}, {{K2, V2}, _}) -> if";
  indent 1 "V1 == V2 -> K1 < K2;";
  indent 1 "true -> V1 < V2";
  "end end).";
  "-define(SORTVerBuffer, fun ({P1, V1}, {P2, V2}) -> if";
  indent 1 "P1 == P2 -> V1 < V2;";
  indent 1 "true -> P1 < P2";
  "end end).";
  "-define(SORTInBuffer, fun ({{P1, V1}, _, _}, {{P2, V2}, _, _}) -> if";
  indent 1 "P1 == P2 -> V1 < V2;";
  indent 1 "true -> P1 < P2";
  "end end).";
  (* update buffer *)
  "buffer_update(Current, Last, {{RVId, RVersion}, Id, RValue}, Buffer) ->";
  indent 1 "H1 = case lists:member(Id, Current) of";
  indent 2 "true  -> maps:update_with({RVId, RVersion}, fun(M) -> M#{ Id => RValue } end, #{ Id => RValue }, Buffer);";
  indent 2 "false -> Buffer";
  indent 1 "end,";
	indent 1 "case lists:member(Id, Last) of";
  indent 2 "true  -> maps:update_with({RVId, RVersion + 1}, fun(M) -> M#{ {last, Id} => RValue } end, #{ {last, Id} => RValue }, H1);";
  indent 2 "false -> H1";
  indent 1 "end.";
  (* sleep *)
  "periodic(Interval) ->";
  indent 1 "timer:sleep(Interval)."
]

let gen_export module_map mod_deps =
  let attributes = 
    [[("start", 0)];
    [("out", 2)];
    List.map (fun (inst_id, inst_info) -> (String.lowercase_ascii inst_id, List.length (inst_info.module_info.party) * 2 + 2)) mod_deps]
  and nodes =
    List.concat
      (List.map
        (fun (inst_id, inst_info) ->
          let inst_id = String.uncapitalize_ascii inst_id in
            [List.map (fun i -> (inst_id ^ "_" ^ i, 0)) inst_info.module_info.source;
            List.map
              (fun (i, _, _, e) ->
                if string_of_expr e = "key_input()" then (inst_id ^ "_" ^ i, 3)
                else (inst_id ^ "_" ^ i, 5))
        inst_info.module_info.node])
        mod_deps)
  and parties =
    let inst_party_info = Party.create_party_map mod_deps module_map in
      [List.map (fun (inst_party, _) -> (inst_party, 1)) (M.bindings inst_party_info)]
  in
    concat_map
      "\n"
      (fun l -> "-export([" ^ (concat_map "," (fun (f,n) -> f ^ "/" ^ string_of_int n) l) ^ "]).")
      (attributes @ nodes @ parties)

let of_xmodule inst_id module_info ti mod_deps graph =
  let user_funs = 
     List.map (fun (i,_) -> (i, EIConst i)) module_info.const @
     List.map (function
               | (i, InternFun EFun(args, _)) -> (i, EIFun (i, List.length args))
               | (i, NativeFun (arg_t, _)) -> (i, EINative (i, List.length arg_t))
               | _ -> assert false) module_info.func in
  let env = List.fold_left (fun m (i,e) -> M.add i e m) M.empty user_funs in
  let env_with_nodes =
    List.map (fun (i, _, _, _) -> i) module_info.node @ module_info.source |>
    List.fold_left (fun m i -> M.add i (EISigVar i) m) env
  in
  String.concat "\n\n" (
    (* concat_map "\n" (fun s -> "%" ^ s) (String.split_on_char '\n' (string_of_graph graph)) :: *)
    List.map (def_const env) module_info.const
    @ List.map (def_fun env) module_info.func
    @ (List.map (fun i -> in_node inst_id i graph mod_deps) module_info.source)
    @ List.map (def_node graph (inst_id, (try_find inst_id mod_deps)) env_with_nodes) module_info.node
  )
        
let gen_leader_msg (leader_module_id, party) dsts n =
  indent n (send leader_module_id ("{" ^ party ^ ", Ver}"))
  ^ (if is_empty dsts then "" else ",\n")
  ^ concat_map ",\n" (fun dst -> indent n (send dst "{" ^ party ^ ", Ver}")) dsts

let spawn id (inst_id, inst_info) module_map graphs =
  let module_id = String.uncapitalize_ascii inst_id in
  let init i = try_find i (try_find inst_info.module_info.id (init_value_map module_map)) |> erlang_of_expr (try_find inst_info.module_info.id (env_map module_map)) in
  let init_map node prog_party_set =
    "#{"
    ^ concat_map
        ", "
        (fun i -> "{last, " ^ i ^ "} => " ^ init i)
        (List.filter
          (fun id -> S.mem (try_find id inst_info.module_info.id2party) prog_party_set)
          (extract_node node.input_last))
    ^ "}"
  in
  if List.exists (fun i -> compare i id == 0) inst_info.module_info.source then
    indent 1 "register(" ^ module_id ^ "_" ^ id ^ ", "
    ^ "spawn(?MODULE, " ^ module_id ^ "_" ^ id ^ ", []))"
  else
    let node = try_find id (try_find inst_info.module_info.id graphs) in
    let target_party =
      List.fold_left
        (fun s (_, prog_p) ->
          S.add (try_find prog_p inst_info.inst_party_map) s)
          S.empty
          (node.input_current @ node.input_last)
    in
    let inv_map =
      List.fold_left
        (fun m (prog_p, inst_p) ->
          if M.mem inst_p m then M.add inst_p (prog_p :: M.find inst_p m) m
          else M.add inst_p [prog_p] m)
        M.empty
        (List.filter (fun (_, inst_p) -> S.mem inst_p target_party) (M.bindings inst_info.inst_party_map))
    in
    let key_input_nodes =
      List.fold_left
        (fun s (id, _, _, e) ->
          if string_of_expr e = "key_input()" then S.add id s
          else s)
        S.empty
        inst_info.module_info.node
      in
      indent 1 "register(" ^ module_id ^ "_" ^ id ^ ", "
      ^ if S.mem id key_input_nodes then "spawn(?MODULE, " ^ module_id ^ "_" ^ id ^ ", [" ^ init id ^ ", 0, []]))"
        else "spawn(?MODULE, " ^ module_id ^ "_" ^ id ^ ", [#{"
      ^ concat_map
          ", "
          (fun (inst_p, lst) ->
            "{" ^ inst_p ^ ", 0} => "
            ^ init_map node (S.of_list lst))
          (M.bindings inv_map)
      ^ "}, #{"
      ^ concat_map
          ", "
          (fun inst_p -> inst_p ^ " => 0")
          (S.elements (S.union (S.of_list (extract_key inv_map)) (S.singleton (try_find (try_find id inst_info.module_info.id2party) inst_info.inst_party_map))))
      ^ "},  #{}, [], []]))"

let gen_start module_map mod_deps graphs =
  let gen_module (inst_id, inst_info) =
    let module_id = String.uncapitalize_ascii inst_id in
    indent 1 ("register(" ^ module_id ^ ", spawn(?MODULE, " ^ module_id ^ ", [[], [], "
              ^ String.concat ", " (List.map (fun (p, inst_p) -> inst_p ^ ", 0") (M.bindings inst_info.inst_party_map)) ^ "]))")
in
let gen_node (inst_id, inst_info) =
   List.map (fun (id, _) -> spawn id (inst_id, inst_info) module_map graphs) (M.bindings inst_info.module_info.id2party)
in
let gen_party mod_dep modules =
  let party_map = Party.create_party_map mod_dep modules in
    concat_map
      ",\n"
      (fun (inst_party, _) ->
        indent 1 ("register(" ^ inst_party ^ ", spawn(?MODULE, " ^ inst_party ^ ", [0]))"))
      (List.filter
        (fun (_, ((_, (fun_id, _)), _)) -> fun_id = "any_party")
        (M.bindings party_map)
      @ List.filter
        (fun (_, ((_, (fun_id, _)), _)) -> fun_id <> "any_party")
        (M.bindings party_map))
  in
    "start() -> \n"
    ^ (if is_empty mod_deps then indent 1 "void."
      else
        String.concat ",\n" (List.map gen_module mod_deps) ^ ",\n"
        ^ String.concat ",\n" (List.concat (List.map gen_node mod_deps)) ^ ",\n"
        ^ gen_party mod_deps module_map ^ ".")

let gen_out mod_dep =
  let create_out (inst_id, inst_info) =
    concat_map
      ";\n"
      (fun sink -> "out(" ^ String.uncapitalize_ascii inst_id ^ "_" ^ sink ^ ", Value) -> void")
      inst_info.module_info.sink
  in
    "% replace here\n"
    ^ String.concat ";\n" (List.map create_out mod_dep) ^ ";\n"
    ^ "out(_, _) -> erlang:error(badarg)."

let def_party mod_deps modules =
  let party_map = Party.create_party_map mod_deps modules in
  let party_graph = Party.get_party_graph mod_deps modules in
  (match Party.find_loop (extract_key party_map) party_graph with
      | [] -> ()
      | loops -> raise (PartyLoop(loops)));
  let mod_dep_map = list2map mod_deps in
    (* Party.string_of_party_graph party_graph ^ "\n" *)
    concat_map
      "\n"
      (fun p ->
        p ^ "(Ver) ->\n"
        ^ let ((leader_module, _), (fun_id, expr)) = fst (try_find p party_map) in
          let leader = String.uncapitalize_ascii leader_module in
          (match fun_id with
            "periodic" ->
              gen_leader_msg (leader, p) (try_find p party_graph).outs 1 ^ ",\n"
              ^ indent 1 ("periodic(" ^ erlang_of_expr (try_find (try_find leader_module mod_dep_map).module_info.id (env_map modules)) (List.hd expr) ^ "),\n")
            | "any_party" ->
              indent 1 "receive\n"
              ^ indent 2 "{Party, _} when " ^ String.concat "; " (List.map (fun party -> "Party =:= " ^ party) (try_find p party_graph).ins) ^ " ->\n"
              ^ gen_leader_msg (leader, p) (try_find p party_graph).outs 3 ^ "\n"
              ^ indent 1 "end,\n"
            | _ ->
              indent 1 ("% fill interval by implementing " ^ fun_id ^  "\n")
              ^ gen_leader_msg (leader, p) (try_find p party_graph).outs 1 ^ ",\n")
              ^ indent 1 (p ^ "(Ver + 1).\n"))
      (extract_key party_map)

let gen_request_node_fun inst_id inst_info graph =
  let target_lst_map = inst_info.outputs_module in
  let parties = extract_key target_lst_map in
  let gen_request_node inst_id party =
    String.concat
      "\n"
      ((String.uncapitalize_ascii inst_id ^ "_request_node(" ^ try_find party inst_info.inst_party_map ^ ", Ver) ->")
        :: let node2party_map = inst_info.module_info.id2party in
          let target_extern_input = List.filter (fun extern_input -> try_find extern_input node2party_map = party) inst_info.module_info.extern_input
          and target_sink =
            let sink_and_party = List.map (fun sink -> (sink, try_find sink node2party_map)) inst_info.module_info.sink in
              List.map
                fst
                (List.filter
                  (fun (sink, p) ->
                    p = party
                    && let sink_dep = try_find sink graph in
                        List.filter
                          (fun root ->
                            try_find root node2party_map = party)
                          (sink_dep.root @ sink_dep.extern_input) = [])
                  sink_and_party)
          in
          let target = List.append target_sink target_extern_input in
            (if target = [] then indent 1 "void"
            else
              concat_map
                ",\n"
                (fun target -> indent 1 (send (String.uncapitalize_ascii inst_id ^ "_" ^ target) ("{request, {" ^ (M.find party inst_info.inst_party_map) ^ ", Ver}}")))
                target)
        :: [])
  in
    concat_map ";\n" (fun party -> gen_request_node inst_id party) parties ^ "."
let gen_args base_module_party inst_party_map =
  concat_map
    ", "
    (fun p ->
      let inst_party = (try_find p inst_party_map) in
        inst_party ^ ", " ^ String.capitalize_ascii inst_party ^ "_ver")
    base_module_party

let check_node_loop mod_deps graphs =
  List.iter (fun (_, inst_info) ->
    match Dependency.find_loop
            (List.map (fun src -> (src, try_find (try_find src inst_info.module_info.id2party) inst_info.inst_party_map)) inst_info.module_info.source)
            (try_find inst_info.module_info.id graphs)
            inst_info.module_info.id2party
            inst_info.inst_party_map
    with
      [] -> ()
      | loops -> raise (NodeLoop(loops)))
  mod_deps

let buf_compare_ver_code (inst_id, inst_info) =
  (String.concat
    ";\n"
    (List.map
      (fun prog_party -> 
        let inst_party = try_find prog_party inst_info.inst_party_map in
          String.concat
            "\n"
            (indent 3 ("{" ^  inst_party ^ ", " ^ "Ver} when Ver > " ^ String.capitalize_ascii inst_party ^ "_verT ->")
            :: indent 4 ("{[{" ^ inst_party ^ ", Ver} | Buf], "
                        ^ concat_map
                          ", "
                          (fun prog_party ->
                            let inst_p = (try_find prog_party inst_info.inst_party_map) in
                              String.capitalize_ascii inst_p ^ "_verT")
                          inst_info.module_info.party
                        ^ "};")
            :: indent 3 ("{" ^  inst_party ^ ", " ^ "Ver} when Ver =:= " ^ String.capitalize_ascii inst_party ^ "_verT ->")
            :: (String.concat
                  ",\n"
                  ((if try_find prog_party inst_info.inputs_module = [] then indent 4 "void"
                  else (concat_map
                          ",\n" 
                          (fun target ->
                            (indent 4 (send (String.uncapitalize_ascii target) ("{" ^ inst_party ^ ", Ver}"))))
                          (try_find prog_party inst_info.inputs_module)))
            :: (indent 4 (String.uncapitalize_ascii inst_id ^ "_request_node(" ^ inst_party ^ ", Ver)"))
            :: indent 4 ("{Buf, "
                        ^ (concat_map
                            ", "
                            (fun prog_party ->
                              let inst_p = (try_find prog_party inst_info.inst_party_map) in
                                if inst_p = inst_party then String.capitalize_ascii inst_p ^ "_verT + 1"
                                else String.capitalize_ascii inst_p ^ "_verT")
                            inst_info.module_info.party)
                       ^ "};")
            ::[]))
            :: indent 3 ("{" ^  inst_party ^ ", " ^ "Ver} when Ver < " ^ String.capitalize_ascii inst_party ^ "_verT ->")
            :: indent 4 ("{Buf, "
                        ^ (concat_map
                            ", "
                            (fun prog_party ->
                              let inst_p = (try_find prog_party inst_info.inst_party_map) in
                                String.capitalize_ascii inst_p ^ "_verT")
                            inst_info.module_info.party)
                        ^ "}")
            :: []))
      inst_info.module_info.party)
  ^ ";")
  :: indent 3 "_ ->"
  :: indent 4 ("{Buf, "
              ^ (concat_map
                  ", "
                  (fun prog_party ->
                    let inst_p = (try_find prog_party inst_info.inst_party_map) in
                      String.capitalize_ascii inst_p ^ "_verT")
                  inst_info.module_info.party)
              ^ "}")
  :: []

let gen_buf_code (inst_id, inst_info) =
  (indent
    1
    ("{NBuffer, "
    ^ concat_map
        ", "
        (fun prog_party ->
          let inst_party = (try_find prog_party inst_info.inst_party_map) in
            String.capitalize_ascii inst_party ^ "_ver0")
        inst_info.module_info.party
    ^ "} = lists:foldl(fun (Version, {Buf, "
    ^ concat_map
        ", "
        (fun prog_party ->
          let inst_party = (try_find prog_party inst_info.inst_party_map) in
            String.capitalize_ascii inst_party ^ "_verT")
        inst_info.module_info.party
    ^ "}) ->")
    :: indent 2 "case Version of"
    :: [])
    @ buf_compare_ver_code (inst_id, inst_info)
    @ (indent 2 "end"
      :: indent 1 ("end, {[], "
                  ^ (concat_map
                      ", "
                      (fun prog_party ->
                        let inst_party = (try_find prog_party inst_info.inst_party_map) in
                          String.capitalize_ascii inst_party ^ "_ver")
                      inst_info.module_info.party)
                  ^ "}, Sorted_ver_buf),")
      :: [])

let request_buf_compare_ver_code (inst_id, inst_info) =
  (String.concat
    ";\n"
    (List.map
      (fun prog_in ->
        let prog_party = try_find prog_in inst_info.module_info.id2party in
        let inst_party = try_find prog_party inst_info.inst_party_map in
        String.concat
          "\n"
          (indent 3 ("{{" ^ inst_party ^ ", Ver}, " ^ prog_in ^ ", Val} when Ver > " ^ String.capitalize_ascii inst_party ^ "_verT ->")
          :: indent 4 ("{[Msg | Buf], "
                      ^ (concat_map
                          ", "
                          (fun prog_p ->
                            let inst_p = (try_find prog_p inst_info.inst_party_map) in
                              String.capitalize_ascii inst_p ^ "_verT")
                          inst_info.module_info.party)
                      ^ "};")
          :: indent 3 ("{{" ^ inst_party ^ ", Ver}, " ^ prog_in ^ ", Val} when Ver =:= " ^ String.capitalize_ascii inst_party ^ "_verT ->")
          :: indent 4 (send (String.uncapitalize_ascii inst_id ^ "_" ^ prog_in) ("{{" ^ inst_party ^ ", Ver}, Val},"))
          :: (if is_empty (try_find prog_party inst_info.inputs_module) then indent 4 "void,"
             else
              ((String.concat
                ",\n"
                ((concat_map
                    ",\n"
                    (fun target ->
                      (indent 4 (send (String.uncapitalize_ascii target) ("{" ^ inst_party ^ ", Ver}"))))
                    (try_find prog_party inst_info.inputs_module))
                :: []))
              ^ ","))
          :: indent 4 ("{Buf, "
                      ^ (concat_map
                          ", "
                          (fun prog_p ->
                            let inst_p = (try_find prog_p inst_info.inst_party_map) in
                              if inst_p = inst_party then String.capitalize_ascii inst_p ^ "_verT + 1"
                              else String.capitalize_ascii inst_p ^ "_verT")
                        inst_info.module_info.party)
                      ^ "};")
          :: indent 3 ("{{" ^ inst_party ^ ", Ver}, " ^ prog_in ^ ", Val} when Ver < " ^ String.capitalize_ascii inst_party ^ "_verT ->")
          :: indent 4 (send (String.uncapitalize_ascii inst_id ^ "_" ^ prog_in) ("{{" ^ inst_party ^ ", Ver}, Val},"))
          :: indent 4 ("{Buf, "
                      ^ (concat_map
                          ", "
                          (fun prog_p ->
                            let inst_p = (try_find prog_p inst_info.inst_party_map) in
                              String.capitalize_ascii inst_p ^ "_verT")
                          inst_info.module_info.party)
                      ^ "}")
          :: []))
    inst_info.module_info.source)
    ^ ";")
  :: indent 3 "_ ->"
  :: indent 4 ("{Buf, "
              ^ (concat_map
                  ", "
                  (fun prog_p ->
                    let inst_p = (try_find prog_p inst_info.inst_party_map) in
                      String.capitalize_ascii inst_p ^ "_verT")
                  inst_info.module_info.party)
              ^ "}")
  :: []

let gen_request_buf_code (inst_id, inst_info) =
  (indent
    1
    ("{NInBuffer, "
    ^ concat_map
        ", "
        (fun prog_party ->
          let inst_party = (try_find prog_party inst_info.inst_party_map) in
            String.capitalize_ascii inst_party ^ "_verN")
        inst_info.module_info.party
    ^ "} = lists:foldl(fun (Msg, {Buf, "
    ^ concat_map
        ", "
        (fun prog_party ->
          let inst_party = (try_find prog_party inst_info.inst_party_map) in
            String.capitalize_ascii inst_party ^ "_verT")
        inst_info.module_info.party
    ^ "}) ->")
  :: indent 2 "case Msg of"
  :: [])
  @ request_buf_compare_ver_code (inst_id, inst_info)
  @ (indent 2 "end"
    :: indent 1 ("end, {[], "
                ^ (concat_map
                    ", "
                    (fun prog_party ->
                      let inst_party = (try_find prog_party inst_info.inst_party_map) in
                        String.capitalize_ascii inst_party ^ "_ver0")
                    inst_info.module_info.party)
                ^ "}, Sorted_in_buf),")
    :: [])

let gen_inst inst_mod modules =
  let mod_dep_graph = get_mod_dep_graph inst_mod.newnode modules in
  let mod_deps = M.bindings mod_dep_graph
  and graphs = node_dep_map modules
  and types = ti_map modules in
  check_node_loop mod_deps graphs;
  (match Dependency.find_beyond_module_loop inst_mod.newnode mod_deps modules graphs with
      [] -> ()
      | loops -> raise (NodeLoop(loops)));
    let mod_dep_map = list2map mod_deps in
      "-module(" ^ String.uncapitalize_ascii inst_mod.id ^ ").\n"
      ^ gen_export modules mod_deps ^ "\n"
      ^ lib_funcs ^ "\n"
      ^ gen_start modules mod_deps graphs ^ "\n\n"
      ^ gen_out mod_deps ^ "\n\n"
      ^ def_party mod_deps modules ^ "\n"
      ^ concat_map
        "\n"
        (fun (inst_id, inst_info) ->
          let ti = try_find inst_info.module_info.id types in
            String.concat
              "\n"
              (((String.uncapitalize_ascii inst_id ^ "(Ver_buffer, In_buffer, "
                ^ gen_args inst_info.module_info.party inst_info.inst_party_map
                ^ ") ->")
              :: indent 1 "Sorted_ver_buf = lists:sort(?SORTVerBuffer, Ver_buffer),"
              :: indent 1 "Sorted_in_buf = lists:sort(?SORTInBuffer, In_buffer),"
              :: [])
              @ gen_buf_code (inst_id, inst_info)
              @ gen_request_buf_code (inst_id, inst_info)
              @ (indent 1 "receive"
                :: indent 2 "{_, _} = Ver_msg ->"
                :: indent 3 (String.uncapitalize_ascii inst_id ^ "(lists:reverse([Ver_msg | NBuffer]), NInBuffer, "
                            ^ concat_map
                                ", "
                                (fun prog_party ->
                                  let inst_party = (try_find prog_party inst_info.inst_party_map) in
                                    inst_party ^ ", " ^ String.capitalize_ascii inst_party ^ "_verN")
                                inst_info.module_info.party
                            ^ ");")
                :: indent 2 "{_, _, _} = In_msg ->"
                :: indent 3 (String.uncapitalize_ascii inst_id ^ "(NBuffer, lists:reverse([In_msg | NInBuffer]), "
                            ^ concat_map
                                ", "
                                (fun prog_party ->
                                  let inst_party = (try_find prog_party inst_info.inst_party_map) in
                                    inst_party ^ ", " ^ String.capitalize_ascii inst_party ^ "_verN")
                                inst_info.module_info.party
                            ^ ")")
                :: indent 1 "end."
                :: of_xmodule inst_id (M.find inst_info.module_info.id modules) ti mod_dep_map (try_find inst_info.module_info.id graphs)
                :: gen_request_node_fun inst_id inst_info (try_find inst_info.module_info.id graphs)
                :: [])))
        mod_deps
