open Syntax
open Util

type tannot
   = TAConst of Type.t option
   | TANode of Type.t option
   | TAFun of Type.t option list * Type.t option

type fun_type
   = | InternFun of expr
     | NativeFun of (Type.t option list * Type.t option)

type t = {
  id: moduleid;
  party: string list;
  source: id list;
  extern_input: id list;
  sink: id list;
  const: (id * expr) list;
  func: (id * fun_type) list;
  node: (id * string * expr option * expr) list;
  newnode: (string list * string * string list * string list) list;
  leader: (id * id * expr list) list;
  typeinfo: tannot M.t;
  id2party: string M.t
}

let collect defs = 
  let (a, b, c, d, e, f) = List.fold_left (fun (cs, fs, exts, ns, nns, ps) -> function
    | Const ((i, _), e) -> ((i, e) :: cs, fs, exts, ns, nns, ps)
    | Fun ((i, _), e)  -> (cs, (i, InternFun e) :: fs, exts, ns, nns, ps)
    | Node ((i, _, p), init, e) ->
      if string_of_expr e = "key_input()" then (cs, fs, i :: exts, (i, p, init, e) :: ns, nns, ps)
      else (cs, fs, exts, (i, p, init, e) :: ns, nns, ps)
    | NewNode (outputs, module_name, parties, inputs) ->  (cs, fs, exts, ns, (outputs, module_name, parties, inputs) :: nns, ps)
    | Native (i, arg_tlst_ret)  -> (cs, (i, NativeFun(arg_tlst_ret))::fs, exts, ns, nns, ps)
    | Party (i, funid, e)  -> (cs, fs, exts, ns, nns, (i, funid, e) :: ps)
  ) ([], [], [], [], [], []) defs in
  (List.rev a, List.rev b, List.rev c, List.rev d, List.rev e, List.rev f)
let remove_type = function
  (it, p) -> ((fst it), p)
let remove_party = function
  (it,_) -> (fst it, snd it)
let extract_node nps = List.map fst nps

let make_type program = 
  let in_t  = List.fold_left (fun m (i,t) -> M.add i (TANode (Some t)) m) M.empty (List.map remove_party program.in_node) in
  let def_t = List.fold_left (fun m -> function
    | (Const ((i, t), _)) -> M.add i (TAConst t) m
    | (Fun ((i, (ta,tr)), _)) -> M.add i (TAFun (ta, tr)) m
    | (Node ((i, t, _), _, _)) -> M.add i (TANode t) m
    | (Native (i, (ta,tr))) -> M.add i (TAFun (ta, tr)) m
    | (NewNode (_, _, _, _)) | Party (_, _, _) -> m
  ) in_t program.definition in
  let out_t = List.fold_left (fun m (i,t) -> M.add i (TANode (Some t)) m) def_t (List.map remove_party program.out_node) in
  out_t

let of_program program =
  let (const, func, old_extern_input, node, newnode, leader) = collect program.definition in
  (* Use only in_node declarations as true extern_input, not key_input() nodes *)
  (* key_input() nodes are computation nodes that generate values internally *)
  let extern_input = extract_node (List.map remove_type program.in_node) in
  {
    id = program.id;
    party = program.party;
    source = extract_node (List.map remove_type program.in_node);
    extern_input = extern_input;
    sink = extract_node (List.map remove_type program.out_node);
    const = const;
    node = node;
    newnode = newnode;
    func = func;
    leader = leader;
    typeinfo = make_type program;
    id2party = list2map program.id_party_lst
  }
