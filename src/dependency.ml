open Syntax
open Util
open Module

type element = 
    Current of id
  | Last of id

module ES = Set.Make(struct
                      type t = element
                      let compare x y = match x, y with
                        | Current a, Current b 
                        | Last a, Last b
                          -> compare a b
                        | Current _, Last _ -> 1
                        | Last _, Current _ -> 1
                    end);;

type dependency = {
  input_current: (id * string) list;
  input_last: (id * string) list;
  root: id list;
  extern_input: id list;
  output: (id * string) list;
  is_output: bool
}

exception InvalidAtLast of string list

let get_graph xmodule = 
  let extract nodes = 
    let rec f = function
      | EConst _ -> ES.empty
      | EId id -> if S.mem id nodes then ES.singleton (Current id) else ES.empty
      | EAnnot (id, _) -> ES.singleton (Last id) (* TODO: Check id is in nodes *)
      | EApp (id, es) -> List.map f es |> List.fold_left ES.union ES.empty
      | EBin (_, e1, e2) -> ES.union (f e1) (f e2)
      | EUni (op, e) -> f e
      | ELet (binders, expr) ->
        let (outer, inner) = List.fold_left (fun (outer, inner) (i, e, _) -> 
          (ES.union (f e) outer, ES.remove (Current i) inner)
        ) (ES.empty, f expr) binders 
        in ES.union outer inner
      | EIf(c, a, b) -> ES.union (f c) (ES.union (f a) (f b))
      | EList es | ETuple es -> 
        List.map f es |> List.fold_left ES.union ES.empty
      | EFun (args, e) -> 
        ES.diff (f e) (ES.of_list (List.map (fun a -> Current a) args))
      | ECase(m, list) -> (f m :: List.map (fun (_, e) -> f e) list) |> List.fold_left ES.union ES.empty
      | EFold (_, init, id) -> 
        (* Fold depends on init expr and the variadic input id *)
        ES.union (f init) (if S.mem id nodes then ES.singleton (Current id) else ES.empty)
      | ECount id ->
        (* Count depends on the variadic input id *)
        if S.mem id nodes then ES.singleton (Current id) else ES.empty
    in f
  in
  let inv_map m =
    M.fold (fun src -> 
      ES.fold (function | Current dst | Last dst ->
        M.update dst (function
          | Some(s) -> Some(S.add src s)
          | None    -> Some(S.singleton src)
        )
      )
    ) m M.empty
  in
  let partition = 
    let rec f (cs, ls) = function
      | [] -> (cs, ls)
      | Current i :: xs -> f (i :: cs, ls) xs
      | Last i :: xs -> f (cs, i :: ls) xs
    in f ([], [])
  in
  let nodes = List.fold_left (fun m (i, _, _, e) -> M.add i e m) M.empty xmodule.node in
  let in_ids = S.of_list (extract_key nodes @ xmodule.source) in
  let ins = M.map (extract in_ids) nodes in
  let inv = inv_map ins in
  let ancestors name =
    let rec f visited name =
      if S.mem name visited then S.empty else
      try
        let out = (M.find name inv) in
        S.fold (fun n ->
          S.union (f (S.add name visited) n)
        ) out out
      with Not_found ->
        S.empty
    in f S.empty name
  in
  let map_root root =
    List.fold_left
      (fun m in_name ->
        S.fold (fun node ->
          M.update node (function
            | Some(s) -> Some(S.add in_name s)
            | None    -> Some(S.singleton in_name)
          )
        ) (ancestors in_name) m)
      M.empty
      root
  in
  let id2root = map_root xmodule.source
  and id2extern_root = map_root xmodule.extern_input in
  List.fold_left (fun m i -> M.add i ES.empty m) ins xmodule.source |>
  M.mapi (fun k i -> partition (ES.elements i)) |>
  M.mapi (fun k (cur, last) ->
    let cur_p = List.map (fun cur_id -> (cur_id, try_find cur_id xmodule.id2party)) cur in
    let last_p = List.map (fun last_id -> (last_id, try_find last_id xmodule.id2party)) last in
    let out_p =
      let output_lst = try S.elements (M.find k inv) with Not_found -> [] in
        List.map (fun output -> (output, try_find output xmodule.id2party)) output_lst
    in
    let roots = try S.elements (M.find k id2root) with Not_found -> [] in
    let extern_roots = try S.elements (M.find k id2extern_root) with Not_found -> [] in
    { 
      input_current = cur_p;
      input_last = last_p;
      output = out_p;
      root = roots;
      extern_input = extern_roots;
      is_output = List.mem k xmodule.sink
    }
  )

let string_of_graph graph =
  graph |>
  M.bindings |>
  List.map (fun (k, r) -> "" ^ k ^ ": in_c(" ^ (String.concat ", " (List.map (fun (i, p) -> i ^ ": " ^ p) r.input_current)) ^ ")"
                                 ^ ": in_l(" ^ (String.concat ", " (List.map (fun (i, p) -> i ^ ": " ^ p) r.input_last)) ^ ")"
                                 ^ ": out(" ^ (String.concat ", " (List.map (fun (i, p) -> i ^ ": " ^ p) r.output)) ^ ")"
                                 ^ ": root(" ^ (String.concat ", " r.root) ^ ")") |>
  String.concat "\n"

(*
module_info   : base module
inst_party_map: # (party in programs => instance party)
in_nns        : # (source in program => instance node)
out_nns       : # (sink in program => instance node)
inst_in_map   : # (sink of instance module => (connected module, corresponding source in program) list)
inputs_module : # (party in program => modules connected via source belonging to the party)
outputs_module: # (party in program => modules connected via sink belonging to the party)
*)
type newnode = {
  module_info: t;
  inst_party_map: string M.t;
  in_nns: string M.t;
  out_nns: string M.t;
  inst_in_map: (string * string) list M.t;
  inputs_module: string list M.t;
  outputs_module: string list M.t
}

let suffix_cnt = ref 0

(* get map: # (instance module name => newnode) *)
let get_mod_dep_graph newnodes module_map =
  let inst_id_lst =
    List.rev
      (List.fold_left
        (fun acc (_, i, _, _) ->
          suffix_cnt := !suffix_cnt + 1;
          (i ^ "_" ^ string_of_int !suffix_cnt) :: acc)
        []
      newnodes)
  in
  let inst_sink2inst_id =
    list2map
      (List.concat
        (List.map2
          (fun (outputs, _, _, _) newid ->
            (List.map
              (fun out -> (out, newid))
              outputs))
          newnodes
          inst_id_lst))
  in
  let map_inst_party prog_party inst_party =
    List.map2
      (fun prog_p inst_p ->(prog_p, inst_p))
      prog_party
      inst_party
  in
  let all_newnode_map =
    map_nil
      (List.concat (List.map (fun (outputs, _, _, inputs) -> outputs @ inputs) newnodes))
  in
  let nn_and_connected_src_info =
    let in_newnode_lst (_, i, _, inputs) inst_id =
      let module_src = (try_find i module_map).source in
        List.map2 (fun inst_in prog_in -> (inst_in, (inst_id, prog_in))) inputs module_src
    in
    List.concat (List.map2 in_newnode_lst newnodes inst_id_lst)
  in
  let create_newnode_info inst_id (outputs, id, parties, inputs) =
    let module_info = try_find id module_map in
    let init_map = map_nil module_info.party in
    let inst_sink2connected_module =
      List.fold_left
        (fun m (nn, target) ->
          let lst = try_find nn m in
            M.add nn (target :: lst) m)
        all_newnode_map
        (List.filter (fun (nn, _) -> List.mem nn outputs) nn_and_connected_src_info)
    in
    let get_connected_modules prog_io nns =
      let get_prog_party_and_inst_module =
        List.map2
          (fun src node ->
            let src_party = try_find src module_info.id2party in
              (src_party, try_find node inst_sink2inst_id))
          prog_io
          nns
      in
        List.fold_left
          (fun m (party, inst_module) ->
            let lst = try_find party m in
              if List.mem inst_module lst then m
              else M.add party (inst_module :: lst) m)
          init_map
          get_prog_party_and_inst_module
    in
    {
      module_info = module_info;
      inst_party_map =  list2map (map_inst_party module_info.party parties);
      in_nns = list2map (List.map2 (fun input nn -> (input, nn)) module_info.source inputs);
      out_nns = list2map (List.map2 (fun output nn -> (output, nn)) module_info.sink outputs);
      inst_in_map = inst_sink2connected_module;
      inputs_module = get_connected_modules module_info.source inputs;
      outputs_module = get_connected_modules module_info.sink outputs
    }
  in
  List.fold_left2 (fun m inst_id inst_info -> M.add inst_id (create_newnode_info inst_id inst_info) m) M.empty inst_id_lst newnodes

let string_of_mod_dep_graph mod_deps =
  let io_rel =
    List.fold_left
      (fun m (inst_id, inst_info) ->
        List.fold_left
          (fun m (out_nn, connected_modules) ->
            List.fold_left
              (fun m (module_id, prog_in) ->
                PairM.add
                  (module_id, prog_in)
                  (inst_id, try_find out_nn (list2map (switch_pair (M.bindings inst_info.out_nns))))
                  m)
              m
              connected_modules)
          m
          (M.bindings inst_info.inst_in_map))
      PairM.empty
      mod_deps
  in
  List.map
    (fun (inst_id, inst_info) ->
      "%" ^ inst_id ^ ": "
      ^ "in("
      ^ String.concat
          ", "
          (List.map
            (fun prog_in ->
              let (module_id, prog_out) = PairM.find (inst_id, prog_in) io_rel in
                prog_in ^ ": (" ^ module_id ^ ", "^ prog_out ^ ")")
            inst_info.module_info.source)
      ^"), "
      ^ "out("
      ^ String.concat
          ", "
          (List.map
            (fun prog_out ->
              let out_nn = try_find prog_out inst_info.out_nns in
              prog_out ^ ": "
              ^ String.concat
                  ", "
                  (List.map
                    (fun (module_id, prog_in) ->
                      "(" ^ module_id ^ ", " ^ prog_in ^ ")")
                    (try_find out_nn inst_info.inst_in_map)))
            inst_info.module_info.sink)
      ^ ")")
    mod_deps
    |> String.concat "\n"

(* find loop *)
let rec f trace np_list inv =
  let rec dropwhile thunk (a, p) = function
    | [] -> None
    | (x, xp) :: xs when x = a -> Some((x, xp) :: thunk)
    | (x, xp) :: xs -> dropwhile ((x, xp) :: thunk) (a, p) xs
  in
    match np_list with
      | [] -> []
      | np :: nps ->
        match dropwhile [] np trace with
      | Some(xps) ->
        let set = List.fold_left (fun s p -> S.add p s) S.empty (List.map snd xps) in
          if S.cardinal set = 1 then [List.hd (S.elements set), List.map fst xps]
          else []
      | None ->
        (f (np :: trace) (Hashtbl.find_all inv np) inv) @ (f trace nps inv)

let find_loop in_nodes graph id2party inst_party_map =
  let inv = Hashtbl.create 13 in
  M.iter (fun dst d ->
    List.iter (fun (src, party) ->
      let key = (src, try_find (try_find src id2party) inst_party_map)
      and dep_val = (dst, try_find (try_find dst id2party) inst_party_map) in
        Hashtbl.add inv key dep_val
    ) d.input_current
  ) graph;
  List.concat (List.map (fun n -> f [] (Hashtbl.find_all inv n) inv) in_nodes)

  let find_beyond_module_loop newnodes mod_deps module_map graphs =
    let inv = Hashtbl.create 13 in
    List.iter
      (fun mod_dep ->
        let graph = try_find mod_dep.module_info.id graphs
        and prog_out_and_inst_sink = List.map (fun prog_out -> (prog_out, try_find prog_out mod_dep.out_nns)) mod_dep.module_info.sink in
          List.iter
            (fun (prog_out, out_nn) ->
              let root_nns =
                List.map
                  (fun prog_in ->
                    (prog_in, try_find prog_in mod_dep.in_nns))
                  (try_find prog_out graph).root
              in
                List.iter
                  (fun (prog_in, src_nn) ->
                    Hashtbl.add inv
                    (src_nn, try_find (try_find prog_in mod_dep.module_info.id2party) mod_dep.inst_party_map)
                    (out_nn, try_find (try_find prog_out mod_dep.module_info.id2party) mod_dep.inst_party_map))
                  root_nns)
            prog_out_and_inst_sink)
      (List.map snd mod_deps);
    let nns =
      List.fold_left
        (fun s inst_info ->
          (List.fold_left
            (fun s (prog_out, out_nn) ->
              PairS.add
                (out_nn, try_find (try_find prog_out inst_info.module_info.id2party) inst_info.inst_party_map)
                s)
            s
            (M.bindings inst_info.out_nns)))
        PairS.empty
        (List.map snd mod_deps)
    in
      List.concat (List.map (fun n -> f [] (Hashtbl.find_all inv n) inv) (PairS.elements nns))
