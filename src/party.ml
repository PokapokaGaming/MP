open Util
open Dependency

exception LeaderError of string
exception ReachableError of string * string * string

type dependency = {
  ins: string list;
  outs: string list;
}

type uft = {
  parent: string M.t;
  rank: int M.t
}

let create_party_map mod_deps module_map =
  let leader_lst =
    let extract_leaders (inst_id, inst_info) = List.map (fun (id, fun_id, e) -> (inst_id, id, fun_id, e)) (inst_info.module_info.leader) in
    List.concat (List.map extract_leaders mod_deps)
  in
  let leader_set = List.fold_left (fun s (inst_id, party, _, _) -> PairS.add (inst_id, party) s) PairS.empty leader_lst
  and leader_map = List.fold_left (fun m (inst_id, party, fun_id, expr) -> PairM.add (inst_id, party) (fun_id, expr) m) PairM.empty leader_lst in
  let inst_p_belong =
    let create_p_tuple (inst_id, inst_info) =
      List.map
        (fun (prog_party, inst_party) -> (inst_party, (inst_id, prog_party)))
        (M.bindings inst_info.inst_party_map)
    in
    List.concat (List.map create_p_tuple mod_deps)
  in
  let inst_p2module_p =
    List.fold_left
      (fun m (inst_party, elm) ->
        if M.mem inst_party m then
          M.add inst_party (PairS.union (PairS.singleton elm) (try_find inst_party m)) m
        else M.add inst_party (PairS.singleton elm)  m)
      M.empty
      inst_p_belong
  in
  List.fold_left
    (fun m (inst_party, set) ->
      let inter_set = PairS.inter set leader_set in
      if PairS.cardinal inter_set <> 1 then raise (LeaderError inst_party)
      else
        let leader = List.hd (PairS.elements inter_set) in
          M.add
            inst_party
            ((leader, PairM.find leader leader_map), set)
            m)
    M.empty
    (M.bindings inst_p2module_p)

let init_uft (inst_id_lst, newnodes) =
  let init_uft =
    {
      parent = List.fold_left (fun m id -> M.add id id m) M.empty inst_id_lst;
      rank = List.fold_left (fun m id -> M.add id 0 m) M.empty inst_id_lst
    }
  in
    List.fold_left
      (fun m p ->
        M.add p (ref init_uft) m)
      M.empty
      (List.concat (List.map (fun inst_info -> List.map snd (M.bindings inst_info.inst_party_map)) newnodes))

let rec find inst_id uft =
  if try_find inst_id !uft.parent = inst_id then inst_id
  else
    let new_parent = find (try_find inst_id !uft.parent) uft in
      uft := {
        parent = M.add inst_id new_parent !uft.parent;
        rank = !uft.rank
      };
      new_parent

let unite x y uft =
  let find_x = find x uft in
  let find_y = find y uft in
  if find_x = find_y then ()
  else
    let rank_x = try_find find_x !uft.rank
    and rank_y = try_find find_y !uft.rank in
      if rank_x < rank_y then
        uft := {
          parent = M.add find_x find_y !uft.parent;
          rank = !uft.rank;
        }
      else
        uft := {
          parent = M.add find_y find_x !uft.parent;
          rank = if rank_x = rank_y then M.add find_x (rank_x + 1) !uft.rank
                 else !uft.rank
        }

let same x y uft=
  find x uft = find y uft

let check_reachable mod_deps party_map =
  let uft = init_uft (List.split mod_deps) in
  List.iter
    (fun (inst_id, inst_info) ->
      let outputs = M.bindings inst_info.inst_in_map in
        List.iter
          (fun (output, ins) ->
            let inv = list2map (switch_pair (M.bindings inst_info.out_nns)) in
              List.iter
                (fun (module_id, _) ->
                  let prog_party = try_find (try_find output inv) inst_info.module_info.id2party in
                    unite inst_id module_id (try_find (try_find prog_party inst_info.inst_party_map) uft))
                ins)
          outputs)
    mod_deps;
  List.iter
    (fun (((leader, lp), _), group) ->
      List.iter
        (fun (module_id, _) ->
          let leader_inst_party = try_find lp (try_find leader (list2map mod_deps)).inst_party_map in
            let lp_uft = try_find leader_inst_party uft in
              if not (same leader module_id lp_uft) then raise (ReachableError (leader, module_id, leader_inst_party))
              else ())
        (PairS.elements group))
    (List.map snd (M.bindings party_map))

let get_party_graph mod_deps module_map =
  let party_map = create_party_map mod_deps module_map in
    check_reachable mod_deps party_map;
    let init_map = map_nil (extract_key party_map) in
    let any_party_leader =
      List.filter
        (fun (_, (_, fun_id, _)) ->
          fun_id = "any_party")
        (List.concat
          (List.map
            (fun newnode ->
              List.map
                (fun leader -> (newnode, leader))
                newnode.module_info.leader)
            (List.map snd mod_deps)))
    in
    let from_map =
      List.fold_left
        (fun m (newnode, (party, _, expr)) ->
          M.add
            (try_find party newnode.inst_party_map)
            (List.map (fun p -> try_find (Syntax.string_of_expr p) newnode.inst_party_map) expr)
            m)
        init_map
        any_party_leader
    and dst_map =
      List.fold_left
        (fun m (newnode, (party, _, expr)) ->
          let parties = List.map (fun p -> try_find (Syntax.string_of_expr p) newnode.inst_party_map) expr in
            List.fold_left
              (fun m dst ->
                let old_lst = try_find dst m in
                  M.add
                    dst
                    ((try_find party newnode.inst_party_map) :: old_lst)
                    m)
              m
              parties)
        init_map
        any_party_leader
    in
      list2map
        (List.map
          (fun party ->
            let dep = {
              ins = try_find party from_map;
              outs = try_find party dst_map
            }
            in
              (party, dep))
            (extract_key party_map))

let string_of_party_graph graph =
  String.concat
    "\n"
    (List.map
      (fun party ->
        let party_dep = try_find party graph in
          "%" ^ party ^ " "
          ^ "from: ["
          ^ String.concat ", " party_dep.ins
          ^ "] "
          ^ "dst: ["
          ^ String.concat ", " party_dep.outs
          ^ "]")
      (extract_key graph))

let find_loop parties dependency =
  let inv = Hashtbl.create 13 in
  M.iter (fun dst d ->
    List.iter (fun src ->
      Hashtbl.add inv src dst
    ) d.ins
  ) dependency;
  let rec dropwhile thunk a = function
    | [] -> None
    | x :: xs when compare x a == 0 -> Some(x :: thunk)
    | x :: xs -> dropwhile (x :: thunk) a xs in
  let rec f trace nodes =
    match nodes with
    | [] -> []
    | n :: ns ->
      match dropwhile [] n trace with
      | Some(xs) ->
        [xs]
      | None ->
        (f (n :: trace) (Hashtbl.find_all inv n)) @ (f trace ns)
  in
  List.concat (List.map (fun n -> f [] (Hashtbl.find_all inv n)) parties)
