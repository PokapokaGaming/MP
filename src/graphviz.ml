open Syntax
open Util
open Module

let colors =
  ["#1f77b4"; "#ff7f0e"; "#2ca02c"; "#d62728"; "#9467bd"; "#8c564b"; "#e377c2"; "#7f7f7f"; "#bcbd22"; "#17becf"]

let loop_table loops =
  List.fold_left (fun set loop ->
    List.fold_left2 (fun set s t -> PairS.add (s,t) set) set loop (List.tl loop @ [List.hd loop])
  ) PairS.empty loops

let of_xmodule inst_mod module_map =
  let mod_deps = M.bindings (Dependency.get_mod_dep_graph inst_mod.newnode module_map) in
  let graphs = List.fold_left (fun m (id, module_info) -> M.add id (Dependency.get_graph module_info) m) M.empty (M.bindings module_map) in
  let loops =
    List.fold_left
      (fun m (inst_id, inst_info) ->
        M.add inst_id
          (loop_table
            (List.map
              snd
              (Dependency.find_loop
                (List.map
                  (fun src ->
                    (src, M.find (M.find src inst_info.Dependency.module_info.id2party) inst_info.Dependency.inst_party_map))
                  inst_info.Dependency.module_info.source)
                (M.find inst_info.Dependency.module_info.id graphs)
                inst_info.Dependency.module_info.id2party
                inst_info.Dependency.inst_party_map)))
        m)
        M.empty
        mod_deps
  in
  let def key (inst_id, inst_info) =
    let module_info = inst_info.Dependency.module_info in
    inst_id ^ "_" ^ key ^ " [label=\"" ^ key ^ "\"" ^
    (if List.mem key module_info.source then ", shape = \"invhouse\"" else "") ^
    (if List.mem key module_info.sink then
     ", style = filled, shape = invtriangle, fillcolor = \"#e4e4e4\"" else "") ^
    "];" in
    let edge (key, dep) inst_id =
      String.concat "" (List.map (fun i ->
        if (PairS.mem (i,key) (M.find inst_id loops)) then
          indent 2 inst_id ^ "_" ^ i ^ " -> " ^ inst_id ^ "_" ^ key ^ " [color = red];\n"
        else
          indent 2 inst_id ^ "_" ^ i ^ " -> " ^ inst_id ^ "_" ^ key ^ ";\n"
      ) (extract_node dep.Dependency.input_current) @
      List.map (fun i -> indent 2 inst_id ^ "_" ^ i ^ " -> " ^ inst_id ^ "_" ^ key ^ " [style = dashed];\n") (extract_node dep.Dependency.input_last))
    in
  let between_mod_edges mod_deps =
    let mod_edge (inst_id, inst_info) =
      String.concat
        ""
        (List.map
          (fun prog_sink ->
            let inst_nn = M.find prog_sink inst_info.Dependency.out_nns in
              String.concat
                ""
                (List.map
                  (fun (module_id, target_in) ->
                    indent 1 (inst_id ^ "_" ^ prog_sink ^ " -> " ^ module_id ^ "_" ^ target_in ^ ";\n"))
                  (M.find inst_nn inst_info.Dependency.inst_in_map)))
                  inst_info.Dependency.module_info.sink)
    in
    String.concat "" (List.map mod_edge mod_deps)
  in
  let def_subgraph key (inst_id, inst_info) color nodes =
    indent 1 "subgraph " ^ key ^ " {\n"
    ^ indent 2 ("label=\"" ^ inst_id ^ "\"; color=\"" ^ color ^ "\"; fontcolor=\"" ^ color ^ "\";\n")
    ^ concat_map "\n" (indent 2) nodes ^ "\n"
    ^ concat_map "" (fun (node, dep) -> edge (node, dep) inst_id) (M.bindings (M.find inst_info.Dependency.module_info.id graphs))
    ^ indent 1 "}" in
  "digraph " ^ inst_mod.id ^ " {\n" ^
    (* concat_map "\n" (indent 1) (List.map def deps) ^ "\n\n" ^ *)
    String.concat "\n" (List.mapi (fun i (inst_id, inst_info) ->
      def_subgraph
        ("cluster_" ^ string_of_int i)
        (inst_id, inst_info)
        (List.nth colors (i mod 10)) (List.map (fun (id, _) -> def id (inst_id, inst_info)) (M.bindings inst_info.module_info.id2party))
    ) mod_deps) ^ "\n" ^
    between_mod_edges mod_deps ^
    (*
    indent 1 "{ rank = source; " ^ String.concat "; " xmod.source ^ "; }\n" ^
    indent 1 "{ rank = sink; " ^ String.concat "; " xmod.sink ^ "; }" ^
    *)
  "}"
