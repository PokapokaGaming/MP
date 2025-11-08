module S = Set.Make(String);;
module M = Map.Make(String);;
module PairS = Set.Make(struct type t = string * string let compare = compare end);;
module PairM = Map.Make(struct
                      type t = string * string
                      let compare (a1, b1) (a2, b2) =
                        if a1 = a2 then compare b1 b2
                        else compare a1 a2
                    end);;

exception UnknownId of string

let try_find id m = begin
  try M.find id m with
    Not_found -> (raise (UnknownId(id)))
end

let is_empty lst = lst = []
let switch_pair pairs = List.map (fun (f, l) -> (l, f)) pairs
let concat_map s f l = String.concat s (List.map f l)
let extract_key map = List.map fst (M.bindings map)
let list2map pairs = List.fold_left (fun m (k, v) -> M.add k v m) M.empty pairs
let map_nil keys = List.fold_left (fun m k -> M.add k [] m) M.empty keys

let indent n s = String.make n '\t' ^ s
