type id = string
type moduleid = string

type id_and_type = id * Type.t

(* Variadic input: (name, type, is_variadic) *)
type id_and_type_variadic = id * Type.t * bool

type id_and_type_opt = id * Type.t option

type id_type_opt_party = id * Type.t option * string

type const 
  = CUnit
  | CBool of bool
  | CChar of char
  | CInt of int
  | CFloat of float

type annot
  = ALast

type uniop
  = UNot | UNeg | UInv

type binop 
  = BMul | BDiv | BMod
  | BAdd | BSub
  | BShL | BShR
  | BLt | BLte | BGt | BGte
  | BEq | BNe
  | BAnd
  | BXor
  | BOr
  | BLAnd
  | BLOr
  | BCons

type pattern
  = PWild
  | PNil
  | PConst of const
  | PVar of id
  | PCons of pattern * pattern
  | PTuple of pattern list

type expr 
  = EConst of const
  | EId of id
  | EAnnot of id * annot
  | EApp of id * expr list
  | EBin of binop * expr * expr
  | EUni of uniop * expr
  | ETuple of expr list
  | EList of expr list
  | EIf of expr * expr * expr
  | ELet of (id * expr * Type.t option) list * expr
  | ECase of expr * (pattern * expr) list
  | EFun of id list * expr
  | EFold of binop * expr * id           (* fold(op, init, variadic_input) *)
  | ECount of id                          (* count(variadic_input) *)

type definition
  = Const of id_and_type_opt * expr
  | Node of id_type_opt_party * expr option * expr
  | NewNode of id list * id * id list * id list
  | Fun of (id * (Type.t option list * Type.t option)) * expr
  | Native of id * (Type.t option list * Type.t option)
  | Party of id * id * expr list

(* 新しいinst用のAST *)
type qualified_id = 
  | SimpleId of id
  | QualifiedId of id * id  (* party_id.instance_id *)
  | ParamRef of id          (* $param - テンプレートパラメータへの参照 *)

(* 出力接続: main.s1(c4, c5) または $upstream(n1) のような記述 *)
type output_connection = qualified_id * id list  (* target_instance, local_instances *)

type party_block = {
  party_id: id;
  leader: id;
  periodic_ms: int;
  instances: (id list * id * qualified_id list) list;  (* (outputs, module, inputs) *)
}

(* template: 動的インスタンス化の雛形 *)
(* template_params: 接続先などを動的に指定するためのパラメータ *)
(* 例: template chain(upstream) で upstream がパラメータになる *)
type party_template = {
  template_id: id;
  template_params: id list;  (* パラメータリスト - API呼び出し時に指定 *)
  template_leader: id;  (* 新規パーティ立ち上げ時のリーダー（必須） *)
  template_periodic: int;  (* 新規パーティ立ち上げ時の周期（必須） *)
  template_instances: (id list * id * qualified_id list) list;  (* newnode instances *)
  template_outputs: output_connection list;  (* 既存インスタンスへの出力接続 *)
}

type inst_block =
  | PartyBlock of party_block
  | TemplateBlock of party_template

type inst_program = {
  parties: party_block list;
  templates: party_template list;
}

(* in_node: ((id, type), party, is_variadic) *)
type program = {
  id: moduleid;
  party: string list;
  in_node: (id_and_type * string * bool) list;  (* Added is_variadic flag *)
  out_node: (id_and_type * string) list;
  definition: definition list;
  id_party_lst: (id * string) list;
}

exception InvalidId of string

let rec string_of_const = function
  | CUnit -> "()"
  | CBool b -> string_of_bool b
  | CInt i  -> string_of_int i
  | CFloat f  -> string_of_float f
  | CChar c -> String.make 1 c

let rec string_of_pat = function
  | PNil -> "[]"
  | PWild -> "_"
  | PConst c -> string_of_const c
  | PVar id -> id
  | PCons(hd, tl) -> string_of_pat hd ^ "::" ^ string_of_pat tl
  | PTuple t -> List.map string_of_pat t |> String.concat ","

let rec string_of_expr = function
  | EConst c -> string_of_const c
  | EId id -> id
  | EAnnot (id, ALast) -> id ^ "@last"
  | EApp (id, es) -> id ^ "(" ^ String.concat "," (List.map string_of_expr es) ^ ")"
  | EBin (op, e1, e2) -> string_of_expr e1 ^ " " ^ (match op with
      | BMul -> "*"  | BDiv -> "/"   | BMod -> "%"
      | BAdd -> "+"  | BSub -> "-"   | BShL -> ">>"
      | BShR -> "<<" | BLt -> "<"    | BLte -> "<="
      | BGt -> ">"   | BGte -> ">="  | BEq -> "=="
      | BNe -> "!="  | BAnd -> "&"   | BXor -> "^"
      | BOr -> "|"   | BLAnd -> "&&" | BLOr -> "||" 
      | BCons -> "::") ^ " " ^ string_of_expr e2
  | EUni (op, e) -> (match op with 
    | UNot -> "!" 
    | UNeg -> "-" 
    | UInv -> "~") ^ string_of_expr e
  | ELet (binders, e) ->
    "let " ^ String.concat ", "
              (List.map (fun (i,e,_) -> i ^ " = " ^ string_of_expr e) binders)
    ^ " in " ^ string_of_expr e
  | EIf(c, a, b) ->
    "if " ^ string_of_expr c ^ " then " ^ string_of_expr a ^ " else " ^ string_of_expr b
  | EList es ->
    "[" ^ String.concat "," (List.map string_of_expr es) ^ "]"
  | ETuple es ->
    "(" ^ String.concat "," (List.map string_of_expr es) ^ ")"
  | EFun (args, e) ->
    "fun (" ^ String.concat ", " args ^ ") -> " ^ string_of_expr e
  | ECase(m, cls) -> 
    let f (p, e) = string_of_pat p ^ " -> "^ string_of_expr e ^ "; " in
    "case " ^ string_of_expr m ^ " of " ^ String.concat "" (List.map f cls)
  | EFold (op, init, id) ->
    let op_str = match op with
      | BAdd -> "+" | BSub -> "-" | BMul -> "*"
      | BLAnd -> "&&" | BLOr -> "||" | _ -> "?" in
    "fold(" ^ op_str ^ ", " ^ string_of_expr init ^ ", " ^ id ^ ")"
  | ECount id -> "count(" ^ id ^ ")"

let string_of_definition defs = 
  let str_ty = function | Some (t) -> Type.string_of_type t
                        | None -> "?" in
  let str_opt_expr = function | Some (t) -> string_of_expr t
                              | None -> "?" in
  let str_def = function
    | Const((i,t),e) -> Printf.sprintf "const %s : %s = %s" i (str_ty t) (string_of_expr e)
    | Fun((i,(at,rt)),EFun(ai, e)) ->
      Printf.sprintf "function %s(%s): %s = %s" i (List.map2 (fun i t -> i ^ ":" ^ str_ty t) ai at |> String.concat ",") 
                                                (str_ty rt) (string_of_expr e)
    | Fun(_,_) -> assert false
    | Node((i, t, p), init, e) -> Printf.sprintf
                                    "node %s = %s init = %s type=%s party=%s"
                                    i
                                    (string_of_expr e)
                                    (str_opt_expr init)
                                    (str_ty t)
                                    p
    | NewNode (outputs, moduleid, parties, inputs) -> Printf.sprintf
                                                        "newnode (%s)=%s{%s}(%s)"
                                                        (outputs |> String.concat ",")
                                                        moduleid
                                                        (parties |> String.concat ",")
                                                        (inputs |> String.concat ",")
    | Native(i, (at, rt)) -> 
      let str_lst = List.map str_ty at
      and str_rt = str_ty rt in
        Printf.sprintf
          "native fun %s(%s): %s"
          i
          (str_lst |> String.concat ",")
          str_rt
    | Party (id, funid, exprs) -> Printf.sprintf
                                    "party %s=%s(%s)"
                                    id
                                    funid
                                    (List.map (fun expr -> string_of_expr expr) exprs |> String.concat ",")
  in
    String.concat "\n" (List.map str_def defs)

let print program =
  let str_ty t = Type.string_of_type t in
  print_endline program.id;
  (program.party |> String.concat ",") |> print_endline;
  (List.map (fun (it, p, v) -> "(" ^ fst it ^ "," ^  str_ty (snd it) ^ "," ^ p ^ "," ^ string_of_bool v ^ ")") program.in_node) |> String.concat "," |> print_endline;
  (List.map (fun (it, p) -> "(" ^ fst it ^ "," ^  str_ty (snd it) ^ "," ^ p ^ ")") program.out_node) |> String.concat "," |> print_endline;
  string_of_definition program.definition |> print_endline;
