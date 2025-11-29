%{
open Syntax
open Type

module S = Set.Make(String);;
module NodesS = Set.Make(struct
                           type t = id * string
                           let compare x y = match x, y with
                             | (a, _), (b, _)  -> compare a b
                    end);;

let nodes = ref NodesS.empty

let update (i, p) = 
  nodes := NodesS.add (i, p) !nodes

let reserved_word = S.of_list [
  "in";
  "out"
]

let itp2it = fun (i, t, p) -> (i, t)
let itp2ip = fun (i, t, p) -> (i, p)
let itp2i = fun (i, t, p) -> i
%}

%token
  MODULE IN OUT CONST NODE NEWNODE INIT FUN IF THEN ELSE
  LET CASE OF LAST TRUE FALSE PARTY NATIVE
  LEADER PERIODIC PARTY_TEMPLATE

%token
  COMMA LBRACKET RBRACKET LPAREN RPAREN LBRACE RBRACE COLON COLON2
  SEMICOLON AT ARROW TILDE PLUS MINUS PERCENT ASTERISK DOT
  SLASH XOR OR2 AND2 OR AND EQUAL2 NEQ LSHIFT LTE LT
  RSHIFT GTE GT BANG EQUAL

%token <char> CHAR
%token <string> ID
%token <float> FLOAT
%token <int> INT 
%token EOF

%start <Syntax.program> prog_module
%start <Syntax.inst_program> prog_inst

%right prec_let
%right prec_if
%left OR2
%left AND2
%left OR
%left XOR
%left AND
%left EQUAL2 NEQ
%left LT LTE GT GTE
%right COLON2
%left LSHIFT RSHIFT
%left PLUS MINUS
%left ASTERISK SLASH PERCENT
%right prec_uni

%%

prog_module:
  | MODULE id = ID
    IN innodes = separated_list(COMMA, id_and_type)
    OUT outnodes = separated_list(COMMA, id_and_type)
    defs = nonempty_list(definition)
    EOF
    {  
    {
      id = id;
      party = [];
      in_node = List.map (fun it -> (it, "")) innodes;
      out_node = List.map (fun it -> (it, "")) outnodes;
      definition = defs;
      id_party_lst = NodesS.elements !nodes
    } }
  | MODULE id = ID
    defs = nonempty_list(definition)
    EOF
    {  
    {
      id = id;
      party = [];
      in_node = [];
      out_node = [];
      definition = defs;
      id_party_lst = NodesS.elements !nodes
    } }

definition:
  | CONST it = id_and_type_opt EQUAL e = expr
    { Const(it, e) }
  | NODE init = option(INIT LBRACKET e = expr RBRACKET { e }) it = id_and_type_opt EQUAL e = expr
    { Node((fst it, snd it, ""), init, e) }
  | NEWNODE LPAREN output_ids = separated_list(COMMA, ID) RPAREN EQUAL id = ID LBRACE party_ids = separated_list(COMMA, ID) RBRACE LPAREN input_ids = separated_list(COMMA, ID) RPAREN
    { NewNode(output_ids, id, party_ids, input_ids) }
  | FUN id = ID LPAREN a = fargs RPAREN t_opt = option(COLON type_spec { $2 }) EQUAL e = expr
    { let (ai, at) = List.split a in Fun((id, (at, t_opt)), EFun(ai, e)) }
  | NATIVE FUN id = ID LPAREN a = fargs RPAREN t_opt = option(COLON type_spec { $2 })
    { let (ai, at) = List.split a in Native(id, (at, t_opt)) }
  | PARTY id = ID EQUAL funid = ID LPAREN exprs = separated_list(COMMA, expr) RPAREN
    { Party(id, funid, exprs) }

expr:
  | constant { EConst($1) }
  | ID       { if S.mem $1 reserved_word then (raise (InvalidId($1))) else EId($1) }
  | id = ID AT a = annotation { EAnnot(id, a) }
  | id = ID LPAREN args = args RPAREN { EApp(id, args) }
  | expr binop expr { EBin($2, $1, $3) }
  | uniop expr %prec prec_uni { EUni($1, $2) }
  | LBRACKET args = args RBRACKET { EList(args) }
  | LPAREN xs = args RPAREN 
    { match xs with
        | []   -> EConst(CUnit)
        | [x]  -> x
        | _    -> ETuple(xs)
    }
  | IF c = expr THEN a = expr ELSE b = expr
    %prec prec_if
    { EIf(c, a, b) }
  | LET bs = separated_nonempty_list(SEMICOLON, binder) IN e = expr
    %prec prec_let
    { ELet(bs, e) }
  | CASE e = expr OF bs = nonempty_list(case_body)
    { ECase(e, bs) }

%inline
binop:
  | ASTERISK { BMul }
  | SLASH    { BDiv }
  | PERCENT  { BMod }
  | PLUS     { BAdd }
  | MINUS    { BSub }
  | LSHIFT   { BShL } 
  | RSHIFT   { BShR }
  | COLON2   { BCons }
  | LT       { BLt }
  | LTE      { BLte }
  | GT       { BGt }
  | GTE      { BGte }
  | EQUAL2   { BEq } 
  | NEQ      { BNe }
  | AND      { BAnd }
  | XOR      { BXor }
  | OR       { BOr }
  | AND2     { BLAnd }
  | OR2      { BLOr }

%inline
uniop:
  | MINUS    { UNeg }
  | TILDE    { UInv }
  | BANG     { UNot }
 
args:
  | separated_list(COMMA, expr) { $1 }

fargs:
  | separated_list(COMMA, id_and_type_opt) { $1 }

annotation:
  | LAST { ALast }

constant:
  | TRUE { CBool(true) }
  | FALSE { CBool(false) }
  | CHAR { CChar($1) }
  | INT { CInt($1) }
  | FLOAT { CFloat($1) }

id_party_type:
  | i = ID LBRACE p = ID RBRACE COLON t = type_spec
    { let it = (i, t) in
      update (i, p);
      (it, p) }

id_and_type_opt:
  | i = ID COLON t = type_spec
    { (i, Some t) }
  | i = ID
    { (i, None) }

id_and_type:
  | i = ID COLON t = type_spec
    { (i, t) }

id_party_type_opt:
  | i = ID LBRACE p = ID RBRACE COLON t = type_spec
    { (i, Some t, p) }
  | i = ID LBRACE p = ID RBRACE
    { (i, None, p) }

type_spec:
  | t = prim_type_spec
    { t }
  | LPAREN tpl = separated_nonempty_list(COMMA, type_spec) RPAREN
    { TTuple(tpl) }
  | LBRACKET t = type_spec RBRACKET
    { TList(t) }

prim_type_spec:
  | t = ID
    { match t with
      | "Unit"  -> TUnit
      | "Bool"  -> TBool
      | "Char"  -> TChar
      | "Int"   -> TInt
      | "Float" -> TFloat
      | _ -> assert false }

binder:
  | it = id_and_type_opt EQUAL e = expr
    { let (i,t) = it in (i,e,t) }

case_body:
  | p = pattern ARROW e = expr SEMICOLON
    { (p, e) }

pattern:
  | prim_pattern { $1 }
  | hd = pattern COLON2 tl = pattern { PCons(hd, tl) }
  | LPAREN ps = separated_list(COMMA, pattern) RPAREN
    { match ps with
      | [] -> PConst(CUnit)
      | _  -> PTuple(ps)
    }

prim_pattern:
  | ID
    { match $1 with
      | "_" -> PWild
      | _   -> PVar($1)
    }
  | constant { PConst($1) }
  | LBRACKET RBRACKET { PNil }

(* inst file parser *)
prog_inst:
  | blocks = nonempty_list(inst_block) EOF
    { 
      let parties = List.filter_map (function PartyBlock p -> Some p | _ -> None) blocks in
      let templates = List.filter_map (function TemplateBlock t -> Some t | _ -> None) blocks in
      { parties = parties; templates = templates }
    }

inst_block:
  | p = party_block { PartyBlock p }
  | t = template_block { TemplateBlock t }

party_block:
  | PARTY pid = ID
    LEADER EQUAL lid = ID
    PERIODIC LPAREN ms = INT RPAREN
    instances = nonempty_list(newnode_stmt)
    {
      {
        party_id = pid;
        leader = lid;
        periodic_ms = ms;
        instances = instances;
      }
    }

newnode_stmt:
  | NEWNODE LPAREN outputs = separated_list(COMMA, ID) RPAREN EQUAL 
    mid = ID LPAREN inputs = separated_list(COMMA, qualified_id) RPAREN
    { (outputs, mid, inputs) }

qualified_id:
  | id = ID { SimpleId id }
  | pid = ID DOT iid = ID { QualifiedId(pid, iid) }

(* party_template: partyと同じ構文で書ける *)
template_block:
  | PARTY_TEMPLATE tid = ID
    LEADER EQUAL lid = ID
    PERIODIC LPAREN ms = INT RPAREN
    instances = nonempty_list(newnode_stmt)
    {
      {
        template_id = tid;
        template_leader = lid;
        template_periodic = ms;
        template_instances = instances;
      }
    }
