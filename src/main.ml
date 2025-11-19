open Lexing
open Util

exception CommandError of string
exception CompileError of string
exception FunctionError of string

type mode =
  Erlang | Dot

let extension_len = String.length "xfrp"

let output_file = ref None
let module_files  = ref []
let instance_file_ref = ref None
let stdout_flag = ref false
let mode = ref Erlang

let speclist = [
  ("-o", Arg.String(fun s -> output_file := Some(s)), " [file] Write output to file.");
  ("-inst", Arg.String(fun s -> instance_file_ref := Some(s)), " [file] Instance file.");
  ("-std", Arg.Unit(fun _ -> stdout_flag := true), " [Output to stdout.");
  ("-dot", Arg.Unit(fun _ -> mode := Dot), "Output the dependency graph.");
]

let load_file f =
  let ic = open_in f in
  let n = in_channel_length ic in
  let s = Bytes.create n in
  really_input ic s 0 n;
  close_in ic;
  (s)

(* parse a mpfrp module file *)
let create_module in_c filename =
  let lexbuf = from_channel in_c in
    try
      let program = Parser.prog_module Lexer.read lexbuf in
        Module.of_program program
    with 
      | Lexer.Error msg ->
          raise (CompileError(Printf.sprintf "[%s] Lexing error: %s" filename msg))
      | Syntax.InvalidId(id) ->
          let pos = lexbuf.lex_curr_p in
            raise (CompileError(Printf.sprintf "[%s] Id \"%s\" is reserved at Line %d, Char %d." filename id pos.pos_lnum (pos.pos_cnum - pos.pos_bol + 1)))
      | Parser.Error ->
          let pos = lexbuf.lex_curr_p in
            raise (CompileError(Printf.sprintf "[%s] Syntax error at Line %d, Char %d." filename pos.pos_lnum (pos.pos_cnum - pos.pos_bol + 1)))

(* parse a mpfrp instance file *)
let create_inst in_c filename =
  let lexbuf = from_channel in_c in
    try
      Parser.prog_inst Lexer.read lexbuf
    with 
      | Lexer.Error msg ->
          raise (CompileError(Printf.sprintf "[%s] Lexing error: %s" filename msg))
      | Syntax.InvalidId(id) ->
          let pos = lexbuf.lex_curr_p in
            raise (CompileError(Printf.sprintf "[%s] Id \"%s\" is reserved at Line %d, Char %d." filename id pos.pos_lnum (pos.pos_cnum - pos.pos_bol + 1)))
      | Parser.Error ->
          let pos = lexbuf.lex_curr_p in
            raise (CompileError(Printf.sprintf "[%s] Syntax error at Line %d, Char %d." filename pos.pos_lnum (pos.pos_cnum - pos.pos_bol + 1)))

(* peek at first token to determine file type *)
let peek_file_type filename =
  let ic = open_in filename in
  let lexbuf = from_channel ic in
  let rec skip_whitespace () =
    match Lexer.read lexbuf with
    | Parser.MODULE -> close_in ic; `Module
    | Parser.PARTY -> close_in ic; `Party
    | Parser.PARTY_TEMPLATE -> close_in ic; `PartyTemplate
    | Parser.EOF -> close_in ic; raise (CompileError("Empty file"))
    | _ -> skip_whitespace ()
  in
  try
    skip_whitespace ()
  with
  | Lexer.Error msg -> close_in ic; raise (CompileError("Lexing error: " ^ msg))
  | _ -> close_in ic; raise (CompileError("Unable to determine file type"))

(* generate a code of instance module from module_map *)
let compile inst_mod module_map =
  try
    match !mode with
      | Erlang ->
        Codegen.gen_inst inst_mod module_map
      | Dot ->
        Graphviz.of_xmodule inst_mod module_map
    with
      | Codegen.NodeLoop(loops) ->
        raise (CompileError("Loop detected: " ^
          String.concat ", " (List.map (fun (party, loop) ->
              String.concat " -> " (loop @ [List.hd loop]) ^ " of party " ^ party
            ) loops)
          ))
      | Codegen.PartyLoop(loops) ->
        raise (CompileError("Loop detected: " ^
          String.concat ", " (List.map (fun loop ->
            String.concat " -> " (loop @ [List.hd loop])
          ) loops)))
      | UnknownId(id) ->
        raise (CompileError("Not found id \"" ^ id ^ "\""))
      | Typing.TypeError s ->
        raise (CompileError("Type error: " ^ s))
      | Dependency.InvalidAtLast ss ->
        raise (CompileError("Invalid usage of @last: \n" ^ String.concat "\n" (List.map (fun s -> "\t" ^ s) ss)  ))
      | Party.LeaderError party ->
          raise (CompileError("Leader must be one: " ^ party))
      | Party.ReachableError (mod1, mod2, p) ->
          raise (CompileError(mod1 ^ " and " ^ mod2 ^ " must be connected via party " ^ p ^ "."))

(* generate code from new inst_program structure - NOT IMPLEMENTED YET *)
(* let compile_new inst_prog module_map =
  raise (CompileError("party/party_template syntax not yet implemented. Use old module syntax.")) *)

let () =
  Arg.parse speclist (fun s -> module_files := s :: !module_files) "Usage:";
  module_files := List.rev !module_files;
  let instance_file =
    match !instance_file_ref with
      Some s -> s
      | None -> raise (CommandError("Specify an instance file."))
  in
    let output_path =
      let filename =
        match (List.rev (String.split_on_char '/' instance_file)) with
          body :: _ -> body
          | [] -> raise (FunctionError "Error in split_on_char")
      in
        let body = String.sub filename 0 (String.length filename - extension_len) in
          "erlang/" ^ body ^ "erl"
    in
      if !module_files = [] then raise (CommandError("Specify an input file."))
      else
        try
          let module_map =
            List.fold_left (fun m program ->
                              let input = open_in program in
                                let module_info = create_module input program in
                                  close_in input;
                                  M.add module_info.id module_info m)
                            M.empty
                            !module_files
          in
          (* Determine instance file type and parse accordingly *)
          let result = 
            match peek_file_type instance_file with
            | `Module ->
                (* Old format: instance file is also a module *)
                let input = open_in instance_file in
                let inst_module = create_module input instance_file in
                close_in input;
                compile inst_module module_map
            | `Party | `PartyTemplate ->
                (* New format: instance file with party/party_template blocks *)
                let input = open_in instance_file in
                let inst_prog = create_inst input instance_file in
                close_in input;
                Codegen_new.gen_new_inst inst_prog module_map
          in
            if !mode = Dot then print_string result
            else if !stdout_flag then print_string result
            else begin
              let oc = open_out output_path in
                output_string oc result;
                close_out oc
            end
      with
        | CommandError msg -> Printf.eprintf "Command Error: %s\n" msg;
        | CompileError msg -> Printf.eprintf "%s\n" msg;
        | e -> 
            Printf.eprintf "Unexpected error: %s\n" (Printexc.to_string e);
            Printexc.print_backtrace stderr;
            flush stderr

(* only parsing *)
(*
open Lexing
let () =
  let expr = Parser.prog_module Lexer.read (Lexing.from_channel stdin) in
    print_string "Parsed\n";
    Syntax.print expr
*)
