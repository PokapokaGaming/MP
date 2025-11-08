open Lexing

let test_module_parser filename =
  let ic = open_in filename in
  let lexbuf = Lexing.from_channel ic in
  try
    let program = Parser.prog_module Lexer.read lexbuf in
    close_in ic;
    print_endline ("Successfully parsed module file: " ^ filename);
    Syntax.print program
  with
  | Lexer.Error msg ->
      close_in ic;
      Printf.eprintf "Lexing error: %s\n" msg
  | Parser.Error ->
      close_in ic;
      let pos = lexbuf.lex_curr_p in
      Printf.eprintf "Syntax error at Line %d, Char %d.\n" 
        pos.pos_lnum (pos.pos_cnum - pos.pos_bol + 1)

let test_inst_parser filename =
  let ic = open_in filename in
  let lexbuf = Lexing.from_channel ic in
  try
    let program = Parser.prog_inst Lexer.read lexbuf in
    close_in ic;
    print_endline ("Successfully parsed inst file: " ^ filename);
    Printf.printf "Parties: %d, Templates: %d\n" 
      (List.length program.Syntax.parties)
      (List.length program.Syntax.templates);
    List.iter (fun p ->
      Printf.printf "  Party: %s, Leader: %s, Period: %dms, Instances: %d\n"
        p.Syntax.party_id p.Syntax.leader p.Syntax.periodic_ms
        (List.length p.Syntax.instances)
    ) program.Syntax.parties;
    List.iter (fun t ->
      Printf.printf "  Template: %s, Defs: %d, Connections: %d\n"
        t.Syntax.template_id
        (List.length t.Syntax.template_defs)
        (List.length t.Syntax.default_connections)
    ) program.Syntax.templates
  with
  | Lexer.Error msg ->
      close_in ic;
      Printf.eprintf "Lexing error: %s\n" msg
  | Parser.Error ->
      close_in ic;
      let pos = lexbuf.lex_curr_p in
      Printf.eprintf "Syntax error at Line %d, Char %d.\n" 
        pos.pos_lnum (pos.pos_cnum - pos.pos_bol + 1)

let () =
  if Array.length Sys.argv < 3 then
    Printf.eprintf "Usage: %s <module|inst> <file>\n" Sys.argv.(0)
  else
    match Sys.argv.(1) with
    | "module" -> test_module_parser Sys.argv.(2)
    | "inst" -> test_inst_parser Sys.argv.(2)
    | _ -> Printf.eprintf "Invalid mode: %s (use 'module' or 'inst')\n" Sys.argv.(1)
