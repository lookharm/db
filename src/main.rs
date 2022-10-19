use std::{io::{self}, process::exit};
#[macro_use] extern crate scan_fmt;

const COLUMN_USERNAME_SIZE: usize = 32;
const COLUMN_EMAIL_SIZE: usize = 255;
const  ID_SIZE: usize = 4;
const  USERNAME_SIZE: usize = 32;
const  EMAIL_SIZE: usize = 255;
// const  ID_OFFSET: usize = 0;
// const  USERNAME_OFFSET: usize = ID_OFFSET + ID_SIZE;
// const  EMAIL_OFFSET: usize = USERNAME_OFFSET + USERNAME_SIZE;
const  ROW_SIZE: usize = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROWS_PER_PAGE: u32 = PAGE_SIZE as u32 / ROW_SIZE as u32;
const TABLE_MAX_ROWS: u32 = ROWS_PER_PAGE as u32 * TABLE_MAX_PAGES as u32;

// TODO: Save rows in memeory like C, The Box?
// Current way we can not iter with only single loop like the tutorial:
// +  for (uint32_t i = 0; i < table->num_rows; i++) {
// +     deserialize_row(row_slot(table, i), &row);
// +     print_row(&row);
// +  }
// The page is a concept to reserve memory that fits with OS memory page but must not effect the way we query.
// Select the rows should not think about pages.

enum MetaCommandResult {
    Success,
    UnrecognizedCommand
}

enum PrepareResult {
    Success,
    SyntaxError,
    PrepareStringTooLong,
    PrepareNegativeId,
    UnrecognizedStatement
}

enum ExecuteResult{ 
    Success, 
    TableFull 
}

enum StatementType {
    Insert,
    Select
}

struct Table<'a> {
    num_rows: u32,
    pages: &'a mut Vec<Vec<Row>> // TODO:Use Box
}

#[derive(Clone)]
struct Row {
    id: u32,
    username: String,
    email: String
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Row
}



fn do_meta_command(input: &str) -> MetaCommandResult {
    if input.eq(".exit") {
        exit(0);
    } else {
        return MetaCommandResult::UnrecognizedCommand
    }
}

fn prepare_insert(input: &str, statement: &mut Statement) -> PrepareResult {
    statement.statement_type = StatementType::Insert;
    let (id,username,email) = scan_fmt_some!(input, "insert {} {} {}", i64, String, String);

    match id {
        Some(id) => {
            if id < 0 {
                return PrepareResult::PrepareNegativeId;
            }
            statement.row_to_insert.id = id as u32;
        }
        None => {
            return PrepareResult::SyntaxError;
        }
    }

    match username {
        Some(username) => {
            if username.len() > COLUMN_USERNAME_SIZE {
                return PrepareResult::PrepareStringTooLong;
            }
            statement.row_to_insert.username = username;
        }
        None => {
            return PrepareResult::SyntaxError;
        }
    }

    match email {
        Some(email) => {
            if email.len() > COLUMN_EMAIL_SIZE {
                return PrepareResult::PrepareStringTooLong;
            }
            statement.row_to_insert.email = email;
        }
        None => {
            return PrepareResult::SyntaxError;
        }
    }

    return PrepareResult::Success;
}

fn prepare_statement(input: &str, statement: &mut Statement) -> PrepareResult {
    if input.starts_with("insert") {
        return prepare_insert(input, statement);
    }
    if input.starts_with("select") {
        statement.statement_type = StatementType::Select;
        return PrepareResult::Success;
    }

    return PrepareResult::UnrecognizedStatement
}

fn execute_insert(statement: &Statement, table: &mut Table) -> ExecuteResult {
    if table.num_rows >= TABLE_MAX_ROWS {
        return ExecuteResult::TableFull;
    }

    // TODO: Copy trait
    let row = Row { 
        id: statement.row_to_insert.id, 
        username: statement.row_to_insert.username.clone(), 
        email: statement.row_to_insert.email.clone()
    };
    let page_num = table.num_rows / ROWS_PER_PAGE;
    
    if page_num >= table.pages.len() as u32 {
        table.pages.push(vec![Row { // TODO: Constructor
            id: 0, 
            username: String::new(), 
            email: String::new(),
        }; 0]);
    }
    table.pages[page_num as usize].push(row);
    table.num_rows += 1;
    return ExecuteResult::Success
}

fn execute_select(statement: &mut Statement, table: &mut Table) -> ExecuteResult {
    for page in table.pages.iter() { // TODO: Why need to use iter?
        for row in page {
            println!("({}, {}, {})", row.id, row.username, row.email);
        }
    }
    return ExecuteResult::Success
}

fn execute_statement(statement: &mut Statement, table: &mut Table) -> ExecuteResult {
    match statement.statement_type { 
        StatementType::Insert => {
            return execute_insert(statement, table);
        } 
        StatementType::Select =>{
            return execute_select(statement, table);
        }
    }
}

fn main() -> ! {
    let table = &mut Table { num_rows: 0, pages: &mut vec![vec![Row { // TODO: Constructor
        id: 0, 
        username: String::new(), 
        email: String::new(),
    }; 0]] };
    loop {
        println!("db > ");
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let input= input.trim();

        if input.starts_with(".") {
            match do_meta_command(input) {
                MetaCommandResult::Success => {
                    continue;
                }
                MetaCommandResult::UnrecognizedCommand => {
                    println!("Unrecognized command {input}");
                    continue;
                }
            }
        }

        let statement = &mut Statement{
            statement_type: StatementType::Insert, // TODO: Null value
            row_to_insert: Row { // TODO: Constructor
                id: 0, 
                username: String::new(), 
                email: String::new(),
            }
        };

        match prepare_statement(input, statement) {
            PrepareResult::Success => {}
            PrepareResult::UnrecognizedStatement => {
                println!("Unrecognized keyword at start of {input}.");
                continue;
            }
            PrepareResult::PrepareStringTooLong => {
                println!("String is too long.");
                continue;
            }
            PrepareResult::PrepareNegativeId => {
                println!("ID must be positive.");
                continue;
            }
            PrepareResult::SyntaxError => {
                println!("Syntax error. Could not parse statement.");
                continue;
            }
        }

        match execute_statement(statement, table) {
            ExecuteResult::Success => {
                println!("Executed.");
            }
            ExecuteResult::TableFull => {
                println!("Error: Table full.");
            }
        }
    }
}
