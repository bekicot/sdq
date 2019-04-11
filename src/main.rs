#[macro_use]
extern crate clap;

use std::str;
use std::fs;

use clap::App;
use quick_xml::Reader;
use quick_xml::events::{Event};

struct DBValues {
    keys: Vec<String>,
    values: Vec<String>
}

use rusqlite::{Connection, NO_PARAMS};

fn main() {
    let yaml = load_yaml!("cli.yml");
    let matches = App::from_yaml(yaml).get_matches();

    if let Some(format) = matches.subcommand_matches("export") {
        let conn = Connection::open(format.value_of("output").unwrap()).unwrap();
        let xml = fs::read_to_string(format.value_of("input_xml").unwrap()).expect("Unable to read file");

        let mut reader = Reader::from_str(&xml);
        reader.trim_text(true);

        let mut buf = Vec::new();
        let mut current_table = "users".to_string();
        loop {
            match reader.read_event(&mut buf) {
                Ok(Event::Empty(ref e)) => {
                    match e.name() {
                        b"row" => {
                          let mut attr_values: DBValues = DBValues {
                              keys: Vec::new(),
                              values: Vec::new()
                          };
                          e.attributes().map(|a| {
                              let attr = a.unwrap();
                              attr_values.keys.push(String::from_utf8(attr.key.to_vec()).unwrap());
                              attr_values.values.push(String::from_utf8(attr.value.to_vec()).unwrap());
                          }).collect::<Vec<_>>();
                          insert_into_table(&conn, &mut current_table, &mut attr_values);
                        },
                        _ => ()
                    }
                },
                Ok(Event::Start(ref e)) => {
                    current_table = String::from_utf8(e.name().to_vec()).unwrap();
                    match e.name() {
                        b"badges" => create_badge_table(&conn),
                        b"comments" => create_comment_table(&conn),
                        b"postswithdeleted" => create_postswithdeleted_table(&conn),
                        b"users" => create_user_table(&conn),
                        b"votes" => create_vote_table(&conn),
                        table => {
                          println!("exporting {} is not supported", str::from_utf8(table).unwrap())
                        }
                    }
                }
                Ok(Event::Eof) => break,
                Err(e) => panic!("Error at position {}: {:?}", reader.buffer_position(), e),
                _ => (),
            }
            buf.clear();
        }
    }
}

fn create_badge_table(conn: &Connection) {
    conn.execute(&"DROP TABLE IF EXISTS badges".to_string(), NO_PARAMS).unwrap();
    conn.execute(&"
    CREATE TABLE badges(
      Id INT,
      UserId INT,
      Name TEXT,
      Date TEXT,
      Class TEXT,
      TagBased TEXT
    )".to_string(), NO_PARAMS).unwrap();
}

fn create_comment_table(conn: &Connection) {
    conn.execute(&"DROP TABLE IF EXISTS comments".to_string(), NO_PARAMS).unwrap();
    conn.execute(&"
    CREATE TABLE comments(
      Id INT,
      PostId INT,
      Score TEXT,
      Text TEXT,
      CreationDate TEXT,
      UserId INT,
      UserDisplayName TEXT
    )".to_string(), NO_PARAMS).unwrap();
}

fn create_postswithdeleted_table(conn: &Connection) {
    conn.execute(&"DROP TABLE IF EXISTS postswithdeleted".to_string(), NO_PARAMS).unwrap();
    conn.execute(&"
    CREATE TABLE postswithdeleted(
      Id INT,
      PostTypeId TEXT,
      AcceptedAnswerId TEXT,
      CreationDate TEXT,
      Score TEXT,
      ViewCount INT,
      Body TEXT,
      OwnerUserId INT,
      LastEditorUserId TEXT,
      LastEditDate TEXT,
      LastActivityDate TEXT,
      Title TEXT,
      Tags TEXT,
      AnswerCount INT,
      CommentCount INT,
      FavoriteCount INT,
      ParentId INT,
      ClosedDate TEXT,
      CommunityOwnedDate TEXT,
      OwnerDisplayName TEXT,
      LastEditorDisplayName TEXT,
      DeletionDate TEXT
    )".to_string(), NO_PARAMS).unwrap();
}

fn create_user_table(conn: &Connection) {
    conn.execute(&"DROP TABLE IF EXISTS users".to_string(), NO_PARAMS).unwrap();
    conn.execute(&"
    CREATE TABLE users(
      Id INT,
      Reputation TEXT,
      CreationDate TEXT,
      DisplayName TEXT,
      LastAccessDate TEXT,
      Location TEXT,
      AboutMe TEXT,
      Views TEXT,
      UpVotes TEXT,
      DownVotes TEXT,
      Age TEXT,
      AccountId TEXT,
      WebsiteUrl TEXT,
      ProfileImageUrl TEXT
    )".to_string(), NO_PARAMS).unwrap();
}

fn create_vote_table(conn: &Connection) {
    conn.execute(&"DROP TABLE IF EXISTS votes".to_string(), NO_PARAMS).unwrap();
    conn.execute(&"
    CREATE TABLE votes(
      Id INT,
      PostId INT,
      VoteTypeId TEXT,
      CreationDate TEXT,
      UserId INT,
      BountyAmount INT
    )".to_string(), NO_PARAMS).unwrap();
}

fn insert_into_table(conn: &Connection, table_name: &mut String, db_attr: &mut DBValues) {
    let mut query = "".to_string();

    query.push_str("INSERT INTO");
    query.push_str(" ");
    query.push_str(table_name);
    query.push_str(" ");
    query.push_str("(");

    for key in &mut db_attr.keys {
        query.push_str(key);
        query.push_str(",");
    }
    // Remove last comma
    query.pop();
    query.push_str(")");
    query.push_str(" ");
    query.push_str("values");
    query.push_str(" ");
    query.push_str("(");

    for i in 1..(db_attr.values.len() + 1) {
        query.push_str("?");
        query.push_str(&i.to_string());
        query.push_str(",");
    }
    // Remove last comma
    query.pop();
    query.push_str(")");
    // println!("{:?}", query);
    conn.execute(&query, &db_attr.values).unwrap();
}
