//use crate::*;
//pub use sqlite::*;
//use ::std::sync::Arc;
use crate::util::Bresult;

pub struct Connection {
    pub filename: String,
    pub conn: ::sqlite::Connection
}

impl Connection {
    pub fn new (filename:&str) -> Bresult<Self> {
        let conn = ::sqlite::open(filename.to_string())? ;
        Ok(Connection {filename:filename.to_string(), conn})
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Connection{{{:?}}}", self.filename))
    }
}

#[macro_export]
macro_rules! getsql {
    ( $conn:expr, $sql:expr ) => {(|| -> Bresult<Vec<HashMap<String, String>>> {
        info!("SQLite <= \x1b[1;36m{}", $sql);
        let statement = $conn.conn.prepare( $sql )?;
        // No place holders to bind
        let col_names = ::sqlite::Statement::column_names(&statement).iter().map( |e| e.to_string() ).collect::<Vec<String>>();
        let mut cursor = ::sqlite::Statement::into_cursor(statement);
        let mut rows :Vec<HashMap<String,String>> = Vec::new();
        while let Some(vals) = cursor.next()? {
            rows.push(
                vals.iter().enumerate()
                .map( |(i, val)| (
                    col_names[i].to_string(), 
                    match val.kind() {
                        ::sqlite::Type::String  => val.as_string().unwrap().to_string(),
                        ::sqlite::Type::Float   => val.as_float().unwrap().to_string(),
                        ::sqlite::Type::Integer => val.as_integer().unwrap().to_string(),
                                              _ => "NULL".to_string()
                     } ) )
                .collect() ) }
        info!("SQLite => \x1b[36m{:?}", rows);
        Bresult::Ok(rows)
    })()};

    ( $conn:expr, $sql:expr, $($v:expr),* ) => {(|| -> Bresult<Vec<HashMap<String, String>>> {
        info!("SQLite <= \x1b[1;36m{}", $sql);
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut placeholderidx = 0;
        $(
            placeholderidx += 1;
            info!("SQLite    \x1b[36m{} {}", placeholderidx, $v);
            statement.bind(placeholderidx, $v)?;
        )*
        let col_names = ::sqlite::Statement::column_names(&statement).iter().map( |e| e.to_string() ).collect::<Vec<String>>();
        let mut cursor = ::sqlite::Statement::into_cursor(statement);
        let mut rows :Vec<HashMap<String,String>> = Vec::new();
        while let Some(vals) = cursor.next()? {
            rows.push(
                vals.iter().enumerate()
                .map( |(i, val)|
                    (col_names[i].to_string(), 
                    match val.kind() {
                        ::sqlite::Type::String  => val.as_string().unwrap().to_string(),
                        ::sqlite::Type::Float   => val.as_float().unwrap().to_string(),
                        ::sqlite::Type::Integer => val.as_integer().unwrap().to_string(),
                                              _ => "NULL".to_string()
                }))
                .collect())
        }
        info!("SQLite => \x1b[36m{:?}", rows);
        Bresult::Ok(rows)
    })()};
}

#[macro_export]
macro_rules! getsqlquiet {
    ( $conn:expr, $sql:expr, $($v:expr),* ) => {(|| -> Bresult<Vec<HashMap<String, String>>> {
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut placeholderidx = 0;
        $(
            placeholderidx += 1;
            statement.bind(placeholderidx, $v)?;
        )*
        let col_names = ::sqlite::Statement::column_names(&statement).iter().map( |e| e.to_string() ).collect::<Vec<String>>();
        let mut cursor = ::sqlite::Statement::into_cursor(statement);
        let mut rows :Vec<HashMap<String,String>> = Vec::new();
        while let Some(vals) = cursor.next()? {
            rows.push(
                vals.iter().enumerate()
                .map( |(i, val)| (
                    col_names[i].to_string(), 
                    match val.kind() {
                        ::sqlite::Type::String  => val.as_string().unwrap().to_string(),
                        ::sqlite::Type::Float   => val.as_float().unwrap().to_string(),
                        ::sqlite::Type::Integer => val.as_integer().unwrap().to_string(),
                                              _ => "NULL".to_string()
                     } ) )
                .collect() ) }
        Bresult::Ok(rows)
    })()};
}