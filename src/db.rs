use ::std::{
    fmt,
    collections::{HashMap}};
pub use sqlite::{Statement};
//use crate::*;
use crate::util::Bresult;

////////////////////////////////////////////////////////////////////////////////
pub struct Connection {
    pub conn: ::sqlite::Connection,
    pub filename: String
}

impl Connection {
    pub fn new (filename:String) -> Bresult<Self> {
        ::sqlite::open(&filename)
        .map( |conn| Connection{conn, filename} )
        .map_err( Box::from )
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Connection{{{:?}}}", self.filename))
    }
}
////////////////////////////////////////////////////////////////////////////////
// SQLite macros that facilitate placeholders
////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! getsql {
    ( $conn:expr, $sql:expr ) => { (|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        info!("SQLite <= \x1b[1;36m{}", $sql);
        let statement = $conn.conn.prepare( $sql )?;
        let col_names :Vec<String> =
            Statement::column_names(&statement).into_iter().map( String::from ).collect();
        let mut cursor = Statement::into_cursor(statement);
        let mut rows :Vec<HashMap<String, sqlite::Value>> = Vec::new();
        while let Some(vals) = cursor.next()? {
            rows.push(vals.iter().enumerate()
                .map( |(i, v)| (col_names[i].clone(), v.clone()) )
                .collect() )
        }
        info!("SQLiteRaw => \x1b[36m{:?}", rows);
        Bresult::Ok(rows)
    })()};

    ( $conn:expr, $sql:expr, $($v:expr),* ) => { (|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        info!("SQLite <= \x1b[1;36m{}", $sql);
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut placeholderidx = 0;
        $(
            placeholderidx += 1;
            info!("SQLite    \x1b[36m{} {}", placeholderidx, $v);
            statement.bind(placeholderidx, $v)?;
        )*
        let col_names :Vec<String> =
            Statement::column_names(&statement).into_iter().map( String::from ).collect();
        let mut cursor = Statement::into_cursor(statement);
        let mut rows :Vec<HashMap<String, sqlite::Value>> = Vec::new();
        while let Some(vals) = cursor.next()? {
            rows.push(vals.iter().enumerate()
                .map( |(i, v)| (col_names[i].clone(), v.clone()) )
                .collect() )
        }
        info!("SQLite => \x1b[36m{:?}", rows);
        Bresult::Ok(rows)
    })() };
}

#[macro_export]
macro_rules! getsqlquiet {
    ( $conn:expr, $sql:expr, $($v:expr),* ) => {(|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut placeholderidx = 0;
        $(
            placeholderidx += 1;
            statement.bind(placeholderidx, $v)?;
        )*
        let col_names :Vec<String> = Statement::column_names(&statement).iter().map( |e| e.to_string() ).collect();
        let mut cursor = Statement::into_cursor(statement);
        let mut rows :Vec<HashMap<String, sqlite::Value>> = Vec::new();
        while let Some(vals) = cursor.next()? {
            rows.push(vals.iter().enumerate()
                .map( |(i, v)| (col_names[i].clone(), v.clone()) )
                .collect() )
        }
        Bresult::Ok(rows)
    })()};
}

////////////////////////////////////////////////////////////////////////////////
// Primitive traits for accessing row values
////////////////////////////////////////////////////////////////////////////////

pub trait GetI64    { fn get_i64 (&self, key:&str) -> Bresult<i64>; }
pub trait GetF64    { fn get_f64 (&self, key:&str) -> Bresult<f64>; }
pub trait GetStr    { fn get_str (&self, key:&str) -> Bresult<&str>; }
pub trait GetString { fn get_string (&self, key:&str) -> Bresult<String>; }

impl GetI64 for HashMap<String, ::sqlite::Value> {
    fn get_i64 (&self, key:&str) -> Bresult<i64> {
        Ok(self
            .get(key).ok_or(format!("Can't find key '{}'", key))?
            .as_integer().ok_or(format!("Not an integer '{}'", key))?)
    }
}

impl GetF64 for HashMap<String, ::sqlite::Value> {
    fn get_f64 (&self, key:&str) -> Bresult<f64> {
        self
            .get(key) /* Option */
            .ok_or(format!("Can't find key '{}'", key)) /* Result */
            ? /* sqlite::Value */
            .as_float() /* Option */
            .ok_or(format!("Not an integer '{}'", key).into()) /* Result */
    }
}

impl GetStr for HashMap<String, ::sqlite::Value> {
    fn get_str (&self, key:&str) -> Bresult<&str> {
        self
            .get(key).ok_or(format!("Can't find key '{}'", key))?
            .as_string().ok_or(format!("Not a string '{}'", key))
            .map_err( Box::from )
    }
}

impl GetString for HashMap<String, ::sqlite::Value> {
    fn get_string (&self, key:&str) -> Bresult<String> {
        Ok(self
            .get(key).ok_or(format!("Can't find key '{}'", key))?
            .as_string().ok_or(format!("Not a string '{}'", key))?
            .to_string())
    }
}

////////////////////////////////////////

pub trait GetI64Or    { fn get_i64_or    (&self, default:i64,  key:&str) -> i64; }
pub trait GetF64Or    { fn get_f64_or    (&self, default:f64,  key:&str) -> f64; }
pub trait GetStringOr { fn get_string_or (&self, default:&str, key:&str) -> String; }

impl GetI64Or for HashMap<String, ::sqlite::Value> {
    fn get_i64_or (&self, default:i64, key:&str) -> i64 {
        self.get(key).and_then( ::sqlite::Value::as_integer ).unwrap_or(default)
    }
}

impl GetF64Or for HashMap<String, ::sqlite::Value> {
    fn get_f64_or (&self, default:f64, key:&str) -> f64 {
        self.get(key).and_then( ::sqlite::Value::as_float ).unwrap_or(default)
    }
}

impl GetStringOr for HashMap<String, ::sqlite::Value> {
    fn get_string_or (&self, default:&str, key:&str) -> String {
        self.get(key).and_then( ::sqlite::Value::as_string ).unwrap_or(default).to_string()
    }
}

////////////////////////////////////////

pub trait ToString { fn to_string (&self, key:&str) -> Bresult<String>; }

impl ToString for HashMap<String, ::sqlite::Value> {
    fn to_string (&self, key:&str) -> Bresult<String> {
        let v = self.get(key).ok_or(format!("Can't find key '{}'", key))?; /* sqlite::Value */
        v.as_string()
        .map( String::from )
        .or( v.as_integer().map( |i| i.to_string() ) )
        .or( v.as_float().map( |f| f.to_string() ) )
        .ok_or( "Can't stringify".into())
    }
}