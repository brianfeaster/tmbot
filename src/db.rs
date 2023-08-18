use crate::util::Bresult;
pub use sqlite::{Statement};
use std::collections::HashMap;

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

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Connection{{{:?}}}", self.filename))
    }
}
////////////////////////////////////////////////////////////////////////////////
// SQLite macros that facilitate placeholders

#[macro_export]
macro_rules! getsql {
    ( $conn:expr, $sql:expr ) => { (|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        info!("SQLite <= {BLD_CYN}{}", $sql);
        let mut statement = $conn.conn.prepare( $sql )?;
        let col_names :Vec<String> =
            Statement::column_names(&statement).into_iter().map( String::from ).collect();
        let mut rows :Vec<HashMap<String, sqlite::Value>> = Vec::new();
        statement.iter().for_each(|items| {
            items.map(|vals| {
                let vv :Vec<sqlite::Value> = vals.into();
                rows.push(vv.iter()
                    .enumerate()
                    .map( |(i, row)| (col_names[i].clone(), row.clone()))
                    .collect());
            }).ok();
        });
        info!("SQLiteRaw -> {CYN}{:?}", rows);
        Ok(rows)
    })()};

    ( $conn:expr, $sql:expr, $($v:expr),* ) => { (|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        info!("SQLite {BLD_CYN}{}", $sql);
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut placeholderidx = 0;
        let mut info = String::new();
        $(
            placeholderidx += 1;
            info.push_str(&format!(" {}:{}", placeholderidx, $v));
            statement.bind((placeholderidx, $v))?;
        )*
        info!("SQLite{CYN}{}", info);
        let col_names :Vec<String> =
            Statement::column_names(&statement).into_iter().map( String::from ).collect();
        let mut rows :Vec<HashMap<String, sqlite::Value>> = Vec::new();
        statement.iter().for_each(|items| {
            items.map(|vals| {
                let vv :Vec<sqlite::Value> = vals.into();
                rows.push(vv.iter()
                    .enumerate()
                    .map( |(i, row)| (col_names[i].clone(), row.clone()))
                    .collect());
            }).ok();
        });
        info!("SQLite -> {CYN}{:?}", rows);
        Ok(rows)
    })() };
}

#[macro_export]
macro_rules! getsqlquiet {
    ( $conn:expr, $sql:expr, $($v:expr),* ) => {(|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut placeholderidx = 0;
        $(
            placeholderidx += 1;
            statement.bind((placeholderidx, $v))?;
        )*
        let col_names :Vec<String> = Statement::column_names(&statement).iter().map( |e| e.to_string() ).collect();
        let mut rows :Vec<HashMap<String, sqlite::Value>> = Vec::new();
        statement.iter().for_each(|items| {
            items.map(|vals| {
                let vv :Vec<sqlite::Value> = vals.into();
                rows.push(vv.iter()
                    .enumerate()
                    .map( |(i, row)| (col_names[i].clone(), row.clone()))
                    .collect());
            }).ok();
        });
        Ok(rows)
    })()};
}

////////////////////////////////////////////////////////////////////////////////
// Primitive traits for accessing row values

pub trait RowGet  {
    fn get_i64 (&self, key:&str) -> Bresult<i64>;
    fn get_f64 (&self, key:&str) -> Bresult<f64>;
    fn get_str (&self, key:&str) -> Bresult<&str>;
    fn get_string (&self, key:&str) -> Bresult<String>;
    fn get_i64_or    (&self, default:i64,  key:&str) -> i64;
    fn get_f64_or    (&self, default:f64,  key:&str) -> f64;
    fn get_string_or (&self, default:&str, key:&str) -> String;
    fn to_string (&self, key:&str) -> Bresult<String>;
}

impl RowGet for HashMap<String, ::sqlite::Value> {
    fn get_i64 (&self, key:&str) -> Bresult<i64> {
        Ok(self
            .get(key).ok_or(format!("Can't find key '{}'", key))?
            .try_into().map_err(|e| format!("Not an integer '{}' {:?}", key, e))?)
    }
    fn get_f64 (&self, key:&str) -> Bresult<f64> {
        self
            .get(key) /* Option */
            .ok_or(format!("Can't find key '{}'", key)) /* Result */
            ? /* sqlite::Value */
            .try_into() /* Option */
            .map_err(|e|format!("Not an integer '{}' {:?}", key, e).into()) /* Result */
    }
    fn get_str (&self, key:&str) -> Bresult<&str> {
        self
            .get(key).ok_or(format!("Can't find key '{}'", key))?
            .try_into().map_err(|e| format!("Not a string '{}' {:?}", key, e))
            .map_err( Box::from )
    }
    fn get_string (&self, key:&str) -> Bresult<String> {
        Ok(self
            .get(key).ok_or(format!("Can't find key '{}'", key))?
            .try_into::<&str>().map_err(|e| format!("Not a string '{}' {:?}", key, e))?
            .to_string())
    }
    fn get_i64_or (&self, default:i64, key:&str) -> i64 {
        self.get(key).and_then( |v| sqlite::Value::try_into::<i64>(v).ok() ).unwrap_or(default)
    }
    fn get_f64_or (&self, default:f64, key:&str) -> f64 {
        self.get(key).and_then( |v| ::sqlite::Value::try_into(v).ok() ).unwrap_or(default)
    }
    fn get_string_or (&self, default:&str, key:&str) -> String {
        self.get(key).and_then( |v| ::sqlite::Value::try_into(v).ok() ).unwrap_or(default).to_string()
    }
    fn to_string (&self, key:&str) -> Bresult<String> {
        let v = self.get(key).ok_or(format!("Can't find key '{}'", key))?; /* sqlite::Value */
        v.try_into::<&str>()
        .map( String::from )
        .or( v.try_into().map( |i: i64| i.to_string() ) )
        .or( v.try_into().map( |f: f64| f.to_string() ) )
        .or( Err("Can't stringify".into()))
    }
}
