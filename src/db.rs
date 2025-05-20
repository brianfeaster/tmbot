use crate::*;


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

pub type Table = Vec<HashMap<String, sqlite::Value>>;

pub trait WithConn {
  fn withConn (&self, f:impl Fn(&Connection)->Bresult<Table>) -> Bresult<Table>;
}

pub fn statementToTable (stmt: ::sqlite::Statement) -> Table {
    stmt.into_iter()
    .filter_map(|resrow|
        resrow.map(|row|
            row.iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect())
        .ok())
    .collect()
}

#[macro_export]
macro_rules! getsql2 {
    ( $dbc:expr, $sql:expr, $($v:expr),* ) => {
        $dbc.withConn( |conn: &Connection| {
            info!("SQLite {BLD_CYN}{}{RST}", $sql);
            let mut statement = conn.conn.prepare( $sql )?;
            let mut idx = 0;
            let mut args = String::new();
            $(
                idx += 1;
                args.push_str(&format!(" ({}, {})", idx, $v));
                statement.bind((idx, $v))?;
            )*
            let rows = statementToTable(statement);
            info!("SQLite{CYN}{} {RED}{:?}", args, rows);
            info!("SQLite -> {CYN}{:?}", rows);
            Ok(rows)
        })
    };
}

#[macro_export]
macro_rules! getsql {
    ( $conn:expr, $sql:expr ) => { (|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        info!("SQLite <= {BLD_CYN}{}{RST}", $sql);
        let statement = $conn.conn.prepare( $sql )?;
        let rows = statementToTable(statement);
        info!("SQLiteRaw -> {CYN}{:?}", rows);
        Ok(rows)
    })()};

    ( $conn:expr, $sql:expr, $($v:expr),* ) => { (|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        info!("SQLite {BLD_CYN}{}", $sql);
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut idx = 0;
        let mut args = String::new();
        $(
            idx += 1;
            args.push_str(&format!(" ({}, {})", idx, $v));
            statement.bind((idx, $v))?;
        )*
        info!("SQLite{CYN}{}", args);
        let rows = statementToTable(statement);
        info!("SQLite -> {CYN}{:?}", rows);
        Ok(rows)
    })() };
}

#[macro_export]
macro_rules! getsqlquiet {
    ( $conn:expr, $sql:expr, $($v:expr),* ) => {(|| -> Bresult<Vec<HashMap<String, sqlite::Value>>> {
        let mut statement = $conn.conn.prepare( $sql )?;
        let mut idx = 0;
        $(
            idx += 1;
            statement.bind((idx, $v))?;
        )*
        Ok(statementToTable(statement))
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
