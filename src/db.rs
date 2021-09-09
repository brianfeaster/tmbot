//use crate::*;
//use ::sqlite::*;

#[macro_export]
macro_rules! getsql {
    ( $cmd:expr ) => 
    {(|| -> Bresult<Vec<HashMap<String, String>>> {
        info!("SQLite <= \x1b[1;36m{}", $cmd);
        let connection = ::sqlite::open( "tmbot.sqlite" )?;
        let statement = connection.prepare( $cmd )?;
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
                        _             => "NULL".to_string()
                }))
                .collect())
        }
        info!("SQLite => \x1b[36m{:?}", rows);
        Bresult::Ok(rows)
    })()};

    ( $cmd:expr, $( $x:expr ),* ) =>
    {(|| -> Bresult<Vec<HashMap<String, String>>> {
        info!("SQLite <= \x1b[1;36m{}", $cmd);
        let connection = ::sqlite::open( "tmbot.sqlite" )?;
        let mut statement = connection.prepare( $cmd )?;
        let mut placeholderidx = 0;
        $(
            placeholderidx += 1;
            info!("SQLite    \x1b[1;36m{} {}", placeholderidx, $x);
            statement.bind(placeholderidx, $x)?;
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
                        _             => "NULL".to_string()
                }))
                .collect())
        }
        info!("SQLite => \x1b[36m{:?}", rows);
        Bresult::Ok(rows)
    })()};
}