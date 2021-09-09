use crate::*;
use ::sqlite::*;

////////////////////////////////////////////////////////////////////////////////

pub fn get_sql ( cmd :&str ) -> Bresult<Vec<HashMap<String, String>>> {
    info!("SQLite <= \x1b[1;36m{}", cmd);
    let connection = open( "tmbot.sqlite" )?;
    let statement = connection.prepare(cmd)?;
    let col_names = statement.column_names().iter().map( |e| e.to_string() ).collect::<Vec<String>>();
    let mut cursor = statement.into_cursor();
    let mut rows = Vec::new();
    while let Some(vals) = cursor.next()? {
        rows.push(
            vals.iter().enumerate()
            .map( |(i, val)|
                (col_names[i].to_string(), 
                 match val.kind() {
                    Type::String  => val.as_string().unwrap().to_string(),
                    Type::Float   => val.as_float().unwrap().to_string(),
                    Type::Integer => val.as_integer().unwrap().to_string(),
                    _             => "NULL".to_string()
            }))
            .collect())
    }
    info!("SQLite => \x1b[36m{:?}", rows);
    Ok(rows)
}