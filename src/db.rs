use crate::*;

use ::std::{
    sync::{mpsc::{channel, Sender} }
};

////////////////////////////////////////////////////////////////////////////////

fn sql_results_handler_ (
    snd :Sender<HashMap<String, String>>,
    res :&[(&str, Option<&str>)] // [ (column, value) ]
) -> bool {
    let mut v = HashMap::new();
    for r in res {
        trace!("sql_results_handler vec <- {:?}", r);
        v.insert( r.0.to_string(), r.1.unwrap_or("NULL").to_string() );
    }
    let res = snd.send(v);
    trace!("sql_results_handler snd <- {:?}", res);
    true
}

fn get_sql_ ( cmd :&str ) -> Bresult<Vec<HashMap<String, String>>> {
    info!("SQLite <= \x1b[1;36m{}", cmd);
    let sql = ::sqlite::open( "tmbot.sqlite" )?;
    let (snd, rcv) = channel::<HashMap<String, String>>();
    sql.iterate(cmd, move |r| sql_results_handler_(snd.clone(), r) )?;
    Ok(rcv.iter().collect::<Vec<HashMap<String,String>>>())
}

////////////////////////////////////////////////////////////////////////////////

pub fn get_sql ( cmd :&str ) -> Bresult<Vec<HashMap<String, String>>> {
    let sql = get_sql_(cmd);
    info!("SQLite => \x1b[36m{:?}", sql);
    sql
}