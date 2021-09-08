use crate::*;
use ::std::sync::mpsc::channel;

////////////////////////////////////////////////////////////////////////////////

pub fn get_sql ( cmd :&str ) -> Bresult<Vec<HashMap<String, String>>> {
    info!("SQLite <= \x1b[1;36m{}", cmd);

    let (snd, rcv) = channel::<HashMap<String, String>>();

    ::sqlite::open( "tmbot.sqlite" )?
    .iterate( cmd,
        move |ary_of_tuples|
            snd.send(
                ary_of_tuples.iter()
                .map( |(col,val)| (col.to_string(), val.unwrap_or("NULL").to_string()) )
                .collect())
            .map_err( |e| error!("get_sql snd.send => {:?}", e) )
            .is_ok() // Function needs to return a bool
        )?; // Must be used

    let sql = rcv.iter().collect();
    info!("SQLite => \x1b[36m{:?}", sql);

    Ok(sql)
}