use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;

use rusqlite::{Connection, functions::FunctionFlags};

const BYTES_PER_VEC: usize = 512 * 4; // 512 packed f32s

fn gen_vec(rng: &mut SmallRng) -> [f32; 512] {
    let mut v = [0f32; 512];
    rng.fill(&mut v);
    v
}

fn gen_vecs(rng: &mut SmallRng, count: usize) -> Vec<[f32; 512]> {
    let mut output = vec![];
    for _ in 0..count {
        output.push(gen_vec(rng));
    }
    output
}

fn do_dots(v: [f32; 512], vecs: Vec<[f32; 512]>) -> Vec<f32> {
    let mut output = vec![];
    for v1 in vecs {
        let dot: f32 = v.iter().zip(v1.iter()).map(|(e, e1)| e*e1).sum();
        output.push(dot);
    }
    output
}

fn open_db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.create_scalar_function(
        "dot",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let v1 = ctx.get::<[u8; BYTES_PER_VEC]>(0)?;
            let v2 = ctx.get::<[u8; BYTES_PER_VEC]>(1)?;

            let v1 = unsafe { std::mem::transmute::<_, [f32; 512]>(v1) };
            let v2 = unsafe { std::mem::transmute::<_, [f32; 512]>(v2) };

            Ok(v1.iter().zip(v2.iter()).map(|(e1, e2)| e1*e2).sum::<f32>())
        },
    ).unwrap();

    conn.execute("CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY, v BLOB);", [])
        .unwrap();

    conn
}

fn insert_data(conn: &mut Connection, data: &[[f32; 512]]) {
    let tx = conn.transaction().unwrap();
    {
        let mut stmt = tx.prepare_cached("INSERT INTO data (v) VALUES (?)").unwrap();
        for v in data {
            let v = unsafe { std::mem::transmute::<_, [u8; BYTES_PER_VEC]>(*v) };
            stmt.execute([v.as_slice(),]).unwrap();
        }
    }
    tx.commit().unwrap()
}

fn do_dots_sql(conn: &Connection, v: [f32; 512]) -> Vec<f32> {
    let v = unsafe { std::mem::transmute::<_, [u8; BYTES_PER_VEC]>(v) };
    let mut stmt = conn.prepare("SELECT dot(data.v, ?) AS d FROM data ORDER BY d DESC LIMIT 5;").unwrap();
    let rows = stmt.query_map([v.as_slice(),], |row| row.get(0)).unwrap();

    let mut dots = vec![];
    for d in rows {
        dots.push(d.unwrap());
    }

    dots
}

fn main() {
    let mut rng = rand::rngs::SmallRng::from_entropy();
    let vecs = gen_vecs(&mut rng, 1_000_000);
    let v = gen_vec(&mut rng);

    let mut db = open_db();

    insert_data(&mut db, &vecs);

    let start_local = std::time::Instant::now();
    let local_dots = do_dots(v, vecs);
    let local_t = start_local.elapsed();

    let start_db = std::time::Instant::now();
    let db_dots = do_dots_sql(&db, v);
    let db_t = start_db.elapsed();


    println!("local {}ms", local_t.as_millis());
    println!("db {}ms", db_t.as_millis());
    println!("max local {}", local_dots.iter().fold(0f32, |f1, f2| f1.max(*f2)));
    println!("max db {}", db_dots[0]);
}
