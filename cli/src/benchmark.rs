//! A generic module for benchmarking SET/GET operations
//! **NOTE:** This is experimental and only uses a single connection. So any
//! benchmark comparisons you might do - aren't fair since it is likely
//! that most of them were done using parallel connections
#[cfg(test)]
#[test]
#[ignore]
fn benchmark_server() {
    const MAX_TESTS: usize = 1000000;
    use corelib::terrapipe::QueryBuilder;
    use devtimer::DevTime;
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};
    use std::io::{Read, Write};
    use std::net::TcpStream;
    let rand = thread_rng();
    let mut dt = DevTime::new_complex();
    let keys: Vec<String> = (0..MAX_TESTS)
        .into_iter()
        .map(|_| {
            let rand_string: String = rand.sample_iter(&Alphanumeric).take(32).collect();
            rand_string
        })
        .collect();
    let values: Vec<String> = (0..MAX_TESTS)
        .into_iter()
        .map(|_| {
            let rand_string: String = rand.sample_iter(&Alphanumeric).take(32).collect();
            rand_string
        })
        .collect();
    let set_packs: Vec<Vec<u8>> = (0..MAX_TESTS)
        .map(|idx| {
            let mut q = QueryBuilder::new_simple();
            q.add("SET");
            q.add(&keys[idx]);
            q.add(&values[idx]);
            q.prepare_response().1
        })
        .collect();
    let get_packs: Vec<Vec<u8>> = (0..MAX_TESTS)
        .map(|idx| {
            let mut q = QueryBuilder::new_simple();
            q.add("GET");
            q.add(&keys[idx]);
            q.prepare_response().1
        })
        .collect();
    let del_packs: Vec<Vec<u8>> = (0..MAX_TESTS)
        .map(|idx| {
            let mut q = QueryBuilder::new_simple();
            q.add("DEL");
            q.add(&keys[idx]);
            q.prepare_response().1
        })
        .collect();
    let mut con = TcpStream::connect("127.0.0.1:2003").unwrap();
    dt.create_timer("SET").unwrap();
    dt.start_timer("SET").unwrap();
    for packet in set_packs {
        con.write_all(&packet).unwrap();
        // We don't care about the return
        let _ = con.read(&mut vec![0; 1024]).unwrap();
    }
    dt.stop_timer("SET").unwrap();
    dt.create_timer("GET").unwrap();
    dt.start_timer("GET").unwrap();
    for packet in get_packs {
        con.write_all(&packet).unwrap();
        // We don't need the return
        let _ = con.read(&mut vec![0; 1024]).unwrap();
    }
    dt.stop_timer("GET").unwrap();

    // Delete all the created keys
    for packet in del_packs {
        con.write_all(&packet).unwrap();
        // We don't need the return
        let _ = con.read(&mut vec![0; 1024]).unwrap();
    }
    println!(
        "Time for {} SETs: {} ns",
        MAX_TESTS,
        dt.time_in_nanos("SET").unwrap()
    );
    println!(
        "Time for {} GETs: {} ns",
        MAX_TESTS,
        dt.time_in_nanos("GET").unwrap()
    );
}
