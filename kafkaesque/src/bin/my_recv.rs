extern crate timely;
extern crate timely_communication;

extern crate rdkafka;
extern crate kafkaesque;
extern crate hostname;

use std::collections::HashMap;
use std::sync::Arc;

use hostname::get_hostname;


use timely::dataflow::operators::capture::Event;
use timely::dataflow::operators::Inspect;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::operators::Accumulate;

use timely_communication::Configuration;

use rdkafka::config::ClientConfig;

use kafkaesque::EventConsumer;

fn main() {
    timely::execute_from_args(std::env::args(), |worker, config| {


       	let mut hostname_local_degree : Vec<(String, i32)> = Vec::new();
        match & *config {
            &Configuration::Thread => {
                println!("config Thread");
                hostname_local_degree.push((get_hostname().unwrap(),1));
            }
            // Use one process with an indicated number of threads.
            &Configuration::Process(ref _threads) => println!("config process"),
            // Expect multiple processes indicated by `(threads, process, host_list, report)`.
            &Configuration::Cluster(ref threads, ref process, ref addresses, ref _report) => {
                let mut hostname_index = HashMap::new();
                let mut hostname_local_degree_map : HashMap<String, i32> = HashMap::new();
                for address in addresses.iter() {
                    let mut key : String = "".to_owned();
                    let splits : Vec<&str> = address.split(':').collect();
                    let hostname : &str = splits[0];
                    let index = hostname_index.entry(hostname.to_string()).or_insert(-1);
                    *index += 1;
                    let port : &str = splits[1];
                    key.push_str(hostname);
                    key.push_str("[");
                    key.push_str(&(index.to_string())[..]);
                    key.push_str("]");
                    hostname_local_degree.push((key, *threads as i32));
                }
                println!("config cluster");
            }
        }

        for &(ref hostname, degree) in &hostname_local_degree {
            println!("hostname: {}, degree: {}", hostname, degree);
        } 

        let hostname_local_degree = Arc::new(hostname_local_degree);


        let topic = std::env::args().nth(1).unwrap();
        let source_peers = std::env::args().nth(2).unwrap().parse::<usize>().unwrap();
        let brokers = std::env::args().nth(3).unwrap();

        // Create Kafka stuff.
        let mut consumer_config = ClientConfig::new();
        consumer_config
            .set("produce.offset.report", "true")
            .set("auto.offset.reset", "smallest")
            .set("group.id", "example")
            .set("enable.auto.commit", "false")
            .set("enable.partition.eof", "false")
            .set("auto.offset.reset", "earliest")
            .set("session.timeout.ms", "6000")
            .set("bootstrap.servers", &brokers);

		println!("worker peers: {}", worker.peers());

        // create replayers from disjoint partition of source worker identifiers.
       // let replayers =
       // (0 .. source_peers)
       //     .filter(|i| i % worker.peers() == worker.index())
       //     .map(|i| {
       //         let topic = format!("{}-{:?}", topic, i);
       //         EventConsumer::<_,u64>::new(consumer_config.clone(), topic)
       //     })
       //     .collect::<Vec<_>>();

		let replayers =
		(0 .. worker.peers())
			.filter(|i| i % worker.peers() == worker.index())
			.map(|i| {
                let topic = format!("{}",topic);
                println!("replays worker index: {}", i);
				EventConsumer::<_,String>::new_with_index(consumer_config.clone(), topic, i, hostname_local_degree.clone())
			})
			.collect::<Vec<_>>();


        worker.dataflow::<u64,(),_>(|scope| {
            replayers
                .replay_into(scope)
            //    .count()
                .inspect(|x| {
                  //  match &x {
                  //      &Event::Messages(ref timestamp, ref data) => println!("real message"),
                  //      &Event::Progress(ref _progress) => println!("progress message"),
                  //  }
                    println!("replayed: {:?}", x)
                })
                ;
        })
    }).unwrap(); // asserts error-free execution
}

