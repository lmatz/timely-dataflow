extern crate timely;
extern crate rdkafka;
extern crate kafkaesque;

use std::io;
use std::io::BufReader;
use std::io::BufRead;
use std::io::Write;
use std::fs::File;

use timely::dataflow::InputHandle;
use timely::dataflow::operators::ToStream;
use timely::dataflow::operators::capture::Capture;

use rdkafka::config::ClientConfig;

use kafkaesque::EventProducer;

fn main() {
    timely::execute_from_args(std::env::args(), |worker, config| {

        // target topic name.
        let topic = std::env::args().nth(1).unwrap();
        let local_filename = std::env::args().nth(2).unwrap();
        let brokers = std::env::args().nth(3).unwrap();

        // Create Kafka stuff.
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("produce.offset.report", "true")
            .set("bootstrap.servers", &brokers);

        let topic = format!("{}", topic);
        let producer : EventProducer<_, String> = EventProducer::new(producer_config, topic);

        let mut input = InputHandle::new();
        let f = File::open(local_filename).unwrap();
        let file = BufReader::new(&f);

        worker.dataflow::<u64,_,_>(|scope|
            input
                .to_stream(scope)
                .capture_into(producer)
        );

        input.send(String::from("kddb"));
        input.advance_to(1);
        worker.step();

//        for (num, line) in file.lines().enumerate() {
//      
//            input.send(line);
//        }
//
//        input.advance_to(0+1);
//        worker.step();
    }).unwrap();
}

