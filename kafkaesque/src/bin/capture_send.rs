extern crate timely;
extern crate rdkafka;
extern crate kafkaesque;

use timely::dataflow::operators::ToStream;
use timely::dataflow::operators::capture::Capture;

use rdkafka::config::ClientConfig;

use kafkaesque::EventProducer;

fn main() {
    timely::execute_from_args(std::env::args(), |worker| {

        // target topic name.
        let topic = std::env::args().nth(1).unwrap();
        let count = std::env::args().nth(2).unwrap().parse::<u64>().unwrap();
        let brokers = std::env::args().nth(3).unwrap();

        // Create Kafka stuff.
        let mut producer_config = ClientConfig::new();
        producer_config
            .set("produce.offset.report", "true")
            .set("bootstrap.servers", brokers);

        let topic = format!("{}-{:?}", topic, worker.index());
        let producer = EventProducer::new(producer_config, topic);

        worker.dataflow::<u64,_,_>(|scope|
            (0 .. count)
                .to_stream(scope)
                .capture_into(producer)
        );
    }).unwrap();
}
