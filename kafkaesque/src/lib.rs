extern crate rdkafka;
extern crate timely;
extern crate abomonation;
extern crate hostname;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicIsize, Ordering};
use std::vec::Vec;
use std::cmp;

use abomonation::Abomonation;
use timely::dataflow::operators::capture::event::{Event, EventPusher, EventIterator};

use rdkafka::Message;
use rdkafka::client::Context;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{BaseProducer, ProducerContext, DeliveryResult};
use rdkafka::consumer::{Consumer, BaseConsumer, EmptyConsumerContext};
use rdkafka::metadata::{MetadataBroker, MetadataPartition, MetadataTopic, Metadata};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};

use rdkafka::config::FromClientConfigAndContext;

struct OutstandingCounterContext {
    outstanding: Arc<AtomicIsize>,
}

impl Context for OutstandingCounterContext { }

impl ProducerContext for OutstandingCounterContext {
    type DeliveryOpaque = ();
    fn delivery(&self, _report: &DeliveryResult, _: Self::DeliveryOpaque) {
        self.outstanding.fetch_sub(1, Ordering::SeqCst);
    }
}

impl OutstandingCounterContext {
    pub fn new(counter: &Arc<AtomicIsize>) -> Self {
        OutstandingCounterContext {
            outstanding: counter.clone()
        }
    }
}

/// A wrapper for `W: Write` implementing `EventPusher<T, D>`.
pub struct EventProducer<T, D> {
    topic: String,
    buffer: Vec<u8>,
    producer: BaseProducer<OutstandingCounterContext>,
    counter: Arc<AtomicIsize>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<T, D> EventProducer<T, D> {
    /// Allocates a new `EventWriter` wrapping a supplied writer.
    pub fn new(config: ClientConfig, topic: String) -> Self {
        let counter = Arc::new(AtomicIsize::new(0));
        let context = OutstandingCounterContext::new(&counter);
        let producer = BaseProducer::<OutstandingCounterContext>::from_config_and_context(&config, context).expect("Couldn't create producer");
        println!("allocating producer for topic {:?}", topic);
        EventProducer {
            topic: topic,
            buffer: vec![],
            producer: producer,
            counter: counter,
            phant: ::std::marker::PhantomData,
        }
    }
}

impl<T: Abomonation, D: Abomonation> EventPusher<T, D> for EventProducer<T, D> {
    fn push(&mut self, event: Event<T, D>) {
        unsafe { ::abomonation::encode(&event, &mut self.buffer).expect("Encode failure"); }
        // println!("sending {:?} bytes", self.buffer.len());
        self.producer.send_copy::<[u8],()>(self.topic.as_str(), None, Some(&self.buffer[..]), None, (), None).unwrap();
        self.counter.fetch_add(1, Ordering::SeqCst);
        self.producer.poll(0);
        self.buffer.clear();
    }
}

impl<T, D> Drop for EventProducer<T, D> {
    fn drop(&mut self) {
        while self.counter.load(Ordering::SeqCst) > 0 {
            self.producer.poll(10);
        }
    }
}

/// A Wrapper for `R: Read` implementing `EventIterator<T, D>`.
pub struct EventConsumer<T, D> {
    consumer: BaseConsumer<EmptyConsumerContext>,
    buffer: Vec<u8>,
    phant: ::std::marker::PhantomData<(T,D)>,
}

impl<T, D> EventConsumer<T, D> {
    /// Allocates a new `EventReader` wrapping a supplied reader.
    pub fn new(config: ClientConfig, topic: String) -> Self {
        println!("allocating consumer for topic {:?}", topic);
        let consumer : BaseConsumer<EmptyConsumerContext> = config.create().expect("Couldn't create consumer");
        consumer.subscribe(&[&topic]).expect("Failed to subscribe to topic");
        EventConsumer {
            consumer: consumer,
            buffer: Vec::new(),
            phant: ::std::marker::PhantomData,
        }
    }

    pub fn new_with_index(config: ClientConfig, topic: String, index: usize, hostname_local_degree: Arc<Vec<(String, i32)>>) -> Self {
        println!("hostname: {}", hostname::get_hostname().unwrap());
        let consumer : BaseConsumer<EmptyConsumerContext> = config.create().expect("Couldn't create sonsumer");

        let metadata = consumer.fetch_metadata(Some(&topic[..]), 600).expect("Failed to fetch metadata");

        println!("Cluster information:");
        println!("  Broker count: {}", metadata.brokers().len());
        println!("  Topics count: {}", metadata.topics().len());
        println!("  Metadata broker name: {}", metadata.orig_broker_name());
        println!("  Metadata broker id: {}\n", metadata.orig_broker_id());

        let h_to_b = PartitionAssigner::hostname_to_broker_id(metadata.brokers());
        let b_to_h = PartitionAssigner::broker_id_to_hostname(metadata.brokers());
        
        let topics : Vec<&MetadataTopic> = metadata.topics().iter().filter(|&t| t.name()==&topic[..]).collect();
        assert!(topics.len() == 1, "topics vector is not 1 but {}", topics.len());
        let partitions = topics[0].partitions();
        let b_to_p = PartitionAssigner::broker_id_to_partitions(partitions);

		let partition_assignments = PartitionAssigner::partition_assignment(h_to_b, b_to_h, b_to_p, hostname_local_degree);

        println!("get the partition assignments");

        assert!(index < partition_assignments.len(), "index is {}, length of partition assignments is {}", index, partition_assignments.len());

        let my_partition = &partition_assignments[index];

        println!("partition size:{}", my_partition.len());
        
        let mut my_partition_list = TopicPartitionList::new();

        for p in my_partition.iter() {
            my_partition_list.add_partition(&topic, *p);
        }

        consumer.assign(&my_partition_list).unwrap();
        println!("consumer assign");
        
        EventConsumer {
            consumer: consumer,
            buffer: Vec::new(),
            phant: ::std::marker::PhantomData,
        }
    } 

}

pub struct PartitionAssigner {

}

impl PartitionAssigner {

    fn max_partitions_per_thread(num_partitions : & i32, num_peers : & i32) -> i32 {
        assert!(*num_peers != 0, "num_peers must not be equal to 0");
        return (num_partitions + num_peers - 1) / num_peers;
    }

    fn min_partitions_per_thread(num_partitions : & i32, num_peers : & i32) -> i32 {
        assert!(*num_peers != 0, "num_peers must not be equal to 0");
        return num_partitions / num_peers;
    }

	fn partition_assignment<'a>(h_to_b : HashMap<&'a str, i32>, b_to_h : HashMap<i32, &'a str>, mut b_to_p : HashMap<i32, Vec<i32>>, hostname_local_degree : Arc<Vec<(String, i32)>>) -> Vec<Vec<i32>> {
		let mut machine_degree = HashMap::new();
		let mut num_peers : i32 = 0;
		for &(ref hostname, degree) in (*hostname_local_degree).iter() {
			let splits : Vec<&str> = hostname.split('[').collect();
			let machine = splits[0];
			let m_degree = machine_degree.entry(machine).or_insert(0);
			*m_degree += degree;
			num_peers += degree;
		}

		println!("num_peers: {}", num_peers);
		for (machine, degree) in &machine_degree {
			println!("machine: {}, degree: {}", machine, degree);
		}	

        let mut num_partitions : i32 = 0;
        for (broker, partitions) in b_to_p.iter() {
            num_partitions += partitions.len() as i32;
        }
        println!("num_partitions: {}", num_partitions);

        // step 1
        let max_p = PartitionAssigner::max_partitions_per_thread(&num_partitions, &num_peers);
        let min_p = PartitionAssigner::min_partitions_per_thread(&num_partitions, &num_peers);

        println!("max partitions per thread: {}", max_p);
        println!("min partitions per thread: {}", min_p);

        // step 2
        let mut surplus : i32 = num_partitions - num_peers * min_p;

        let mut machine_partitions : HashMap<String, Vec<i32>> = HashMap::new();
        
        for (machine, degree) in machine_degree.iter() {
            if (h_to_b.contains_key(*machine)) {
                let partitions_on_machine = b_to_p.get_mut(h_to_b.get(machine).unwrap()).unwrap();
                let max_partitions_can_have : i32 = degree * min_p + cmp::min(surplus, *degree);
                let mut num_partitions_really_have : i32 = cmp::min(partitions_on_machine.len() as i32, max_partitions_can_have);
                if (num_partitions_really_have > degree * min_p) {
                    surplus -= num_partitions_really_have - degree * min_p;
                } 
                while (num_partitions_really_have > 0) {
                    let m_partitions = machine_partitions.entry((*machine).to_string()).or_insert(Vec::new());
                    m_partitions.push(partitions_on_machine.pop().unwrap());
                    num_partitions_really_have -= 1;
                }
            } else {
                machine_partitions.entry((*machine).to_string()).or_insert(Vec::new());
            }
        }

//        println!("step 2");
//
//        for (machine, partitions) in machine_partitions.iter() {
//            println!("machine: {}", machine);
//            for & partition in partitions.iter() {
//                print!("{}, ", partition);
//            }
//            println!("");
//        }

        // step 3
        let mut left_partitions : Vec<i32> = Vec::new();
        for (broker, partitions) in b_to_p.iter() {
            left_partitions.extend(partitions);
        }

        println!("left partitions");
        for partition in left_partitions.iter() {
            print!("{}, ", partition);
        }
        println!("");

        for (machine, partitions) in machine_partitions.iter_mut() {
            let num_partitions_already_have : i32 = partitions.len() as i32;
            let local_degree = machine_degree.get(&machine[..]).unwrap();
            let max_partitions_can_have : i32 = local_degree * min_p + cmp::min(surplus, *local_degree);
            let mut num_partitions_really_have : i32 = max_partitions_can_have - num_partitions_already_have;
            if (max_partitions_can_have < num_partitions_already_have) {
                num_partitions_really_have = 0;
            }
            num_partitions_really_have = cmp::min(num_partitions_really_have, left_partitions.len() as i32);
            if (num_partitions_really_have > local_degree * min_p) {
                surplus -= (num_partitions_really_have - local_degree * min_p);
            }
            while(num_partitions_really_have > 0) {
                partitions.push(left_partitions.pop().unwrap());
                num_partitions_really_have -= 1;
            }
        }

//        println!("step 3");
//        for (machine, partitions) in machine_partitions.iter() {
//            println!("machine: {}", machine);
//            for & partition in partitions.iter() {
//                print!("{}, ", partition);
//            }
//            println!("");
//        }
        

        // step 4
	    let mut partition_assignments : Vec<Vec<i32>> = Vec::new();  
        for i in 0..num_peers {
            partition_assignments.push(Vec::new());
        }

        println!("step 4");

        for (i, &(ref hostname, ref local_degree)) in hostname_local_degree.iter().enumerate() {
			let splits : Vec<&str> = hostname.split('[').collect();
			let machine = splits[0];
            let partitions_to_machine = machine_partitions.get_mut(&machine.to_string()).unwrap();
            let degree : &mut i32 = machine_degree.get_mut(machine).unwrap();
            let mut local_degree = *local_degree;

            while(local_degree > 0) {
                let mut num_partitions_should_have : i32 = 0;
                let num_partitions : i32 = partitions_to_machine.len() as i32;
                // we allocate min_p at first
                // until we have to allocate max_p
                if (num_partitions < *degree * max_p) {
                    num_partitions_should_have = min_p;
                } else {
                    num_partitions_should_have = max_p;
                }
    
                let mut res : Vec<i32> = Vec::new();
                while(num_partitions_should_have > 0) {
                    res.push(partitions_to_machine.pop().unwrap());
                    num_partitions_should_have -= 1;
                }
                partition_assignments[i].extend(res);
                local_degree -= 1;
                *degree -= 1;
            }
        }

        for (i, partitions) in partition_assignments.iter().enumerate() {
            println!("for worker index: {}", i);
            for &partition in partitions.iter() {
                print!("{}, ", partition);
            }
            println!("");
        }
	

	    return partition_assignments;
	}
	
	
	fn broker_id_to_partitions(partitions: &[MetadataPartition]) -> HashMap<i32, Vec<i32>> {
	        let mut b_to_p : HashMap<i32, Vec<i32>> = HashMap::new();
	
	        for partition in partitions.iter() {
	            let b_id = partition.leader();
	            let p_id = partition.id();
	            let p_vec = b_to_p.entry(b_id).or_insert(Vec::new());
	            p_vec.push(p_id);
	        }
	
	        return b_to_p;
	    }
	
	fn broker_id_to_hostname<'a>(brokers: &'a [MetadataBroker]) -> HashMap<i32, &'a str> {
	    let mut b_to_h = HashMap::new();
	
	    for broker in brokers.iter() {
	        b_to_h.insert(broker.id(), broker.host());
	    }
	
	    return b_to_h;
	}
	
	fn hostname_to_broker_id<'a>(brokers: &'a [MetadataBroker]) -> HashMap<&'a str, i32> {
	    let mut h_to_b = HashMap::new();
	
	    for broker in brokers.iter() {
	        h_to_b.insert(broker.host(), broker.id());
	    }
	
	    return h_to_b;
	}

}


impl<T: Abomonation, D: Abomonation> EventIterator<T, D> for EventConsumer<T, D> {
    fn next(&mut self) -> Option<&Event<T, D>> {
        if let Some(result) = self.consumer.poll(0) {
            match result {
                Ok(message) =>  {
                    if message.payload().is_none() {
                        println!("message payload is none");
                    } else {
                        println!("message payload is not none");
                    }
                    self.buffer.clear();
                    self.buffer.extend_from_slice(message.payload().unwrap());
                    Some(unsafe { 
                        //::abomonation::decode::<Event<T,D>>(&mut self.buffer[..]).unwrap().0 
                        let thing = ::abomonation::decode::<Event<T,D>>(&mut self.buffer[..]);
                        let msg = thing.unwrap().0;
                        msg
                    })
                },
                Err(err) => {
                    println!("KafkaConsumer error: {:?}", err);
                    None
                },
            }
        }
        else { None }
    }
}


