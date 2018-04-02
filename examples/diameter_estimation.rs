extern crate time;
extern crate rand;
extern crate timely;
extern crate timely_sort;

use std::{thread};

use std::cell::RefCell;

use std::vec::Vec;
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Range;

use timely::dataflow::operators::generic::Operator;
use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Input};

use timely::dataflow::operators::{Probe, ConnectLoop, LoopVariable};
use timely::dataflow::channels::pact::Exchange;
use timely::logging::{LoggerConfig, EventPusherTee};

fn main() {
    let logger_config = LoggerConfig::new(
        |_setup| EventPusherTee::new(),
        |_setup| EventPusherTee::new());

    timely::execute_from_args_logging(std::env::args(), logger_config, move |worker, _config| {
        let mut edge_input = InputHandle::new();
        let mut probe = ProbeHandle::new();

        worker.dataflow::<_,(),_>(|scope| {
            let edges = scope.input_from(&mut edge_input);
            let (handle, msg_stream) = scope.loop_variable(usize::max_value(), 1);

            let exchange1 = Exchange::new(|x: &(u32, u32)| (x.0) as u64);
            let exchange2 = Exchange::new(|x: &(u32, (u32, u32))| (x.0) as u64);

            let msgs = edges.binary(&msg_stream, exchange1, exchange2, "DiameterEstimation", |_capability, _info| {

                let mut neighbors = HashMap::<u32, Vec<u32>>::new();
                let mut history = HashMap::<u32, Vec<HashSet<u32>>>::new();

                let neighbors_ref = RefCell::new(neighbors);

                move |input1, input2, output| {
                    // Drain first input, check second map, update first map.
                    input1.for_each(|time, data| {
                        let mut neighbors = neighbors_ref.borrow_mut();
                        let mut session = output.session(&time);
                        let one_second = std::time::Duration::from_millis(1000);
                        thread::sleep(one_second);
                        println!("input1 time:{:?}", time);
                        for (key, val) in data.drain(..) {
                            println!("key: {}, val: {}", key, val);
                            {
                                let entry = neighbors.entry(key).or_insert(Vec::new());
                            entry.push(val);
                            }
                            {
                                let entry = neighbors.entry(val).or_insert(Vec::new());
                            entry.push(key);
                            }
                            history.entry(key).or_insert(Vec::new());
                            history.entry(val).or_insert(Vec::new());
                        }
                        println!("iterate neighbor");
                        for (key, value) in neighbors.iter() {
                            println!("key: {}", key);
                            for n_id in value.iter() {
                                println!("sending n_id: {}", *n_id);
                                session.give((*n_id, (*key, 1)));
                            }
                        }
                        println!("input1 time:{:?} end!", time);
                    });
                    // Drain second input, check first map, update second map.
                    input2.for_each(|time, data| {
                        let mut neighbors = neighbors_ref.borrow_mut();
                        let mut session = output.session(&time);
                        let one_second = std::time::Duration::from_millis(1000);
                        thread::sleep(one_second);
                        println!("input2 time:{:?}", time);
                        for (key, pair) in data.drain(..) {
                            let src = pair.0;
                            let val = pair.1;
                            println!("key: {}, src: {}, val: {}", key, src, val);
                            let all_vecs = history.get_mut(&key).unwrap();
                            let mut len = all_vecs.len();
                            while val >= (len as u32) {
                                all_vecs.push(HashSet::new());
                                len = all_vecs.len();
                            }

                            let mut contains = false;
                            let mut history_at_prev_clone;
                            {
                                let history_at_prev = &mut all_vecs[(val as usize) - 1];
                                history_at_prev_clone = history_at_prev.clone();
                                contains = history_at_prev.contains(&src);
                            }
                            let history_at_cur = &mut all_vecs[(val as usize)];
                            history_at_cur.extend(history_at_prev_clone);
                            if !contains {
                                history_at_cur.insert(src);
                                let nbs = neighbors.get(&key).unwrap();
                                for n_id in nbs.iter() {
                                    session.give((*n_id, (src, val+1)));
                                }
                            } else {

                            }
                        }
                        for (key, srcs) in history.iter() {
                            println!("play history for key:{}",key);
                            for src in srcs.last().unwrap() {
                                println!("{}",src);
                            }
                        }
                        println!("input2 time:{:?} end!", time);
                    });

                }
            });


            msgs.probe_with(&mut probe).connect_loop(handle);
        });


        let nodes : Range<u32> = 0..3;
        for n in nodes {
            edge_input.send((n, n+1));
        }

        edge_input.advance_to(1);
        while probe.less_than(edge_input.time()) {
            worker.step();
        }
    }).unwrap();
}
