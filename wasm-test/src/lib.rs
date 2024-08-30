use timely::dataflow::{InputHandle, ProbeHandle};
use timely::dataflow::operators::{Inspect, Probe};
use timely::WorkerConfig;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen_test::*;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(js_namespace = console)]
    fn log(s: &str);
}

#[wasm_bindgen_test]
pub fn basic_wasm_example() {
    let allocator = timely::communication::allocator::Thread::new();
    let mut worker = timely::worker::Worker::new(WorkerConfig::default(), allocator, None);
    let mut input = InputHandle::new();
    let mut probe = ProbeHandle::new();

    worker.dataflow(|scope| {
        input
            .to_stream(scope)
            .inspect(|x| log(&format!("{:?}", x)))
            .probe_with(&mut probe);
    });

    let mut step_count = 0;
    for i in 0..10 {
        input.send(i);
        input.advance_to(i);
        while probe.less_than(input.time()) {
            worker.step();
            step_count += 1;

            if step_count >= 1000 {
                panic!("Reached 1000 steps without completing")
            }
        }
    }
}
