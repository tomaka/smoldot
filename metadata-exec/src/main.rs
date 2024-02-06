use std::{fs, iter, path::PathBuf, time::Instant};

use smoldot::executor;

#[derive(clap::Parser)]
struct CliArgs {
    #[arg(long)]
    path_to_wasm: PathBuf,
}

fn main() {
    let args = <CliArgs as clap::Parser>::parse();

    let wasm = fs::read(args.path_to_wasm).unwrap();

    let vm = {
        let before_compilation = Instant::now();
        let vm = executor::host::HostVmPrototype::new(executor::host::Config {
            module: wasm,
            heap_pages: executor::DEFAULT_HEAP_PAGES,
            exec_hint: executor::vm::ExecHint::ForceWasmi {
                lazy_validation: false,
            },
            allow_unresolved_imports: true,
        })
        .unwrap();
        let elapsed = before_compilation.elapsed();
        println!("Compilation time: {elapsed:?}");
        vm
    };

    let before_runtime = Instant::now();

    let mut runtime_call = executor::runtime_call::run(executor::runtime_call::Config {
        virtual_machine: vm,
        function_to_call: "Metadata_metadata",
        parameter: iter::empty::<Vec<u8>>(),
        storage_main_trie_changes: Default::default(),
        max_log_level: 4,
        calculate_trie_changes: false,
    })
    .unwrap();

    loop {
        match runtime_call {
            executor::runtime_call::RuntimeCall::Finished(Err(err)) => panic!("{err}"),
            executor::runtime_call::RuntimeCall::Finished(Ok(_)) => break,
            executor::runtime_call::RuntimeCall::StorageGet(vm) => {
                runtime_call = vm.inject_value(None::<(iter::Empty<Vec<u8>>, _)>);
                //panic!("{:?}", vm.key().as_ref());
            }
            executor::runtime_call::RuntimeCall::NextKey(_vm) => {
                unreachable!()
            }
            executor::runtime_call::RuntimeCall::ClosestDescendantMerkleValue(vm) => {
                runtime_call = vm.resume_unknown();
            }
            executor::runtime_call::RuntimeCall::LogEmit(vm) => {
                runtime_call = vm.resume();
            }
            executor::runtime_call::RuntimeCall::Offchain(_) => unreachable!(),
            executor::runtime_call::RuntimeCall::OffchainStorageSet(vm) => {
                runtime_call = vm.resume();
            }
            executor::runtime_call::RuntimeCall::SignatureVerification(vm) => {
                runtime_call = vm.verify_and_resume();
            }
        }
    }

    let elapsed = before_runtime.elapsed();
    println!("Call duration: {elapsed:?}");
}
