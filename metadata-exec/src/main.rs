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
                if vm.key().as_ref()
                    == [
                        95, 62, 73, 7, 247, 22, 172, 137, 182, 52, 125, 21, 236, 236, 237, 202, 11,
                        106, 69, 50, 30, 250, 233, 42, 234, 21, 224, 116, 14, 199, 175, 231,
                    ]
                {
                    runtime_call = vm.inject_value(Some((
                        iter::once([18, 29, 0, 0]),
                        executor::runtime_call::TrieEntryVersion::V0,
                    )));
                } else {
                    panic!()
                }
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
