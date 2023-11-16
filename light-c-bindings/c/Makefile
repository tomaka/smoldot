
all: build/example

.PHONY: all ../../target/release/libsmoldot_light_c_bindings.a

../../target/release/libsmoldot_light_c_bindings.a:
	cargo build --release --manifest-path=../rust/Cargo.toml

build/libsmoldot.a: ../../target/release/libsmoldot_light_c_bindings.a
	cp ../../target/release/libsmoldot_light_c_bindings.a ./build/libsmoldot.a

build/example: ./example.c build/libsmoldot.a
	cc -o ./build/example example.c -L./build -lsmoldot
