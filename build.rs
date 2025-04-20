// build.rs
extern crate vergen;
use vergen::*;
use vergen_git2::Git2Builder;
fn main() {
  let build = BuildBuilder::all_build().expect("Should be able to generate build time info");
  let cargo = CargoBuilder::all_cargo().expect("Should be able to generate build time info");
  let rustc = RustcBuilder::all_rustc().expect("Should be able to generate build time info");
  let si = SysinfoBuilder::all_sysinfo().expect("Should be able to generate build time info");
  let git2 = Git2Builder::all_git().expect("Should be able to do git");

  Emitter::default()
    .add_instructions(&build)
    .expect("Should be able to generate build time info")
    .add_instructions(&cargo)
    .expect("Should be able to generate build time info")
    .add_instructions(&git2)
    .expect("Should load git")
    .add_instructions(&rustc)
    .expect("Should be able to generate build time info")
    .add_instructions(&si)
    .expect("Should be able to generate build time info")
    .emit()
    .expect("Should be able to generate build time info");
}
