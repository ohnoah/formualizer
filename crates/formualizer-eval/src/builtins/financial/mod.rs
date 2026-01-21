//! Financial functions
//! Functions implemented: PMT, PV, FV, NPV, NPER, RATE, IPMT, PPMT, SLN, SYD, DB, DDB

mod depreciation;
mod tvm;

pub use depreciation::*;
pub use tvm::*;

pub fn register_builtins() {
    tvm::register_builtins();
    depreciation::register_builtins();
}
